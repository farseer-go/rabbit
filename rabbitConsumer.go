package rabbit

import (
	"fmt"
	"time"

	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/asyncLocal"
	"github.com/farseer-go/fs/color"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/flog"
	"github.com/farseer-go/fs/trace"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitConsumer struct {
	manager *rabbitManager
}

func newConsumer(config rabbitConfig) *rabbitConsumer {
	return &rabbitConsumer{
		manager: newManager(config),
	}
}

// 创建绑定队列并订阅消息
func (receiver *rabbitConsumer) createQueueAndBindAndConsume(queueName, routingKey string, prefetchCount int, autoAck bool) (*amqp.Channel, <-chan amqp.Delivery, error) {
	var chl *amqp.Channel
	var err error
	if chl, err = receiver.createAndBindQueue(chl, queueName, routingKey); err != nil {
		_ = flog.Error(err)
		return nil, nil, err
	}

	// 设置预读数量
	if err = chl.Qos(prefetchCount, 0, false); err != nil {
		_ = flog.Errorf("rabbit：failed to Qos %s: %s", queueName, err)
		return nil, nil, err
	}

	// 订阅消息
	deliveries, err := chl.Consume(queueName, "", autoAck, false, false, false, nil)
	if err != nil {
		_ = flog.Errorf("rabbit：failed to Subscribe %s: %s", queueName, err)
		return nil, nil, err
	}
	return chl, deliveries, nil
}

func (receiver *rabbitConsumer) Subscribe(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs)) {
	fs.AddInitCallback(fmt.Sprintf("rabbit.Subscribe消费，队列：%s，routingKey：%s，预读数量：%d", queueName, routingKey, prefetchCount), func() {
		go func() {
			for {
				// 创建一个连接和通道
				chl, deliveries, err := receiver.createQueueAndBindAndConsume(queueName, routingKey, prefetchCount, true)
				if err != nil {
					// 3秒后重试
					time.Sleep(3 * time.Second)
					continue
				}
				// 读取通道的消息
				for page := range deliveries {
					entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(page.CorrelationId, page.AppId, receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
					args := receiver.createEventArgs(page, queueName)
					exception.Try(func() {
						consumerHandle(string(page.Body), args)
					}).CatchException(func(exp any) {
						err = flog.Errorf("rabbit：Subscribe exception:%s", exp)
					})
					container.Resolve[trace.IManager]().Push(entryMqConsumer, err)
					asyncLocal.Release()
				}
				// 通道关闭了
				if chl != nil {
					_ = chl.Close()
				}
				// 1秒后重试
				time.Sleep(1 * time.Second)
			}
		}()
	})
}

func (receiver *rabbitConsumer) SubscribeAck(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs) bool) {
	fs.AddInitCallback(fmt.Sprintf("rabbit.SubscribeAck消费，队列：%s，routingKey：%s，预读数量：%d", queueName, routingKey, prefetchCount), func() {
		go func() {
			for {
				// 创建一个连接和通道
				chl, deliveries, err := receiver.createQueueAndBindAndConsume(queueName, routingKey, prefetchCount, false)
				if err != nil {
					flog.Errorf("rabbit.SubscribeAck 创建通道时失败，3秒后重试：%s", err.Error())
					// 3秒后重试
					time.Sleep(3 * time.Second)
					continue
				}

				for page := range deliveries {
					entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(page.CorrelationId, page.AppId, receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
					args := receiver.createEventArgs(page, queueName)
					isSuccess := false
					exception.Try(func() {
						if isSuccess = consumerHandle(string(page.Body), args); isSuccess {
							if err = page.Ack(false); err != nil {
								_ = flog.Errorf("rabbit：SubscribeAck failed to Ack q=%s: %s %s", queueName, err, string(page.Body))
							}
						}
					}).CatchException(func(exp any) {
						err = flog.Errorf("rabbit：SubscribeAck exception: q=%s err:%s", queueName, exp)
					})
					if !isSuccess {
						// Nack
						if err = page.Nack(false, true); err != nil {
							err = flog.Errorf("rabbit：SubscribeAck failed to Nack %s: q=%s %s", queueName, err, string(page.Body))
						}
					}
					container.Resolve[trace.IManager]().Push(entryMqConsumer, err)
					asyncLocal.Release()
				}

				// 通道关闭了
				if chl != nil {
					_ = chl.Close()
				}
				// 1秒后重试
				time.Sleep(1 * time.Second)
			}
		}()
	})
}

func (receiver *rabbitConsumer) SubscribeBatch(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs])) {
	if pullCount < 1 {
		flog.Panicf("rabbit：the parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	if _, err := receiver.createAndBindQueue(nil, queueName, routingKey); err != nil {
		panic(err)
	}

	fs.AddInitCallback(fmt.Sprintf("rabbit.SubscribeBatch消费，队列：%s，routingKey：%s，每次拉取数量：%d", queueName, routingKey, pullCount), func() {
		go func() {
			var chl *amqp.Channel
			for {
				time.Sleep(100 * time.Millisecond)
				// 创建一个连接和通道
				var err error
				if chl == nil || chl.IsClosed() {
					entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer("", "", receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
					if chl, err = receiver.manager.CreateChannel(); err != nil {
						container.Resolve[trace.IManager]().Push(entryMqConsumer, err)
						_ = flog.Error(err)
						continue
					}
				}

				if lst, _ := receiver.pullBatch(queueName, true, pullCount, chl); lst.Count() > 0 {
					entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer("", "", receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
					exception.Try(func() {
						consumerHandle(lst)
					}).CatchException(func(exp any) {
						err = flog.Errorf("rabbit：SubscribeBatch exception:%s", exp)
					})
					// 数量大于0，才追踪
					container.Resolve[trace.IManager]().Push(entryMqConsumer, err)
				}
				asyncLocal.Release()
			}
		}()
	})
}

func (receiver *rabbitConsumer) SubscribeBatchAck(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs]) bool) {
	receiver.SubscribeBatchAckTime(queueName, routingKey, pullCount, 100*time.Millisecond, consumerHandle)
}

func (receiver *rabbitConsumer) SubscribeBatchAckTime(queueName string, routingKey string, pullCount int, sleepTime time.Duration, consumerHandle func(messages collections.List[EventArgs]) bool) {
	if pullCount < 1 {
		flog.Panicf("rabbit：the parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	if _, err := receiver.createAndBindQueue(nil, queueName, routingKey); err != nil {
		panic(err)
	}

	fs.AddInitCallback(fmt.Sprintf("rabbit.SubscribeBatchAck消费，队列：%s，routingKey：%s，每次拉取数量：%d", queueName, routingKey, pullCount), func() {
		go func() {
			var chl *amqp.Channel
			for {
				time.Sleep(sleepTime)
				// 创建一个连接和通道
				var err error
				if chl == nil || chl.IsClosed() {
					entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer("", "", receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
					if chl, err = receiver.manager.CreateChannel(); err != nil {
						container.Resolve[trace.IManager]().Push(entryMqConsumer, err)
						_ = flog.Error(err)
						continue
					}
				}

				if lst, lastPage := receiver.pullBatch(queueName, false, pullCount, chl); lst.Count() > 0 {
					entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer("", "", receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
					isSuccess := false
					exception.Try(func() {
						isSuccess = consumerHandle(lst)
						if isSuccess {
							if err = lastPage.Ack(true); err != nil {
								err = flog.Errorf("rabbit：SubscribeBatchAck failed to Ack %s: %s", queueName, err)
							}
						}
					}).CatchException(func(exp any) {
						if file, funcName, line := trace.GetCallerInfo(); file != "" {
							_ = flog.Errorf("%s:%s %s \n", file, color.Blue(line), funcName)
						}
						err = flog.Errorf("rabbit：SubscribeBatchAck exception %s:%s", queueName, exp)
					})
					if !isSuccess {
						// Nack
						if err = lastPage.Nack(true, true); err != nil {
							err = flog.Errorf("rabbit：SubscribeBatchAck failed to Nack %s: %s", queueName, err)
						}
					}
					// 数量大于0，才追踪
					container.Resolve[trace.IManager]().Push(entryMqConsumer, err)
				}
				asyncLocal.Release()
			}
		}()
	})
}

// 生成事件参数
func (receiver *rabbitConsumer) createEventArgs(page amqp.Delivery, queueName string) EventArgs {
	return EventArgs{
		QueueName:       queueName,
		ConsumerTag:     page.ConsumerTag,
		DeliveryTag:     page.DeliveryTag,
		Redelivered:     page.Redelivered,
		Exchange:        page.Exchange,
		RoutingKey:      page.RoutingKey,
		Body:            page.Body,
		BodyString:      string(page.Body),
		Headers:         page.Headers,
		ContentType:     page.ContentType,
		ContentEncoding: page.ContentEncoding,
		DeliveryMode:    page.DeliveryMode,
		Priority:        page.Priority,
		ReplyTo:         page.ReplyTo,
		Expiration:      page.Expiration,
		MessageId:       page.MessageId,
		Timestamp:       page.Timestamp,
		Type:            page.Type,
		UserId:          page.UserId,
		AppId:           page.AppId,
		CorrelationId:   page.CorrelationId,
		MessageCount:    page.MessageCount,
	}
}

// 手动拉取数据
func (receiver *rabbitConsumer) pullBatch(queueName string, autoAck bool, pullCount int, chl *amqp.Channel) (collections.List[EventArgs], amqp.Delivery) {
	lst := collections.NewList[EventArgs]()
	var lastPage amqp.Delivery
	for lst.Count() < pullCount {
		msg, ok, err := chl.Get(queueName, autoAck)
		if err != nil {
			_ = flog.Errorf("rabbit：failed to Get %s: %s", queueName, err)
		}
		if !ok {
			break
		}
		lastPage = msg
		lst.Add(receiver.createEventArgs(msg, queueName))
	}

	return lst, lastPage
}

// 创建频道并绑定
func (receiver *rabbitConsumer) createAndBindQueue(chl *amqp.Channel, queueName, routingKey string) (*amqp.Channel, error) {
	var err error
	if chl == nil || chl.IsClosed() {
		// 创建一个连接和通道
		if chl, err = receiver.manager.CreateChannel(); err != nil {
			if chl != nil {
				_ = chl.Close()
			}
			return chl, err
		}

		// 创建队列
		if err = receiver.manager.CreateQueue(chl, queueName, receiver.manager.config.IsDurable, receiver.manager.config.AutoDelete, nil); err != nil {
			if chl != nil {
				_ = chl.Close()
			}
			return nil, err
		}

		// 绑定队列
		if err = receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.config.Exchange, nil); err != nil {
			if chl != nil {
				_ = chl.Close()
			}
			return nil, err
		}
	}
	return chl, nil
}
