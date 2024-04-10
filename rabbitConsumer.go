package rabbit

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/asyncLocal"
	"github.com/farseer-go/fs/exception"
	"github.com/farseer-go/fs/flog"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
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
				asyncLocal.GC()
				entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
				args := receiver.createEventArgs(page, queueName)
				exception.Try(func() {
					consumerHandle(string(page.Body), args)
				}).CatchException(func(exp any) {
					entryMqConsumer.Error(flog.Errorf("rabbit：Subscribe exception:%s", exp))
				})
				entryMqConsumer.End()
			}
			// 通道关闭了
			if chl != nil {
				_ = chl.Close()
			}
			// 1秒后重试
			time.Sleep(1 * time.Second)
		}
	}()
}

func (receiver *rabbitConsumer) SubscribeAck(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs) bool) {
	go func() {
		for {
			// 创建一个连接和通道
			chl, deliveries, err := receiver.createQueueAndBindAndConsume(queueName, routingKey, prefetchCount, false)
			if err != nil {
				// 3秒后重试
				time.Sleep(3 * time.Second)
				continue
			}
			// 读取通道的消息
			for page := range deliveries {
				asyncLocal.GC()
				entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
				args := receiver.createEventArgs(page, queueName)
				isSuccess := false
				exception.Try(func() {
					if isSuccess = consumerHandle(string(page.Body), args); isSuccess {
						if err = page.Ack(false); err != nil {
							_ = flog.Errorf("rabbit：SubscribeAck failed to Ack q=%s: %s %s", queueName, err, string(page.Body))
						}
					}
				}).CatchException(func(exp any) {
					entryMqConsumer.Error(flog.Errorf("rabbit：SubscribeAck exception: q=%s err:%s", queueName, exp))
				})
				if !isSuccess {
					// Nack
					if err = page.Nack(false, true); err != nil {
						entryMqConsumer.Error(flog.Errorf("rabbit：SubscribeAck failed to Nack %s: q=%s %s", queueName, err, string(page.Body)))
					}
				}
				entryMqConsumer.End()
			}
			// 通道关闭了
			if chl != nil {
				_ = chl.Close()
			}
			// 1秒后重试
			time.Sleep(1 * time.Second)
		}
	}()
}

func (receiver *rabbitConsumer) SubscribeBatch(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs])) {
	if pullCount < 1 {
		flog.Panicf("rabbit：the parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	if _, err := receiver.createAndBindQueue(nil, queueName, routingKey); err != nil {
		panic(err)
	}

	go func() {
		var chl *amqp.Channel
		for {
			asyncLocal.GC()
			time.Sleep(500 * time.Millisecond)
			entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
			// 创建一个连接和通道
			var err error
			if chl == nil || chl.IsClosed() {
				if chl, err = receiver.manager.CreateChannel(); err != nil {
					entryMqConsumer.Error(err)
					entryMqConsumer.End()
					_ = flog.Error(err)
					continue
				}
			}

			lst, _ := receiver.pullBatch(queueName, true, pullCount, chl)
			if lst.Count() > 0 {
				exception.Try(func() {
					consumerHandle(lst)
				}).CatchException(func(exp any) {
					entryMqConsumer.Error(flog.Errorf("rabbit：SubscribeBatch exception:%s", exp))
				})
				// 数量大于0，才追踪
				entryMqConsumer.End()
			}
		}
	}()
}

func (receiver *rabbitConsumer) SubscribeBatchAck(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs]) bool) {
	if pullCount < 1 {
		flog.Panicf("rabbit：the parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	if _, err := receiver.createAndBindQueue(nil, queueName, routingKey); err != nil {
		panic(err)
	}

	go func() {
		var chl *amqp.Channel
		for {
			asyncLocal.GC()
			time.Sleep(100 * time.Millisecond)
			entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
			// 创建一个连接和通道
			var err error
			if chl == nil || chl.IsClosed() {
				if chl, err = receiver.manager.CreateChannel(); err != nil {
					entryMqConsumer.Error(err)
					entryMqConsumer.End()
					_ = flog.Error(err)
					continue
				}
			}

			lst, lastPage := receiver.pullBatch(queueName, false, pullCount, chl)
			if lst.Count() > 0 {
				isSuccess := false
				exception.Try(func() {
					isSuccess = consumerHandle(lst)
					if isSuccess {
						if err := lastPage.Ack(true); err != nil {
							entryMqConsumer.Error(flog.Errorf("rabbit：SubscribeBatchAck failed to Ack %s: %s", queueName, err))
						}
					}
				}).CatchException(func(exp any) {
					entryMqConsumer.Error(flog.Errorf("rabbit：SubscribeBatchAck exception %s:%s", queueName, exp))
				})
				if !isSuccess {
					// Nack
					if err := lastPage.Nack(true, true); err != nil {
						entryMqConsumer.Error(flog.Errorf("rabbit：SubscribeBatchAck failed to Nack %s: %s", queueName, err))
					}
				}
				// 数量大于0，才追踪
				entryMqConsumer.End()
			}
		}
	}()
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
		CorrelationId:   page.CorrelationId,
		ReplyTo:         page.ReplyTo,
		Expiration:      page.Expiration,
		MessageId:       page.MessageId,
		Timestamp:       page.Timestamp,
		Type:            page.Type,
		UserId:          page.UserId,
		AppId:           page.AppId,
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
