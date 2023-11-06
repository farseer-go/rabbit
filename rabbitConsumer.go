package rabbit

import (
	"github.com/farseer-go/collections"
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

func (receiver *rabbitConsumer) createQueueAndBindAndConsume(queueName, routingKey string, prefetchCount int, autoAck bool) (*amqp.Channel, <-chan amqp.Delivery) {
	var chl *amqp.Channel
	var err error
	if chl, err = receiver.createAndBindQueue(chl, queueName, routingKey); err != nil {
		panic(err.Error())
	}

	// 设置预读数量
	if err := chl.Qos(prefetchCount, 0, false); err != nil {
		flog.Panicf("Failed to Qos %s: %s", queueName, err)
	}

	// 订阅消息
	deliveries, err := chl.Consume(queueName, "", autoAck, false, false, false, nil)
	if err != nil {
		flog.Panicf("Failed to Subscribe %s: %s", queueName, err)
	}
	return chl, deliveries
}

func (receiver *rabbitConsumer) Subscribe(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs)) {
	// 创建一个连接和通道
	chl, deliveries := receiver.createQueueAndBindAndConsume(queueName, routingKey, prefetchCount, true)

	go func(chl *amqp.Channel) {
		// 读取通道的消息
		for page := range deliveries {
			entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
			args := receiver.createEventArgs(page)
			exception.Try(func() {
				consumerHandle(string(page.Body), args)
			}).CatchException(func(exp any) {
				entryMqConsumer.Error(flog.Errorf("Subscribe exception:%s", exp))
			})
			entryMqConsumer.End()
		}
		_ = chl.Close()
		// 关闭后，重新调用自己
		receiver.Subscribe(queueName, routingKey, prefetchCount, consumerHandle)
	}(chl)
}

func (receiver *rabbitConsumer) SubscribeAck(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs) bool) {
	// 创建一个连接和通道
	chl, deliveries := receiver.createQueueAndBindAndConsume(queueName, routingKey, prefetchCount, false)

	go func(chl *amqp.Channel) {
		// 读取通道的消息
		for page := range deliveries {
			entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
			args := receiver.createEventArgs(page)
			isSuccess := false
			exception.Try(func() {
				isSuccess = consumerHandle(string(page.Body), args)
				if isSuccess {
					if err := page.Ack(false); err != nil {
						_ = flog.Errorf("SubscribeAck Failed to Ack q=%s: %s %s", queueName, err, string(page.Body))
					}
				}
			}).CatchException(func(exp any) {
				entryMqConsumer.Error(flog.Errorf("SubscribeAck exception: q=%s err:%s", queueName, exp))
			})
			if !isSuccess {
				// Nack
				if err := page.Nack(false, true); err != nil {
					entryMqConsumer.Error(flog.Errorf("SubscribeAck Failed to Nack %s: q=%s %s", queueName, err, string(page.Body)))
				}
			}
			entryMqConsumer.End()
		}
		_ = chl.Close()
		// 关闭后，重新调用自己
		receiver.SubscribeAck(queueName, routingKey, prefetchCount, consumerHandle)
	}(chl)
}

func (receiver *rabbitConsumer) SubscribeBatch(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs])) {
	if pullCount < 1 {
		flog.Panicf("The parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	go func() {
		var chl *amqp.Channel
		var err error
		for {
			time.Sleep(500 * time.Millisecond)
			entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
			if chl, err = receiver.createAndBindQueue(chl, queueName, routingKey); err != nil {
				entryMqConsumer.Error(err)
				entryMqConsumer.End()
				continue
			}

			lst, _ := receiver.pullBatch(queueName, true, pullCount, chl)
			if lst.Count() > 0 {
				exception.Try(func() {
					consumerHandle(lst)
				}).CatchException(func(exp any) {
					entryMqConsumer.Error(flog.Errorf("SubscribeBatch exception:%s", exp))
				})
				// 数量大于0，才追踪
				entryMqConsumer.End()
			}
		}
	}()
}

func (receiver *rabbitConsumer) SubscribeBatchAck(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs]) bool) {
	if pullCount < 1 {
		flog.Panicf("The parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	go func() {
		var chl *amqp.Channel
		var err error
		for {
			time.Sleep(100 * time.Millisecond)
			entryMqConsumer := receiver.manager.traceManager.EntryMqConsumer(receiver.manager.config.Server, queueName, receiver.manager.config.RoutingKey)
			if chl, err = receiver.createAndBindQueue(chl, queueName, routingKey); err != nil {
				entryMqConsumer.Error(err)
				entryMqConsumer.End()
				continue
			}
			lst, lastPage := receiver.pullBatch(queueName, false, pullCount, chl)
			if lst.Count() > 0 {
				isSuccess := false
				exception.Try(func() {
					isSuccess = consumerHandle(lst)
					if isSuccess {
						if err := lastPage.Ack(true); err != nil {
							entryMqConsumer.Error(flog.Errorf("SubscribeBatchAck Failed to Ack %s: %s", queueName, err))
						}
					}
				}).CatchException(func(exp any) {
					entryMqConsumer.Error(flog.Errorf("SubscribeBatchAck exception:%s", exp))
				})
				if !isSuccess {
					// Nack
					if err := lastPage.Nack(true, true); err != nil {
						entryMqConsumer.Error(flog.Errorf("SubscribeBatchAck Failed to Nack %s: %s", queueName, err))
					}
				}
				// 数量大于0，才追踪
				entryMqConsumer.End()
			}
		}
	}()
}

// 生成事件参数
func (receiver *rabbitConsumer) createEventArgs(page amqp.Delivery) EventArgs {
	return EventArgs{
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
			_ = flog.Errorf("Failed to Get %s: %s", queueName, err)
		}
		if !ok {
			break
		}
		lastPage = msg
		lst.Add(receiver.createEventArgs(msg))
	}

	return lst, lastPage
}

// 创建频道并绑定
func (receiver *rabbitConsumer) createAndBindQueue(chl *amqp.Channel, queueName, routingKey string) (*amqp.Channel, error) {
	if chl == nil || chl.IsClosed() {
		// 创建一个连接和通道
		var err error
		if chl, err = receiver.manager.CreateChannel(); err != nil {
			return chl, err
		}
		receiver.manager.CreateQueue(chl, queueName, receiver.manager.config.IsDurable, receiver.manager.config.AutoDelete, nil)
		receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.config.Exchange, nil)
	}
	return chl, nil
}
