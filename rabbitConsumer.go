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

func newConsumer(server serverConfig, exchange exchangeConfig) *rabbitConsumer {
	return &rabbitConsumer{
		manager: newManager(server, exchange),
	}
}

func (receiver *rabbitConsumer) createQueueAndBindAndConsume(queueName, routingKey string, prefetchCount int, autoAck bool) (*amqp.Channel, <-chan amqp.Delivery) {
	// 创建一个连接和通道
	chl := receiver.manager.CreateChannel()
	receiver.manager.CreateQueue(chl, queueName, receiver.manager.exchange.IsDurable, receiver.manager.exchange.AutoDelete, nil)
	receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.exchange.ExchangeName, nil)

	// 设置预读数量
	err := chl.Qos(prefetchCount, 0, false)
	if err != nil {
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
			args := receiver.createEventArgs(page)
			exception.Try(func() {
				consumerHandle(string(page.Body), args)
			}).CatchException(func(exp any) {
				_ = flog.Errorf("Subscribe exception:%s", exp)
			})
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
			args := receiver.createEventArgs(page)
			isSuccess := false
			exception.Try(func() {
				isSuccess = consumerHandle(string(page.Body), args)
				if isSuccess {
					if err := page.Ack(false); err != nil {
						_ = flog.Errorf("Failed to Ack %s: %s %s", queueName, err, string(page.Body))
					}
				}
			}).CatchException(func(exp any) {
				_ = flog.Errorf("SubscribeAck exception:%s", exp)
			})
			if !isSuccess {
				// Nack
				if err := page.Nack(false, true); err != nil {
					_ = flog.Errorf("Failed to Nack %s: %s %s", queueName, err, string(page.Body))
				}
			}
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
		for {
			if chl == nil || chl.IsClosed() {
				// 创建一个连接和通道
				chl = receiver.manager.CreateChannel()
				receiver.manager.CreateQueue(chl, queueName, receiver.manager.exchange.IsDurable, receiver.manager.exchange.AutoDelete, nil)
				receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.exchange.ExchangeName, nil)
			}

			lst, _ := receiver.pullBatch(queueName, true, pullCount, chl)
			if lst.Count() > 0 {
				exception.Try(func() {
					consumerHandle(lst)
				}).CatchException(func(exp any) {
					_ = flog.Errorf("SubscribeBatch exception:%s", exp)
				})
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

func (receiver *rabbitConsumer) SubscribeBatchAck(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs]) bool) {
	if pullCount < 1 {
		flog.Panicf("The parameter pullCount must be greater than 0， %s: %d", queueName, pullCount)
	}

	go func() {
		var chl *amqp.Channel
		for {
			if chl == nil || chl.IsClosed() {
				// 创建一个连接和通道
				chl = receiver.manager.CreateChannel()
				receiver.manager.CreateQueue(chl, queueName, receiver.manager.exchange.IsDurable, receiver.manager.exchange.AutoDelete, nil)
				receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.exchange.ExchangeName, nil)
			}

			lst, lastPage := receiver.pullBatch(queueName, false, pullCount, chl)
			if lst.Count() > 0 {
				isSuccess := false
				exception.Try(func() {
					isSuccess = consumerHandle(lst)
					if isSuccess {
						if err := lastPage.Ack(true); err != nil {
							_ = flog.Errorf("Failed to Ack %s: %s", queueName, err)
						}
					}
				}).CatchException(func(exp any) {
					_ = flog.Errorf("SubscribeBatchAck exception:%s", exp)
				})
				if !isSuccess {
					// Nack
					if err := lastPage.Nack(true, true); err != nil {
						_ = flog.Errorf("Failed to Nack %s: %s", queueName, err)
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
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
