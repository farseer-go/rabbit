package rabbit

import (
	"github.com/farseer-go/fs/flog"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitConsumer struct {
	manager *rabbitManager
}

func newConsumer(server serverConfig, exchange exchangeConfig) rabbitConsumer {
	return rabbitConsumer{
		manager: newManager(server, exchange),
	}
}

func (receiver rabbitConsumer) Subscribe(queueName string, routingKey string, consumer func(message string, ea EventArgs)) {
	chl := receiver.manager.CreateChannel()
	receiver.manager.CreateQueue(chl, queueName, receiver.manager.exchange.IsDurable, receiver.manager.exchange.AutoDelete, nil)
	receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.exchange.ExchangeName, nil)

	deliveries, err := chl.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		flog.Panicf("Failed to Subscribe %s: %s", queueName, err)
	}

	go func(chl *amqp.Channel) {
		for page := range deliveries {
			args := receiver.createEventArgs(page)
			consumer(string(page.Body), args)
		}
		_ = chl.Close()
	}(chl)
}

func (receiver rabbitConsumer) SubscribeAck(queueName string, routingKey string, consumer func(message string, ea EventArgs) bool) {
	chl := receiver.manager.CreateChannel()
	receiver.manager.CreateQueue(chl, queueName, receiver.manager.exchange.IsDurable, receiver.manager.exchange.AutoDelete, nil)
	receiver.manager.BindQueue(chl, queueName, routingKey, receiver.manager.exchange.ExchangeName, nil)

	deliveries, err := chl.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		flog.Panicf("Failed to Subscribe %s: %s", queueName, err)
	}

	go func(chl *amqp.Channel) {
		for page := range deliveries {
			args := receiver.createEventArgs(page)
			if consumer(string(page.Body), args) {
				if err = page.Ack(false); err != nil {
					_ = flog.Errorf("Failed to Ack %s: %s %s", queueName, err, string(page.Body))
				}
			} else {
				if err = page.Nack(false, true); err != nil {
					_ = flog.Errorf("Failed to Nack %s: %s %s", queueName, err, string(page.Body))
				}
			}
		}
		_ = chl.Close()
	}(chl)
}

// 生成事件参数
func (receiver rabbitConsumer) createEventArgs(page amqp.Delivery) EventArgs {
	return EventArgs{
		ConsumerTag:     page.ConsumerTag,
		DeliveryTag:     page.DeliveryTag,
		Redelivered:     page.Redelivered,
		Exchange:        page.Exchange,
		RoutingKey:      page.RoutingKey,
		Body:            page.Body,
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
