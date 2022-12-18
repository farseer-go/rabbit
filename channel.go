package rabbit

import (
	"github.com/streadway/amqp"
)

// channel 通道结构体
type channel struct {
	ch         *amqp.Channel
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

// PublishParams 推送结构体
type PublishParams struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp.Publishing
}

// Publish 消息推送
func (c *channel) Publish(params PublishParams) error {
	err := c.ch.Publish(
		params.Exchange,  // exchange
		params.Key,       // routing key
		params.Mandatory, // mandatory
		params.Immediate, // immediate
		params.Msg,
	)
	return err
}

// QueueDeclare 创建队列
func (c *channel) QueueDeclare(pam QueueDeclareParam) (Queue, error) {
	q, err := c.ch.QueueDeclare(pam.Name, pam.Durable, pam.AutoDelete, pam.Exclusive, pam.NoWait, pam.Args)
	if err != nil {
		return Queue{}, err
	}
	var queue Queue
	queue.Name = q.Name
	queue.Messages = q.Messages
	queue.Consumers = q.Consumers
	return queue, err
}

// Ack 设置确认机制
func (c *channel) Ack(tag uint64, multiple bool) error {
	return c.ch.Ack(tag, multiple)
}

// Cancel 取消消息传递
func (c *channel) Cancel(consumer string, noWait bool) error {
	return c.ch.Cancel(consumer, noWait)
}

// Close 关闭通道
func (c *channel) Close() error {
	return c.ch.Close()
}

// Confirm 模式
func (c *channel) Confirm(noWait bool) error {
	return c.ch.Confirm(noWait)
}

// Consume 消费者
func (c *channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) (<-chan amqp.Delivery, error) {
	return c.ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}
