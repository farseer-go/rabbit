package rabbit

import (
	"github.com/streadway/amqp"
)

type channel struct {
	ch         *amqp.Channel
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

type PublishParams struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp.Publishing
}

// Publish 发布
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
