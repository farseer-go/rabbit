package rabbit

import (
	"fmt"
	"github.com/farseer-go/fs/flog"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitProduct struct {
	Server   serverConfig
	Exchange exchangeConfig
}

func newProduct(server serverConfig, exchange exchangeConfig) rabbitProduct {
	return rabbitProduct{
		Server:   server,
		Exchange: exchange,
	}
}
func (receiver rabbitProduct) Send(message string) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", receiver.Server.UserName, receiver.Server.Password, receiver.Server.Server))
	defer conn.Close()
	if err != nil {
		flog.Panicf("Failed to connect to RabbitMQ %s: %s", receiver.Server.Server, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		flog.Panicf("Failed to open a channel %s: %s", receiver.Server.Server, err)
	}
	defer ch.Close()

	_, _ = ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
}
