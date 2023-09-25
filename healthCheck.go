package rabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type healthCheck struct {
	server serverConfig
}

func (c *healthCheck) Check() (string, error) {
	manager := newManager(c.server, exchangeConfig{})
	err := manager.Open()
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(manager.conn)

	return fmt.Sprintf("Rabbit.%s => Version %s", c.server.Server, manager.conn.Properties["version"]), err
}
