package rabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type healthCheck struct {
	name   string
	config rabbitConfig
}

func (c *healthCheck) Check() (string, error) {
	manager := newManager(c.config)
	err := manager.Open()
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(manager.conn)

	return fmt.Sprintf("Rabbit.%s => Version %s", c.name, manager.conn.Properties["version"]), err
}
