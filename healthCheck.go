package rabbit

import amqp "github.com/rabbitmq/amqp091-go"

type healthCheck struct {
	server serverConfig
}

func (c *healthCheck) Check() (string, error) {
	manager := newManager(c.server, exchangeConfig{})
	err := manager.Open()
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(manager.conn)
	return "Rabbit." + c.server.Server, err
}
