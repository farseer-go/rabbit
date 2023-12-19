package rabbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type healthCheck struct {
	name   string
	config rabbitConfig
}

func (receiver *healthCheck) Check() (string, error) {
	manager := newManager(receiver.config)
	defer manager.Close()
	
	// 连接
	err := manager.Open()

	// 创建channel
	var c *amqp.Channel
	if c, err = manager.CreateChannel(); err == nil {
		// 创建交换器
		err = manager.CreateExchange(c, manager.config.Exchange, manager.config.Type, manager.config.IsDurable, manager.config.AutoDelete, nil)
	}
	return fmt.Sprintf("Rabbit.%s => Version %s", receiver.name, manager.conn.Properties["version"]), err
}
