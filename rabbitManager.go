package rabbit

import (
	"fmt"
	"github.com/farseer-go/fs/flog"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

// rabbit客户端管理，可以对raabit做交换器创建、队列创建，以及绑定操作。
type rabbitManager struct {
	config rabbitConfig
	conn   *amqp.Connection
	lock   *sync.Mutex
}

// 创建实例
func newManager(config rabbitConfig) *rabbitManager {
	return &rabbitManager{
		config: config,
		lock:   &sync.Mutex{},
	}
}

// Open 连接服务端
func (receiver *rabbitManager) Open() error {
	if receiver.conn == nil || receiver.conn.IsClosed() {
		receiver.lock.Lock()
		defer receiver.lock.Unlock()
		if receiver.conn == nil || receiver.conn.IsClosed() {
			var err error
			receiver.conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", receiver.config.UserName, receiver.config.Password, receiver.config.Server))
			if err != nil {
				_ = flog.Errorf("Failed to connect to RabbitMQ %s: %s", receiver.config.Server, err)
				return err
			}
			receiver.CreateExchange(receiver.config.Exchange, receiver.config.Type, receiver.config.IsDurable, receiver.config.AutoDelete, nil)
		}
	}
	return nil
}

// CreateExchange 创建交换器
func (receiver *rabbitManager) CreateExchange(exchangeName, exchangeType string, isDurable, autoDelete bool, args amqp.Table) {
	if receiver.config.AutoCreate {
		c, err := receiver.conn.Channel()
		defer c.Close()

		err = c.ExchangeDeclare(exchangeName, exchangeType, isDurable, autoDelete, false, false, args)
		if err != nil {
			flog.Panicf("Failed to Declare Exchange %s: %s", receiver.config.Server, err)
		}
	}
}

// CreateChannel 创建通道
func (receiver *rabbitManager) CreateChannel() *amqp.Channel {
	err := receiver.Open()
	if err != nil {
		return nil
	}
	c, err := receiver.conn.Channel()
	if err != nil {
		flog.Panicf("Failed to Open a channel %s: %s", receiver.config.Server, err)
	}
	return c
}

// CreateQueue 创建队列
func (receiver *rabbitManager) CreateQueue(c *amqp.Channel, queueName string, isDurable, autoDelete bool, args amqp.Table) {
	_, err := c.QueueDeclare(queueName, isDurable, autoDelete, false, false, args)
	if err != nil {
		flog.Panicf("Failed to Declare Exchange %s: %s", receiver.config.Server, err)
	}
}

// BindQueue 创建队列
func (receiver *rabbitManager) BindQueue(c *amqp.Channel, queueName, routingKey, exchangeName string, args amqp.Table) {
	err := c.QueueBind(queueName, routingKey, exchangeName, false, args)
	if err != nil {
		flog.Panicf("Failed to QueueBind %s: %s", receiver.config.Server, err)
	}
}
