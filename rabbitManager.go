package rabbit

import (
	"fmt"
	"github.com/farseer-go/fs/flog"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

// rabbit客户端管理，可以对raabit做交换器创建、队列创建，以及绑定操作。
type rabbitManager struct {
	server   serverConfig
	exchange exchangeConfig
	conn     *amqp.Connection
	lock     *sync.Mutex
}

// 创建实例
func newManager(server serverConfig, exchange exchangeConfig) *rabbitManager {
	return &rabbitManager{
		server:   server,
		exchange: exchange,
		lock:     &sync.Mutex{},
	}
}

// Open 连接服务端
func (receiver *rabbitManager) Open() {
	if receiver.conn == nil || receiver.conn.IsClosed() {
		receiver.lock.Lock()
		defer receiver.lock.Unlock()
		if receiver.conn == nil || receiver.conn.IsClosed() {
			var err error
			receiver.conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", receiver.server.UserName, receiver.server.Password, receiver.server.Server))
			if err != nil {
				flog.Panicf("Failed to connect to RabbitMQ %s: %s", receiver.server.Server, err)
			}
			receiver.CreateExchange(receiver.exchange.ExchangeName, receiver.exchange.ExchangeType, receiver.exchange.IsDurable, receiver.exchange.AutoDelete, nil)
		}
	}
}

// CreateExchange 创建交换器
func (receiver *rabbitManager) CreateExchange(exchangeName, exchangeType string, isDurable, autoDelete bool, args amqp.Table) {
	if receiver.exchange.AutoCreateExchange {
		c, err := receiver.conn.Channel()
		defer c.Close()

		err = c.ExchangeDeclare(exchangeName, exchangeType, isDurable, autoDelete, false, false, args)
		if err != nil {
			flog.Panicf("Failed to Declare Exchange %s: %s", receiver.server.Server, err)
		}
	}
}

// CreateChannel 创建通道
func (receiver *rabbitManager) CreateChannel() *amqp.Channel {
	receiver.Open()
	c, err := receiver.conn.Channel()
	if err != nil {
		flog.Panicf("Failed to Open a channel %s: %s", receiver.server.Server, err)
	}
	return c
}

// CreateQueue 创建队列
func (receiver *rabbitManager) CreateQueue(c *amqp.Channel, queueName string, isDurable, autoDelete bool, args amqp.Table) {
	_, err := c.QueueDeclare(queueName, isDurable, autoDelete, false, false, args)
	if err != nil {
		flog.Panicf("Failed to Declare Exchange %s: %s", receiver.server.Server, err)
	}
}

// BindQueue 创建队列
func (receiver *rabbitManager) BindQueue(c *amqp.Channel, queueName, routingKey, exchangeName string, args amqp.Table) {
	err := c.QueueBind(queueName, routingKey, exchangeName, false, args)
	if err != nil {
		flog.Panicf("Failed to QueueBind %s: %s", receiver.server.Server, err)
	}
}
