package rabbit

import (
	"fmt"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/trace"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

// rabbit客户端管理，可以对rabbit做交换器创建、队列创建，以及绑定操作。
type rabbitManager struct {
	config       rabbitConfig
	conn         *amqp.Connection
	lock         *sync.Mutex
	traceManager trace.IManager
}

// 创建实例
func newManager(config rabbitConfig) *rabbitManager {
	return &rabbitManager{
		config:       config,
		lock:         &sync.Mutex{},
		traceManager: container.Resolve[trace.IManager](),
	}
}

// Open 连接服务端
func (receiver *rabbitManager) Open() error {
	if receiver.conn == nil || receiver.conn.IsClosed() {
		receiver.lock.Lock()
		defer receiver.lock.Unlock()

		if receiver.conn == nil || receiver.conn.IsClosed() {
			traceDetail := receiver.traceManager.TraceMq("Open", receiver.config.Server, receiver.config.Exchange)

			var err error
			receiver.conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", receiver.config.UserName, receiver.config.Password, receiver.config.Server))
			defer func() { traceDetail.End(err) }()
			if err != nil {
				return fmt.Errorf("failed to connect to RabbitMQ %s: %s", receiver.config.Server, err)
			}
		}
	}
	return nil
}

// CreateChannel 创建通道
func (receiver *rabbitManager) CreateChannel() (*amqp.Channel, error) {
	var err error
	traceDetail := receiver.traceManager.TraceMq("CreateChannel", receiver.config.Server, receiver.config.Exchange)
	defer func() { traceDetail.End(err) }()

	// 连接rabbit
	if err = receiver.Open(); err != nil {
		return nil, err
	}

	// 打开通道
	var c *amqp.Channel
	if c, err = receiver.conn.Channel(); err != nil {
		return nil, fmt.Errorf("failed to Open a channel %s: %s", receiver.config.Server, err)
	}
	return c, nil
}

// CreateExchange 创建交换器
func (receiver *rabbitManager) CreateExchange(c *amqp.Channel, exchangeName, exchangeType string, isDurable, autoDelete bool, args amqp.Table) error {
	if receiver.config.AutoCreate {
		var err error
		traceDetail := receiver.traceManager.TraceMq("CreateExchange", receiver.config.Server, receiver.config.Exchange)
		defer func() { traceDetail.End(err) }()

		// 创建交换器
		err = c.ExchangeDeclare(exchangeName, exchangeType, isDurable, autoDelete, false, false, args)
		if err != nil {
			return fmt.Errorf("failed to Declare Exchange %s: %s", receiver.config.Server, err)
		}
	}
	return nil
}

// CreateQueue 创建队列
func (receiver *rabbitManager) CreateQueue(c *amqp.Channel, queueName string, isDurable, autoDelete bool, args amqp.Table) error {
	traceDetail := receiver.traceManager.TraceMq("CreateQueue", receiver.config.Server, receiver.config.Exchange)
	_, err := c.QueueDeclare(queueName, isDurable, autoDelete, false, false, args)
	if err != nil {
		err = fmt.Errorf("failed to Declare Exchange %s: %s", receiver.config.Server, err)
	}
	traceDetail.End(err)
	return err
}

// BindQueue 创建队列
func (receiver *rabbitManager) BindQueue(c *amqp.Channel, queueName, routingKey, exchangeName string, args amqp.Table) error {
	traceDetail := receiver.traceManager.TraceMq("BindQueue", receiver.config.Server, receiver.config.Exchange)
	err := c.QueueBind(queueName, routingKey, exchangeName, false, args)
	if err != nil {
		err = fmt.Errorf("failed to QueueBind %s: %s", receiver.config.Server, err)
	}
	traceDetail.End(err)
	return err
}
