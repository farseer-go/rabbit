package rabbit

import (
	"github.com/farseer-go/collections"
)

type IConsumer interface {
	// Subscribe 订阅队列，自动ACK
	// queueName：队列名称
	// routingKey：路由键
	// prefetchCount：预取数量
	// consumerHandle：消费逻辑
	Subscribe(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs))

	// SubscribeAck 订阅队列，手动ACK
	// queueName：队列名称
	// routingKey：路由键
	// prefetchCount：预取数量
	// consumerHandle：消费逻辑
	SubscribeAck(queueName string, routingKey string, prefetchCount int, consumerHandle func(message string, ea EventArgs) bool)

	// SubscribeBatchAck 订阅队列，批量消费，手动ACK
	// queueName：队列名称
	// routingKey：路由键
	// consumerHandle：消费逻辑
	SubscribeBatchAck(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs]) bool)

	// SubscribeBatch 订阅队列，批量消费，自动ACK
	// queueName：队列名称
	// routingKey：路由键
	// consumerHandle：消费逻辑
	SubscribeBatch(queueName string, routingKey string, pullCount int, consumerHandle func(messages collections.List[EventArgs]))
}
