package rabbit

type IConsumer interface {
	// Subscribe 订阅队列，不需要ACK
	// queueName：队列名称
	// routingKey：路由键
	// consumerHandle：消费逻辑
	Subscribe(queueName string, routingKey string, consumerHandle func(message string, ea EventArgs))

	// SubscribeAck 订阅队列，并需要主动ACK
	// queueName：队列名称
	// routingKey：路由键
	// consumerHandle：消费逻辑
	SubscribeAck(queueName string, routingKey string, consumerHandle func(message string, ea EventArgs) bool)
}
