package rabbit

type IConsumer interface {
	// Subscribe 订阅队列
	// queueName：队列名称
	// consumerHandle：消费逻辑
	Subscribe(queueName string, consumerHandle func(message string, ea EventArgs))
}
