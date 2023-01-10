package rabbit

type IProduct interface {
	// SendString 发送消息（使用配置设置）
	SendString(message string) error

	// SendJson 发送消息，将data序列化成json（使用配置设置）
	SendJson(data any) error

	// SendStringKey 发送消息（使用配置设置）
	SendStringKey(message, routingKey string) error

	// SendJsonKey 发送消息（使用配置设置）
	SendJsonKey(data any, routingKey string) error

	// SendMessage 发送消息
	SendMessage(message []byte, routingKey, messageId string, priority uint8) error
}
