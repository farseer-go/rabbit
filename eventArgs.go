package rabbit

type EventArgs struct {
	consumerTag string
	deliveryTag uint64
	redelivered bool
	exchange    string
	routingKey  string
	properties  IBasicProperties
	body        []byte
}

type IBasicProperties interface {
}
