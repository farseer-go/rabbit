package rabbit

// Queue 队列结构体
type Queue struct {
	Name      string // server confirmed or generated name
	Messages  int    // count of messages not awaiting acknowledgment
	Consumers int    // number of consumers receiving deliveries
}

// QueueDeclareParam 队列参数
type QueueDeclareParam struct {
	Name       string                 `default:""`
	Durable    bool                   `default:false`
	AutoDelete bool                   `default:false`
	Exclusive  bool                   `default:false`
	NoWait     bool                   `default:false`
	Args       map[string]interface{} `default:nil`
}
