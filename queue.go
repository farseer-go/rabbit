package rabbit

type Queue struct {
	Name      string // server confirmed or generated name
	Messages  int    // count of messages not awaiting acknowledgment
	Consumers int    // number of consumers receiving deliveries
}

type queueDeclareParam struct {
	Name       string                 `default:""`
	Durable    bool                   `default:false`
	AutoDelete bool                   `default:false`
	Exclusive  bool                   `default:false`
	NoWait     bool                   `default:false`
	Args       map[string]interface{} `default:nil`
}

func (c *channel) QueueDeclare(pam queueDeclareParam) (Queue, error) {
	q, err := c.ch.QueueDeclare(pam.Name, pam.Durable, pam.AutoDelete, pam.Exclusive, pam.NoWait, pam.Args)
	if err != nil {
		return Queue{}, err
	}
	var queue Queue
	queue.Name = q.Name
	queue.Messages = q.Messages
	queue.Consumers = q.Consumers
	return queue, err
}
