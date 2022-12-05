package rabbit

import (
	"github.com/streadway/amqp"
	"time"
)

type channel struct {
	ch         *amqp.Channel
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

type Publishing struct {
	Headers         map[string]interface{}
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id
	// The application specific payload of the message
	Body []byte
}

type publishParams struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       Publishing
}

// Publish
func (c *channel) Publish(params publishParams) {

}
