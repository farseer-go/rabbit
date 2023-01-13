package rabbit

import "time"

type EventArgs struct {
	ConsumerTag     string
	DeliveryTag     uint64
	Redelivered     bool
	Exchange        string
	RoutingKey      string
	Body            []byte
	BodyString      string
	Headers         map[string]any // Application or header exchange table
	ContentType     string         // MIME content type
	ContentEncoding string         // MIME content encoding
	DeliveryMode    uint8          // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8          // queue implementation use - 0 to 9
	CorrelationId   string         // application use - correlation identifier
	ReplyTo         string         // application use - address to reply to (ex: RPC)
	Expiration      string         // implementation use - message expiration spec
	MessageId       string         // application use - message identifier
	Timestamp       time.Time      // application use - message timestamp
	Type            string         // application use - message type name
	UserId          string         // application use - creating user - should be authenticated user
	AppId           string         // application use - creating application id
	// Valid only with Channel.Get
	MessageCount uint32
}
