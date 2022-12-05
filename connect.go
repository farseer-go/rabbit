package rabbit

import (
	"github.com/farseer-go/fs/configure"
	"github.com/streadway/amqp"
	"strconv"
)

type Connect struct {
	Conn    *amqp.Connection
	Channel *channel
}

// New 创建连接
func New(rabbitName string) *Connect {
	configString := configure.GetString("Rabbit." + rabbitName)
	if configString == "" {
		panic("[farseer.yaml]找不到相应的配置：Rabbit." + rabbitName)
	}
	rabbitConfig := configure.ParseConfig[rabbitConfig](configString)
	conn, err := amqp.Dial(rabbitConfig.Protocol + "://" + rabbitConfig.Username + ":" + rabbitConfig.Password + "@" + rabbitConfig.Host + ":" + strconv.Itoa(rabbitConfig.Port) + "")
	if err != nil {
		panic(err)
	}
	var connect Connect
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	connect.Channel.ch = ch
	connect.Conn = conn
	return &connect
}
