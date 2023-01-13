package test

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/parse"
	"github.com/farseer-go/rabbit"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestSendString(t *testing.T) {
	fs.Initialize[rabbit.Module]("test rabbit")
	C1 := 0
	C2 := 0
	// 注册消费者
	consumer := container.Resolve[rabbit.IConsumer]("Ex1")
	// 手动ACK
	consumer.SubscribeAck("TestSingleStringAck", "Test_Single_String_Ack", 100, func(message string, ea rabbit.EventArgs) bool {
		C1 += parse.Convert(message, 0)
		return true
	})

	// 自动ACK
	consumer.Subscribe("TestSingleStringAutoAck", "Test_Single_String_AutoAck", 100, func(message string, ea rabbit.EventArgs) {
		C2 += parse.Convert(message, 0)
	})

	// 生产消息
	product := container.Resolve[rabbit.IProduct]("Ex1")
	for i := 1; i < 11; i++ {
		_ = product.SendString(strconv.Itoa(i))
	}
	_ = product.SendStringKey("12", "testKey")
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 67, C1)
	assert.Equal(t, 67, C2)

	//for {
	//	go func() {
	//		_ = product.SendString("aaaa")
	//	}()
	//	time.Sleep(1 * time.Millisecond)
	//}
	//time.Sleep(100 * time.Second)
}
