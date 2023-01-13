package test

import (
	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/parse"
	"github.com/farseer-go/rabbit"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	fs.Initialize[rabbit.Module]("test rabbit")
	C1 := 0
	C2 := 0
	// 注册消费者
	consumer := container.Resolve[rabbit.IConsumer]("Ex3")
	// 手动ACK
	consumer.SubscribeBatchAck("TestBatchStringAck", "Test_Batch_String_Ack", 100, func(messages collections.List[rabbit.EventArgs]) bool {
		for _, args := range messages.ToArray() {
			C1 += parse.Convert(args.BodyString, 0)
		}
		return true
	})

	// 自动ACK
	consumer.SubscribeBatch("TestBatchStringAutoAck", "Test_Batch_String_AutoAck", 100, func(messages collections.List[rabbit.EventArgs]) {
		for _, args := range messages.ToArray() {
			C2 += parse.Convert(args.BodyString, 0)
		}
	})

	// 生产消息
	product := container.Resolve[rabbit.IProduct]("Ex3")
	for i := 1; i < 11; i++ {
		_ = product.SendString(strconv.Itoa(i))
	}
	_ = product.SendStringKey("12", "testKey")
	time.Sleep(2000 * time.Millisecond)
	assert.Equal(t, 67, C1)
	assert.Equal(t, 67, C2)
}
