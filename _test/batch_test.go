package test

import (
	"testing"
	"time"

	"github.com/farseer-go/collections"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/flog"
	"github.com/farseer-go/fs/parse"
	"github.com/farseer-go/fs/snc"
	"github.com/farseer-go/rabbit"
	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	C1 := 0
	C2 := 0
	// 注册消费者
	consumer := container.Resolve[rabbit.IConsumer]("Ex3")
	assert.Panics(t, func() {
		consumer.SubscribeBatchAck("", "", -1, func(messages collections.List[rabbit.EventArgs]) bool { return true })
	})
	assert.Panics(t, func() {
		consumer.SubscribeBatch("", "", 0, func(messages collections.List[rabbit.EventArgs]) {})
	})

	// 手动ACK
	consumer.SubscribeBatchAck("TestBatchStringAck", "Test_Batch_String_Ack", 100, func(messages collections.List[rabbit.EventArgs]) bool {
		for _, args := range messages.ToArray() {
			var index string
			snc.Unmarshal([]byte(args.BodyString), &index)
			C1 += parse.ToInt(index)
		}
		return true
	})

	// 自动ACK
	consumer.SubscribeBatch("TestBatchStringAutoAck", "Test_Batch_String_AutoAck", 100, func(messages collections.List[rabbit.EventArgs]) {
		for _, args := range messages.ToArray() {
			var index string
			snc.Unmarshal([]byte(args.BodyString), &index)
			C2 += parse.ToInt(index)
		}
	})

	// 生产消息
	product := container.Resolve[rabbit.IProduct]("Ex3")
	lst := collections.NewListAny("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
	product.BatchSendMessage(lst, "")
	_ = product.SendStringKey("\"12\"", "testKey")
	flog.Info("消息发送完成，等待1秒")
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, 67, C1)
	assert.Equal(t, 67, C2)
}
