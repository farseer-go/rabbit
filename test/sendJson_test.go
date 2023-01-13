package test

import (
	"encoding/json"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/rabbit"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSendJson(t *testing.T) {
	fs.Initialize[rabbit.Module]("test rabbit")
	// 注册消费者
	consumer := container.Resolve[rabbit.IConsumer]("Ex2")
	// 手动ACK
	consumer.SubscribeAck("TestSingleJsonAck", "Test_Single_Json_Ack", 100, func(message string, ea rabbit.EventArgs) bool {
		var dic map[string]string
		_ = json.Unmarshal([]byte(message), &dic)
		assert.Equal(t, 1, len(dic))

		r := dic["ack"]
		assert.Equal(t, "true", r)
		return true
	})

	// 自动ACK
	consumer.Subscribe("TestSingleJsonAutoAck", "Test_Single_Json_AutoAck", 100, func(message string, ea rabbit.EventArgs) {
		var dic map[string]string
		_ = json.Unmarshal([]byte(message), &dic)
		assert.Equal(t, 1, len(dic))

		r := dic["auto_ack"]
		assert.Equal(t, "true", r)
	})

	product := container.Resolve[rabbit.IProduct]("Ex2")
	_ = product.SendJson(map[string]string{"a": "b"})
	_ = product.SendJsonKey(map[string]string{"ack": "true"}, "Test_Single_Json_Ack")
	_ = product.SendJsonKey(map[string]string{"auto_ack": "true"}, "Test_Single_Json_AutoAck")

	time.Sleep(200 * time.Millisecond)
}
