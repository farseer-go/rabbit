package test

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/rabbit"
	"testing"
	"time"
)

func TestProductConsumer(t *testing.T) {
	fs.Initialize[rabbit.Module]("test rabbit")
	// 注册消费者
	consumer := container.Resolve[rabbit.IConsumer]("Ex1")
	consumer.Subscribe("Q1", "", func(message string, ea rabbit.EventArgs) bool {
		return true
	})

	// 生产消息
	product := container.Resolve[rabbit.IProduct]("Ex1")
	for {
		go func() {
			_ = product.SendString("aaaa")
		}()
		time.Sleep(1 * time.Millisecond)
	}

	time.Sleep(100 * time.Second)
}
