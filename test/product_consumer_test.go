package test

import (
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/rabbit"
	"testing"
)

func TestProductConsumer(t *testing.T) {
	fs.Initialize[rabbit.Module]("test rabbit")
	// 注册消费者
	consumer := container.Resolve[rabbit.IConsumer]("Ex1")
	consumer.Subscribe("Q1", func(message string, ea rabbit.EventArgs) {

	})

	// 生产消息
	product := container.Resolve[rabbit.IProduct]("Ex1")
	product.Send("")
}
