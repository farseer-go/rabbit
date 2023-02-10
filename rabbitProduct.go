package rabbit

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/farseer-go/fs"
	"github.com/farseer-go/fs/flog"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"sync/atomic"
	"time"
)

type rabbitProduct struct {
	manager          *rabbitManager
	deliveryMode     uint8
	chlQueue         chan rabbitChannel // 通道队列，打开通道后，不关系，放回此队列
	workChannelCount int32              // 正在使用的通道数量
	lock             *sync.Mutex
}
type rabbitChannel struct {
	chl      *amqp.Channel
	confirms chan amqp.Confirmation
}

func newProduct(server serverConfig, exchange exchangeConfig) *rabbitProduct {
	var deliveryMode uint8
	if exchange.IsDurable {
		deliveryMode = 2
	}
	return &rabbitProduct{
		deliveryMode: deliveryMode,
		manager:      newManager(server, exchange),
		lock:         &sync.Mutex{},
	}
}

// 取出或创建通道
func (receiver *rabbitProduct) popChannel() rabbitChannel {
	receiver.lock.Lock()
	defer receiver.lock.Unlock()

	if receiver.chlQueue == nil {
		receiver.init()
	}

	timer := time.NewTimer(10 * time.Millisecond)
	for {
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C:
			if receiver.workChannelCount >= receiver.manager.server.MaxChannelCount {
				continue
			}
			receiver.workChannelCount++
			return receiver.createChannelAndConfirm()
		case rabbitChl := <-receiver.chlQueue:
			receiver.workChannelCount++
			return rabbitChl
		}
	}
}

func (receiver *rabbitProduct) init() {
	// 首次使用
	if receiver.chlQueue == nil {
		receiver.chlQueue = make(chan rabbitChannel, 2048)
		// 按最低channel要求，创建指定数量的channel
		for len(receiver.chlQueue) < receiver.manager.server.MinChannelCount {
			receiver.chlQueue <- receiver.createChannelAndConfirm()
		}
	}
}
func (receiver *rabbitProduct) createChannelAndConfirm() rabbitChannel {
	chl := receiver.manager.CreateChannel()
	return rabbitChannel{
		chl:      chl,
		confirms: receiver.confirm(chl),
	}
}

// 创建通道
func (receiver *rabbitProduct) pushChannel(rabbitChl rabbitChannel) {
	defer atomic.AddInt32(&receiver.workChannelCount, -1)
	receiver.chlQueue <- rabbitChl
}

// SendString 发送消息（使用配置设置）
func (receiver *rabbitProduct) SendString(message string) error {
	messageId := fmt.Sprintf("%x", md5.Sum([]byte(message)))
	return receiver.SendMessage([]byte(message), receiver.manager.exchange.RoutingKey, messageId, 0)
}

// SendJson 发送消息，将data序列化成json（使用配置设置）
func (receiver *rabbitProduct) SendJson(data any) error {
	message, _ := json.Marshal(data)
	messageId := fmt.Sprintf("%x", md5.Sum(message))
	return receiver.SendMessage(message, receiver.manager.exchange.RoutingKey, messageId, 0)
}

// SendStringKey 发送消息（使用配置设置）
func (receiver *rabbitProduct) SendStringKey(message, routingKey string) error {
	messageId := fmt.Sprintf("%x", md5.Sum([]byte(message)))
	return receiver.SendMessage([]byte(message), routingKey, messageId, 0)
}

// SendJsonKey 发送消息（使用配置设置）
func (receiver *rabbitProduct) SendJsonKey(data any, routingKey string) error {
	message, _ := json.Marshal(data)
	messageId := fmt.Sprintf("%x", md5.Sum(message))
	return receiver.SendMessage(message, routingKey, messageId, 0)
}

// SendMessage 发送消息
func (receiver *rabbitProduct) SendMessage(message []byte, routingKey, messageId string, priority uint8) error {
	rabbitChl := receiver.popChannel()
	defer func(rabbitChl rabbitChannel) {
		receiver.pushChannel(rabbitChl)
	}(rabbitChl)

	// 发布消息
	err := rabbitChl.chl.PublishWithContext(context.Background(),
		receiver.manager.exchange.ExchangeName, // exchange
		routingKey,                             // routing key
		false,                                  // mandatory
		false,                                  // immediate
		amqp.Publishing{
			Headers:      nil,
			DeliveryMode: receiver.deliveryMode,
			Priority:     priority,
			MessageId:    messageId,
			AppId:        fs.AppName,
			Body:         message,
		})
	if err != nil {
		return flog.Errorf("Failed to Publish %s: %s", receiver.manager.server.Server, err)
	}

	// 确认消息
	if rabbitChl.confirms != nil {
		if confirmed := <-rabbitChl.confirms; !confirmed.Ack {
			return flog.Errorf("NoAck to Publish %s: %s", receiver.manager.server.Server, messageId)
		}
	}
	return nil
}

// 是否需要消息确认
func (receiver *rabbitProduct) confirm(chl *amqp.Channel) chan amqp.Confirmation {
	var confirms chan amqp.Confirmation
	if receiver.manager.exchange.UseConfirmModel {
		confirms = chl.NotifyPublish(make(chan amqp.Confirmation, 1))
		if err := chl.Confirm(false); err != nil {
			_ = flog.Errorf("Failed to Confirm %s: %s", receiver.manager.server.Server, err)
			return nil
		}
	}
	return confirms
}
