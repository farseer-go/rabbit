package rabbit

import (
	"context"
	"crypto/md5"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/farseer-go/fs/flog"
	"github.com/farseer-go/fs/parse"
	"github.com/farseer-go/fs/snc"
	"github.com/farseer-go/fs/trace"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitProduct struct {
	manager          *rabbitManager
	deliveryMode     uint8
	chlQueue         chan rabbitChannel // 通道队列，使用完后放回此队列
	workChannelCount int32              // 正在使用的通道数量
	lock             *sync.Mutex
}
type rabbitChannel struct {
	chl      *amqp.Channel
	confirms chan amqp.Confirmation
	err      error
}

func newProduct(config rabbitConfig) *rabbitProduct {
	var deliveryMode uint8
	if config.IsDurable {
		deliveryMode = 2
	}
	return &rabbitProduct{
		deliveryMode: deliveryMode,
		manager:      newManager(config),
		lock:         &sync.Mutex{},
	}
}

// 取出或创建通道
func (receiver *rabbitProduct) popChannel() rabbitChannel {
	receiver.lock.Lock()
	defer receiver.lock.Unlock()

	// 工作队列 + 1
	defer atomic.AddInt32(&receiver.workChannelCount, 1)

	if receiver.chlQueue == nil {
		receiver.init()
	}

	for {
		select {
		case <-time.NewTimer(10 * time.Millisecond).C:
			if receiver.workChannelCount >= receiver.manager.config.MaxChannel {
				flog.Infof("workChannelCount=%d", receiver.workChannelCount)
				continue
			}
			if chl := receiver.createChannelAndConfirm(); chl.err == nil {
				flog.Infof("createChannelAndConfirm")
				return chl
			}
		case rabbitChl := <-receiver.chlQueue:
			// 如果通道是关闭状态，则重新走取出逻辑
			if rabbitChl.err != nil || rabbitChl.chl.IsClosed() {
				flog.ErrorIfExists(rabbitChl.err)
				continue
			}
			return rabbitChl
		}
	}
}

// 初始化本地通道列表
func (receiver *rabbitProduct) init() {
	// 首次使用
	receiver.workChannelCount = 0
	receiver.chlQueue = make(chan rabbitChannel, receiver.manager.config.MaxChannel)
	// 按最低channel要求，创建指定数量的channel
	go func() {
		for (len(receiver.chlQueue) + parse.ToInt(receiver.workChannelCount)) < parse.ToInt(receiver.manager.config.MinChannel) {
			if channel := receiver.createChannelAndConfirm(); channel.chl != nil && !channel.chl.IsClosed() {
				receiver.chlQueue <- channel
			}
		}
	}()
}

// 创建通道
func (receiver *rabbitProduct) createChannelAndConfirm() rabbitChannel {
	chl, err := receiver.manager.CreateChannel()
	if err != nil {
		_ = flog.Error(err)
		return rabbitChannel{
			err: err,
			chl: chl,
		}
	}

	var confirms chan amqp.Confirmation
	confirms, err = receiver.confirm(chl)
	flog.ErrorIfExists(err)
	return rabbitChannel{
		err:      err,
		chl:      chl,
		confirms: confirms,
	}
}

// 通道使用完后，放回队列中
func (receiver *rabbitProduct) pushChannel(rabbitChl rabbitChannel) {
	defer atomic.AddInt32(&receiver.workChannelCount, -1)
	if rabbitChl.err != nil {
		return
	}
	receiver.chlQueue <- rabbitChl
}

// SendString 发送消息（使用配置设置）
func (receiver *rabbitProduct) SendString(message string) error {
	messageId := fmt.Sprintf("%x", md5.Sum([]byte(message)))
	return receiver.SendMessage([]byte(message), receiver.manager.config.RoutingKey, messageId, 0)
}

// SendJson 发送消息，将data序列化成json（使用配置设置）
func (receiver *rabbitProduct) SendJson(data any) error {
	message, _ := snc.Marshal(data)
	messageId := fmt.Sprintf("%x", md5.Sum(message))
	return receiver.SendMessage(message, receiver.manager.config.RoutingKey, messageId, 0)
}

// SendStringKey 发送消息（使用配置设置）
func (receiver *rabbitProduct) SendStringKey(message, routingKey string) error {
	messageId := fmt.Sprintf("%x", md5.Sum([]byte(message)))
	return receiver.SendMessage([]byte(message), routingKey, messageId, 0)
}

// SendJsonKey 发送消息（使用配置设置）
func (receiver *rabbitProduct) SendJsonKey(data any, routingKey string) error {
	traceTest := container.Resolve[trace.IManager]().TraceHand("snc.Marshal")
	message, _ := snc.Marshal(data)
	traceTest.End(nil)
	traceTest = container.Resolve[trace.IManager]().TraceHand("md5.Sum")
	messageId := fmt.Sprintf("%x", md5.Sum(message))
	traceTest.End(nil)
	return receiver.SendMessage(message, routingKey, messageId, 0)
}

// SendMessage 发送消息
func (receiver *rabbitProduct) SendMessage(message []byte, routingKey, messageId string, priority uint8) error {
	traceDetailMq := receiver.manager.traceManager.TraceMqSend("Send", receiver.manager.config.Server, receiver.manager.config.Exchange, receiver.manager.config.RoutingKey)
	rabbitChl := receiver.popChannel()
	defer func(rabbitChl rabbitChannel) {
		receiver.pushChannel(rabbitChl)
	}(rabbitChl)

	// 发布消息
	err := rabbitChl.chl.PublishWithContext(context.Background(),
		receiver.manager.config.Exchange, // exchange
		routingKey,                       // routing key
		false,                            // mandatory
		false,                            // immediate
		amqp.Publishing{
			Headers:       nil,
			DeliveryMode:  receiver.deliveryMode,
			Priority:      priority,
			MessageId:     messageId,
			Body:          message,
			AppId:         core.AppName,
			CorrelationId: trace.GetTraceId(),
		})

	defer func() { traceDetailMq.End(err) }()
	if err != nil {
		//if rabbitError, ok := err.(*amqp.Error); ok {
		//	if rabbitError.Code == 504 {
		//
		//	}
		//}
		return flog.Errorf("failed to Publish %s: %s", receiver.manager.config.Server, err)
	}

	// 确认消息
	if rabbitChl.confirms != nil {
		if confirmed := <-rabbitChl.confirms; !confirmed.Ack {
			return flog.Errorf("NoAck to Publish %s: %s", receiver.manager.config.Server, messageId)
		}
	}
	return nil
}

// 是否需要消息确认
func (receiver *rabbitProduct) confirm(chl *amqp.Channel) (chan amqp.Confirmation, error) {
	var confirms chan amqp.Confirmation
	if receiver.manager.config.UseConfirm {
		confirms = chl.NotifyPublish(make(chan amqp.Confirmation, 1))
		if err := chl.Confirm(false); err != nil {
			return nil, fmt.Errorf("failed to Confirm %s: %s", receiver.manager.config.Server, err)
		}
	}
	return confirms, nil
}
