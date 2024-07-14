package rabbit

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/farseer-go/fs/flog"
)

func Register(key string, configString string) {
	config := configure.ParseString[rabbitConfig](configString)
	if config.Server == "" {
		_ = flog.Error("Rabbit配置缺少Server节点")
		return
	}
	if config.MaxChannel == 0 {
		config.MaxChannel = 2048
	}
	if config.MinChannel == 0 {
		config.MinChannel = 10
	}

	if config.MinChannel > config.MaxChannel {
		config.MaxChannel = config.MinChannel
	}
	if config.Exchange == "" {
		_ = flog.Errorf("Rabbit配置：%s 缺少ExchangeName", config.Server)
		return
	}

	// 注册生产者
	productIns := newProduct(config)
	container.RegisterInstance[IProduct](productIns, key)

	// 注册消费者
	consumerIns := newConsumer(config)
	container.RegisterInstance[IConsumer](consumerIns, key)

	// 注册健康检查
	container.RegisterInstance[core.IHealthCheck](&healthCheck{name: key, config: config}, key)
}
