package rabbit

import (
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/farseer-go/fs/flog"
)

func Register(config rabbitConfig) {
	if config.Server.Server == "" {
		_ = flog.Error("Rabbit配置缺少Server节点")
		return
	}
	if config.Server.MaxChannelCount == 0 {
		config.Server.MaxChannelCount = 2048
	}
	if config.Server.MinChannelCount == 0 {
		config.Server.MinChannelCount = 10
	}

	// 遍历交换器
	for _, exchange := range config.Exchange {
		if exchange.ExchangeName == "" {
			_ = flog.Errorf("Rabbit配置：%s 缺少ExchangeName", config.Server.Server)
			continue
		}

		// 注册生产者
		productIns := newProduct(config.Server, exchange)
		container.RegisterInstance[IProduct](productIns, exchange.ExchangeName)

		// 注册消费者
		consumerIns := newConsumer(config.Server, exchange)
		container.RegisterInstance[IConsumer](consumerIns, exchange.ExchangeName)
	}

	// 注册健康检查
	container.RegisterInstance[core.IHealthCheck](&healthCheck{server: config.Server}, config.Server.Server)
}
