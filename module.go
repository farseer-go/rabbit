package rabbit

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/flog"
	"github.com/farseer-go/fs/modules"
)

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return nil
}

func (module Module) Initialize() {
	rabbitConfigs := configure.ParseConfigs[rabbitConfig]("Rabbit")
	for _, rabbitConfig := range rabbitConfigs {
		if rabbitConfig.Server.Server == "" {
			_ = flog.Error("Rabbit配置缺少Server节点")
			continue
		}
		if rabbitConfig.Server.MaxChannelCount == 0 {
			rabbitConfig.Server.MaxChannelCount = 2048
		}
		if rabbitConfig.Server.MinChannelCount == 0 {
			rabbitConfig.Server.MinChannelCount = 10
		}

		// 遍历交换器
		for _, exchange := range rabbitConfig.Exchange {
			if exchange.ExchangeName == "" {
				_ = flog.Errorf("Rabbit配置：%s 缺少ExchangeName", rabbitConfig.Server.Server)
				continue
			}

			// 注册生产者
			productIns := newProduct(rabbitConfig.Server, exchange)
			container.RegisterInstance[IProduct](productIns, exchange.ExchangeName)

			// 注册消费者
			consumerIns := newConsumer(rabbitConfig.Server, exchange)
			container.RegisterInstance[IConsumer](consumerIns, exchange.ExchangeName)
		}
	}
}
