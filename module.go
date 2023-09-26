package rabbit

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/modules"
)

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return nil
}

func (module Module) Initialize() {
	nodes := configure.GetSubNodes("Rabbit")
	for key, val := range nodes {
		configString := val.(string)
		if configString == "" {
			panic("[farseer.yaml]Rabbit." + key + "，没有正确配置")
		}
		// 注册内部上下文
		Register(key, configString)
	}
}
