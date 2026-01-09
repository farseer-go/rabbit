package rabbit

import (
	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/container"
	"github.com/farseer-go/fs/core"
	"github.com/farseer-go/fs/modules"
)

type Module struct {
}

func (module Module) DependsModule() []modules.FarseerModule {
	return nil
}

func (module Module) PreInitialize() {
	// 注册包级别的连接检查器（默认实现）
	container.Register(func() core.IConnectionChecker { return &connectionChecker{} }, "rabbit")
}

func (module Module) Initialize() {
	nodes := configure.GetSubNodes("Rabbit")
	for key, val := range nodes {
		configString := val.(string)
		if configString == "" {
			panic("[farseer.yaml]Rabbit." + key + "，配置不正确")
		}
		// 注册内部上下文
		Register(key, configString)
	}
}
