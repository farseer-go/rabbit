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
	rabbitConfigs := configure.ParseConfigs[rabbitConfig]("Rabbit")
	for _, config := range rabbitConfigs {
		Register(config)
	}
}
