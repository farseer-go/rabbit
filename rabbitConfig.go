package rabbit

type rabbitConfig struct {
	Server   serverConfig
	Exchange []exchangeConfig
}

type exchangeConfig struct {
	ExchangeName       string // 交换器名称
	RoutingKey         string // RoutingKey
	ExchangeType       string // 交换器类型
	UseConfirmModel    bool   // 是否需要ACK
	AutoCreateExchange bool   // 交换器不存在，是否自动创建
}

// rabbitConfig 配置项
type serverConfig struct {
	Server   string // 服务端地址
	UserName string // 用户名
	Password string // 密码
}
