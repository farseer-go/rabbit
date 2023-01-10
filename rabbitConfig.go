package rabbit

type rabbitConfig struct {
	Server   serverConfig
	Exchange []exchangeConfig
}

type exchangeConfig struct {
	ExchangeName       string // 交换器名称
	RoutingKey         string // RoutingKey
	ExchangeType       string // 交换器类型
	IsDurable          bool   // true：持久化，交换器、消息体、队列，均会设置为持久化
	AutoDelete         bool   // 不使用时，是否自动删除
	UseConfirmModel    bool   // 是否需要ACK
	AutoCreateExchange bool   // 交换器不存在，是否自动创建
}

// rabbitConfig 配置项
type serverConfig struct {
	Server          string // 服务端地址
	UserName        string // 用户名
	Password        string // 密码
	MaxChannelCount int32  // 最多可以创建多少个频道
	MinChannelCount int    // 最多可以创建多少个频道
}
