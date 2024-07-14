package rabbit

type rabbitConfig struct {
	Server     string // 服务端地址
	UserName   string // 用户名
	Password   string // 密码
	MinChannel int32  // 最低保持多少个频道
	MaxChannel int32  // 最多可以创建多少个频道
	Exchange   string // 交换器名称
	RoutingKey string // 路由KEY
	Type       string // 交换器类型（fanout、direct、topic）
	IsDurable  bool   // true：持久化，交换器、消息体、队列，均会设置为持久化
	AutoDelete bool   // 不使用时，是否自动删除
	UseConfirm bool   // 发送消息时，是否需要确保消息发送到服务端
	AutoCreate bool   // 交换器不存在，是否自动创建
}
