package rabbit

type rabbitConsumer struct {
	Server   serverConfig
	Exchange exchangeConfig
}

func newConsumer(server serverConfig, exchange exchangeConfig) rabbitConsumer {
	return rabbitConsumer{
		Server:   server,
		Exchange: exchange,
	}
}
func (receiver rabbitConsumer) Subscribe(queueName string, consumer func(message string, ea EventArgs)) {

}
