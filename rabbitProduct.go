package rabbit

type rabbitProduct struct {
	Server   serverConfig
	Exchange exchangeConfig
}

func newProduct(server serverConfig, exchange exchangeConfig) rabbitProduct {
	return rabbitProduct{
		Server:   server,
		Exchange: exchange,
	}
}
func (receiver rabbitProduct) Send(message string) {

}
