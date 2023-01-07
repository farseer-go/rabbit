package rabbit

type IProduct interface {
	Send(message string)
}
