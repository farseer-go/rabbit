package rabbit

import (
	"errors"
	"io"
	"net"
	"strconv"
)

var errURIScheme = errors.New("AMQP scheme must be either 'amqp://' or 'amqps://'")
var errURIWhitespace = errors.New("URI must not contain whitespace")

var schemePorts = map[string]int{
	"amqp":  5672,
	"amqps": 5671,
}

var defaultURI = URI{
	Scheme:   "amqp",
	Host:     "localhost",
	Port:     5672,
	Username: "guest",
	Password: "guest",
	Vhost:    "/",
}

// URI represents a parsed AMQP URI string.
type URI struct {
	Scheme   string
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

type Connect struct {
	conn io.ReadWriteCloser
}

// New 创建连接
func New(uri URI) (net.Conn, error) {
	var con net.Conn
	var err error
	if uri.Scheme == "" {
		uri.Scheme = defaultURI.Scheme
	} else if uri.Scheme != "amqp" && uri.Scheme != "amqps" {
		err = errors.New("network is wrong(Please select either amqps or amqp)")
		return con, err
	}

	if uri.Host == "" {
		uri.Host = defaultURI.Host
	}
	if uri.Port == 0 {
		uri.Port = defaultURI.Port
	}
	if uri.Username == "" {
		uri.Username = defaultURI.Username
	}
	if uri.Password == "" {
		uri.Password = defaultURI.Password
	}
	if uri.Vhost == "" {
		uri.Vhost = defaultURI.Vhost
	}
	con, err = net.Dial("tcp", uri.Host+":"+strconv.Itoa(uri.Port))
	return con, err
}
