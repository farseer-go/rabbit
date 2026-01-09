package rabbit

import (
	"context"
	"fmt"
	"time"

	"github.com/farseer-go/fs/configure"
	"github.com/farseer-go/fs/core"
)

// 确保实现了IConnectionChecker接口
var _ core.IConnectionChecker = (*connectionChecker)(nil)

type connectionChecker struct{}

// Check 检查连接字符串是否能成功连接到RabbitMQ
// 实现IConnectionChecker接口
func (c *connectionChecker) Check(configString string) (bool, error) {
	if configString == "" {
		return false, fmt.Errorf("连接字符串不能为空")
	}

	// 解析配置字符串
	config := configure.ParseString[rabbitConfig](configString)
	if config.Server == "" {
		return false, fmt.Errorf("Server配置不正确：%s", configString)
	}

	manager := newManager(config)
	err := manager.Open()
	if err != nil {
		return false, fmt.Errorf("连接RabbitMQ失败：%s", err.Error())
	}
	defer manager.Close()

	// 创建通道测试连接
	ch, err := manager.conn.Channel()
	if err != nil {
		return false, fmt.Errorf("创建RabbitMQ通道失败：%s", err.Error())
	}
	defer ch.Close()

	return true, nil
}

// CheckWithTimeout 带超时时间的连接检查
// 实现IConnectionChecker接口，参数为 time.Duration
func (c *connectionChecker) CheckWithTimeout(configString string, timeout time.Duration) (bool, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 创建结果通道
	type result struct {
		success bool
		err     error
	}
	resultChan := make(chan result, 1)

	// 在goroutine中执行连接检查
	go func() {
		success, err := c.Check(configString)
		resultChan <- result{success: success, err: err}
	}()

	// 等待结果或超时
	select {
	case <-ctx.Done():
		return false, fmt.Errorf("连接检查超时，超时时间：%v", timeout)
	case res := <-resultChan:
		return res.success, res.err
	}
}
