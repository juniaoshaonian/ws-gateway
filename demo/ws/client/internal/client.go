package internal

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	apiv1 "gitee.com/flycash/ws-gateway/api/proto/gen/gatewayapi/v1"
	"gitee.com/flycash/ws-gateway/pkg/codec"
	"gitee.com/flycash/ws-gateway/pkg/encrypt"
	"gitee.com/flycash/ws-gateway/pkg/jwt"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// WebSocketClient WebSocket客户端，表示一个连接的生命周期
type WebSocketClient struct {
	wsURL          string
	conn           net.Conn
	tokenGenerator *jwt.UserToken
	codec          codec.Codec
	encryptor      encrypt.Encryptor
	bizID          int64
	userID         int64
	stats          *ClientStats
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	once           sync.Once
	stopChan       chan struct{}
	debug          bool // 调试模式
}

// NewWebSocketClient 创建新的WebSocket客户端
// wsURL: WebSocket服务器URL
// bizID: 业务ID
// userID: 用户ID
// stats: 统计实现（依赖注入）
func NewWebSocketClient(wsURL string, bizID, userID int64, stats *ClientStats) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &WebSocketClient{
		wsURL:          wsURL,
		tokenGenerator: initTokenGenerator(),
		codec:          codec.NewJSONCodec(),
		encryptor:      initEncryptor(),
		bizID:          bizID,
		userID:         userID,
		stats:          stats,
		ctx:            ctx,
		cancel:         cancel,
		once:           sync.Once{},
		stopChan:       make(chan struct{}),
		debug:          false, // 默认关闭调试模式
	}
}

// initTokenGenerator 初始化token生成器
func initTokenGenerator() *jwt.UserToken {
	return jwt.NewUserToken(jwt.UserJWTKey, "mock")
}

// initEncryptor 初始化加密器
func initEncryptor() encrypt.Encryptor {
	// 使用与服务器相同的加密密钥
	encryptor, err := encrypt.NewAESEncryptor("1234567890abcdef1234567890abcdef")
	if err != nil {
		log.Fatalf("初始化加密器失败: %v", err)
	}
	return encryptor
}

// Connect 建立WebSocket连接
func (c *WebSocketClient) Connect(ctx context.Context) error {
	// 生成JWT token
	claims := jwt.UserClaims{
		UserID: c.userID,
		BizID:  c.bizID,
	}
	token, err := c.tokenGenerator.Encode(claims)
	if err != nil {
		return fmt.Errorf("生成token失败: %v", err)
	}

	// 构建WebSocket URL
	wsURL := fmt.Sprintf("%s?token=%s", c.wsURL, url.QueryEscape(token))

	// 建立WebSocket连接
	conn, _, _, err := ws.Dial(ctx, wsURL)
	if err != nil {
		return fmt.Errorf("连接失败: %v", err)
	}

	c.conn = conn
	return nil
}

// Start 启动消息发送流程
func (c *WebSocketClient) Start(ctx context.Context, messagesPerSecond int, testMessage string) error {
	if c.conn == nil {
		return fmt.Errorf("客户端未连接")
	}

	c.stats.IncrementConnections()
	defer c.stats.DecrementConnections()

	// 启动消息循环
	go func() {
		if err := c.startMessageLoop(ctx); err != nil {
			log.Printf("客户端 %d 消息循环错误: %v", c.userID, err)
		}
	}()

	// 启动消息发送
	go func() {
		// 计算消息发送间隔，确保不会过于频繁
		interval := time.Duration(1000/messagesPerSecond) * time.Millisecond
		if interval < 50*time.Millisecond {
			interval = 50 * time.Millisecond // 最小间隔50ms
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 检查连接状态
				if c.conn == nil {
					return
				}

				// 检查连接是否稳定
				if !c.isConnectionAlive() {
					continue
				}

				if err := c.SendMessage(testMessage); err != nil {
					log.Printf("客户端 %d 发送消息失败: %v", c.userID, err)
					// 如果是连接错误，停止发送
					if isConnectionError(err) {
						return
					}
				}
			}
		}
	}()

	return nil
}

// isConnectionError 检查是否为连接相关错误
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return contains(errStr, "broken pipe") ||
		contains(errStr, "connection reset") ||
		contains(errStr, "connection refused") ||
		contains(errStr, "use of closed network connection") ||
		contains(errStr, "write: broken pipe") ||
		contains(errStr, "read: broken pipe")
}

// contains 检查字符串是否包含子字符串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			func() bool {
				for i := 0; i <= len(s)-len(substr); i++ {
					if s[i:i+len(substr)] == substr {
						return true
					}
				}
				return false
			}())))
}

// SendMessage 发送消息并上报统计
func (c *WebSocketClient) SendMessage(content string) error {
	// 检查连接状态
	if c.conn == nil {
		return fmt.Errorf("连接已断开")
	}

	err := c.SendUpstreamMessage(content)
	c.stats.IncrementMessages(err == nil)
	return err
}

// Stop 停止客户端
func (c *WebSocketClient) Stop() {
	c.once.Do(func() {
		// 发送停止信号
		close(c.stopChan)

		// 取消上下文
		c.cancel()

		// 等待所有goroutine结束
		c.wg.Wait()

		// 关闭连接
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

// startMessageLoop 启动消息循环
func (c *WebSocketClient) startMessageLoop(ctx context.Context) error {
	// 启动心跳
	c.startHeartbeat()

	// 启动连接监控
	go c.monitorConnection(ctx)

	// 等待停止信号或上下文取消
	select {
	case <-c.stopChan:
	case <-ctx.Done():
		c.Stop()
	case <-c.ctx.Done():
	}

	return nil
}

// monitorConnection 监控连接状态
func (c *WebSocketClient) monitorConnection(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			if c.conn == nil {
				continue
			}
			// 检查连接是否还活着
			if !c.isConnectionAlive() {
				c.reconnect(ctx)
			}
		}
	}
}

// isConnectionAlive 检查连接是否还活着
func (c *WebSocketClient) isConnectionAlive() bool {
	if c.conn == nil {
		return false
	}

	// 尝试发送一个ping帧来检测连接状态
	// 这里我们使用一个简单的方法：检查连接是否可写
	deadline := time.Now().Add(100 * time.Millisecond)
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return false
	}

	// 尝试写入一个空字节来检测连接状态
	_, err := c.conn.Write([]byte{})
	if err != nil {
		return false
	}

	// 重置写超时
	c.conn.SetWriteDeadline(time.Time{})
	return true
}

// reconnect 尝试重新连接
func (c *WebSocketClient) reconnect(ctx context.Context) {
	maxRetries := 3
	retryDelay := time.Second

	for i := 0; i < maxRetries; i++ {
		// 关闭旧连接
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}

		// 等待一段时间再重连
		time.Sleep(retryDelay)

		// 尝试重新连接
		connCtx, connCancel := context.WithTimeout(ctx, 10*time.Second)
		err := c.Connect(connCtx)
		connCancel()

		if err == nil {
			return
		}

		log.Printf("客户端 %d 重连失败: %v", c.userID, err)
		retryDelay *= 2 // 指数退避
	}

	log.Printf("客户端 %d 重连失败，已达到最大重试次数", c.userID)
}

// sanitizeMessageContent 清理和验证消息内容
func (c *WebSocketClient) sanitizeMessageContent(content string) (string, error) {
	if content == "" {
		return "", fmt.Errorf("消息内容不能为空")
	}

	// 检查内容长度
	if len(content) > 1024*1024 { // 1MB限制
		return "", fmt.Errorf("消息内容过长，最大支持1MB")
	}

	// 检查是否包含非法字符
	for i, char := range content {
		if char == 0 { // null字符
			return "", fmt.Errorf("消息内容包含非法字符null (位置: %d)", i)
		}
	}

	// 移除首尾空白字符
	content = strings.TrimSpace(content)
	if content == "" {
		return "", fmt.Errorf("消息内容不能为空（去除空白后）")
	}

	return content, nil
}

// generateUpstreamMessage 生成上行消息
func (c *WebSocketClient) generateUpstreamMessage(content string) (*apiv1.Message, error) {
	// 清理和验证消息内容
	cleanContent, err := c.sanitizeMessageContent(content)
	if err != nil {
		return nil, fmt.Errorf("消息内容验证失败: %v", err)
	}

	// 业务消息
	businessMessage := &wrapperspb.StringValue{
		Value: cleanContent,
	}

	// 序列化业务消息
	bytes, err := protojson.Marshal(businessMessage)
	if err != nil {
		return nil, fmt.Errorf("序列化业务消息失败: %v", err)
	}

	// 验证序列化结果
	if len(bytes) == 0 {
		return nil, fmt.Errorf("序列化后的消息为空")
	}

	// 调试模式下打印业务消息序列化信息
	if c.debug {
		log.Printf("客户端 %d 调试 - 业务消息序列化: 原始内容=%q, 序列化后长度=%d",
			c.userID, cleanContent, len(bytes))
		log.Printf("客户端 %d 调试 - 业务消息序列化内容: %s", c.userID, string(bytes))
	}

	// 加密业务消息
	encryptedBody, err := c.encryptor.Encrypt(bytes)
	if err != nil {
		return nil, fmt.Errorf("加密业务消息失败: %v", err)
	}

	// 验证加密结果
	if len(encryptedBody) == 0 {
		return nil, fmt.Errorf("加密后的消息为空")
	}

	// 调试模式下打印加密信息
	if c.debug {
		log.Printf("客户端 %d 调试 - 加密信息: 原始长度=%d, 加密后长度=%d",
			c.userID, len(bytes), len(encryptedBody))

		// 验证加密结果
		decryptedBytes, err := c.encryptor.Decrypt(encryptedBody)
		if err != nil {
			log.Printf("客户端 %d 调试 - 加密验证失败: %v", c.userID, err)
		} else {
			log.Printf("客户端 %d 调试 - 加密验证成功: 解密后长度=%d, 内容=%s",
				c.userID, len(decryptedBytes), string(decryptedBytes))
		}
	}

	// 生成唯一key
	key := generateUniqueKey()

	// 构建上行网关消息
	upstreamMessage := &apiv1.Message{
		Cmd:  apiv1.Message_COMMAND_TYPE_UPSTREAM_MESSAGE,
		Key:  key,
		Body: encryptedBody,
	}

	return upstreamMessage, nil
}

// generateDownstreamAck 生成下行确认消息
func (c *WebSocketClient) generateDownstreamAck(key string) *apiv1.Message {
	return &apiv1.Message{
		Cmd: apiv1.Message_COMMAND_TYPE_DOWNSTREAM_ACK,
		Key: key,
	}
}

// generateHeartbeat 生成心跳消息
func (c *WebSocketClient) generateHeartbeat() *apiv1.Message {
	return &apiv1.Message{
		Cmd: apiv1.Message_COMMAND_TYPE_HEARTBEAT,
		Key: generateUniqueKey(),
	}
}

// generateUniqueKey 生成唯一key
func generateUniqueKey() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// sendMessage 发送消息
func (c *WebSocketClient) sendMessage(msg *apiv1.Message) error {
	// 检查连接状态
	if c.conn == nil {
		return fmt.Errorf("连接已断开")
	}

	// 设置写超时
	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("设置写超时失败: %v", err)
	}
	defer c.conn.SetWriteDeadline(time.Time{}) // 重置超时

	// 序列化消息
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %v", err)
	}

	// 调试模式下打印序列化信息
	if c.debug {
		log.Printf("客户端 %d 调试 - 序列化前消息: Cmd=%v, Key=%s, BodyLen=%d",
			c.userID, msg.GetCmd(), msg.GetKey(), len(msg.GetBody()))
		log.Printf("客户端 %d 调试 - 序列化后payload长度: %d", c.userID, len(payload))

		// 打印序列化后的JSON内容（仅对短消息）
		if len(payload) < 500 {
			log.Printf("客户端 %d 调试 - 序列化后payload内容: %s", c.userID, string(payload))
		}

		// 验证序列化结果
		var testMsg apiv1.Message
		if err := c.codec.Unmarshal(payload, &testMsg); err != nil {
			log.Printf("客户端 %d 调试 - 序列化验证失败: %v", c.userID, err)
		} else {
			log.Printf("客户端 %d 调试 - 序列化验证成功: Cmd=%v, Key=%s, BodyLen=%d",
				c.userID, testMsg.GetCmd(), testMsg.GetKey(), len(testMsg.GetBody()))
		}
	}

	// 发送消息 - 使用标准的WebSocket二进制帧
	err = wsutil.WriteClientMessage(c.conn, ws.OpBinary, payload)
	if err != nil {
		return fmt.Errorf("发送消息失败: %v", err)
	}

	// 确保消息完全写入
	if err := c.conn.(*net.TCPConn).SetWriteBuffer(0); err == nil {
		// 强制刷新缓冲区
		c.conn.(*net.TCPConn).SetWriteBuffer(0)
	}

	// 调试模式下打印详细信息
	if c.debug {
		log.Printf("客户端 %d 调试 - 发送的payload长度: %d", c.userID, len(payload))
		if len(payload) < 200 { // 只打印短消息的详细内容
			log.Printf("客户端 %d 调试 - 发送的payload内容: %s", c.userID, string(payload))
		}
	}

	return nil
}

// receiveMessage 接收消息
func (c *WebSocketClient) receiveMessage() (*apiv1.Message, error) {
	// 读取消息 - 使用标准的WebSocket读取方式
	payload, _, err := wsutil.ReadServerData(c.conn)
	if err != nil {
		return nil, fmt.Errorf("读取消息失败: %v", err)
	}

	// 调试模式下打印接收到的原始数据
	if c.debug {
		log.Printf("客户端 %d 调试 - 接收到原始数据长度: %d", c.userID, len(payload))
		if len(payload) < 200 {
			log.Printf("客户端 %d 调试 - 接收到原始数据内容: %s", c.userID, string(payload))
		}
	}

	// 反序列化消息
	msg := &apiv1.Message{}
	err = c.codec.Unmarshal(payload, msg)
	if err != nil {
		// 调试模式下打印反序列化失败的详细信息
		if c.debug {
			log.Printf("客户端 %d 调试 - 反序列化失败: %v", c.userID, err)
			log.Printf("客户端 %d 调试 - 反序列化失败的payload: %s", c.userID, string(payload))
		}
		return nil, fmt.Errorf("反序列化消息失败: %v", err)
	}

	return msg, nil
}

// handleMessage 处理接收到的消息
func (c *WebSocketClient) handleMessage(msg *apiv1.Message) error {
	switch msg.GetCmd() {
	case apiv1.Message_COMMAND_TYPE_UPSTREAM_ACK:
		return nil

	case apiv1.Message_COMMAND_TYPE_DOWNSTREAM_MESSAGE:
		// 发送下行确认
		ackMsg := c.generateDownstreamAck(msg.GetKey())
		return c.sendMessage(ackMsg)

	case apiv1.Message_COMMAND_TYPE_HEARTBEAT:
		// 心跳消息不打印日志
		return nil

	default:
		log.Printf("收到未知消息类型: %v", msg.GetCmd())
		return nil
	}
}

// SendUpstreamMessage 发送上行消息并等待确认
func (c *WebSocketClient) SendUpstreamMessage(content string) error {
	// 检查连接状态
	if c.conn == nil {
		return fmt.Errorf("连接已断开")
	}

	// 添加连接状态检查，确保连接稳定
	if !c.isConnectionAlive() {
		return fmt.Errorf("连接状态不稳定")
	}

	// 生成上行消息
	msg, err := c.generateUpstreamMessage(content)
	if err != nil {
		return fmt.Errorf("生成上行消息失败: %v", err)
	}

	// 发送消息
	err = c.sendMessage(msg)
	if err != nil {
		return fmt.Errorf("发送上行消息失败: %v", err)
	}

	// 添加短暂延迟，确保消息完全发送
	time.Sleep(10 * time.Millisecond)

	// 等待确认（添加超时）
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ackCancel()

	ackMsg, err := c.receiveMessageWithTimeout(ackCtx)
	if err != nil {
		return fmt.Errorf("接收确认消息失败: %v", err)
	}

	// 处理确认消息
	return c.handleMessage(ackMsg)
}

// startHeartbeat 开始心跳
func (c *WebSocketClient) startHeartbeat() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				heartbeat := c.generateHeartbeat()
				if err := c.sendMessage(heartbeat); err != nil {
					// 只在调试模式下打印心跳发送失败的错误
					if c.debug {
						log.Printf("客户端 %d 发送心跳失败: %v", c.userID, err)
					}
					return
				}
			case <-c.stopChan:
				return
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// receiveMessageWithTimeout 带超时的消息接收
func (c *WebSocketClient) receiveMessageWithTimeout(ctx context.Context) (*apiv1.Message, error) {
	// 创建一个带缓冲的通道用于接收结果
	resultCh := make(chan struct {
		msg *apiv1.Message
		err error
	}, 1)

	go func() {
		msg, err := c.receiveMessage()
		resultCh <- struct {
			msg *apiv1.Message
			err error
		}{msg, err}
	}()

	select {
	case result := <-resultCh:
		return result.msg, result.err
	case <-ctx.Done():
		return nil, fmt.Errorf("接收消息超时: %v", ctx.Err())
	}
}

// SetDebug 设置调试模式
func (c *WebSocketClient) SetDebug(debug bool) {
	c.debug = debug
}
