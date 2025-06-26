package main

import (
	"context"
	"flag"
	"log"
	"time"

	"gitee.com/flycash/ws-gateway/demo/ws/client/internal"
)

// 全局统计信息
var stats = &internal.ClientStats{}

func main() {
	// 解析命令行参数
	numClients := flag.Int("clients", 10, "客户端连接数量")
	messagesPerSecond := flag.Int("mps", 1, "每秒发送消息数量")
	testDuration := flag.Duration("duration", 1*time.Minute, "测试持续时间")
	connectionTimeout := flag.Duration("conn-timeout", 10*time.Second, "连接超时时间")
	messageSize := flag.Int("msg-size", 100, "消息大小(字节)")
	serverURL := flag.String("server", "ws://localhost:50051/ws", "WebSocket服务器地址")
	debugMode := flag.Bool("debug", false, "启用调试模式")
	flag.Parse()

	// 初始化统计信息
	stats.StartTime = time.Now()

	log.Printf("开始WebSocket压力测试:")
	log.Printf("  客户端数量: %d", *numClients)
	log.Printf("  每秒消息数: %d", *messagesPerSecond)
	log.Printf("  测试持续时间: %v", *testDuration)
	log.Printf("  连接超时: %v", *connectionTimeout)
	log.Printf("  消息大小: %d 字节", *messageSize)
	log.Printf("  服务器地址: %s", *serverURL)
	log.Printf("  调试模式: %v", *debugMode)

	// 创建测试上下文
	testCtx, testCancel := context.WithTimeout(context.Background(), *testDuration)
	defer testCancel()

	// 生成测试消息
	testMessage := generateTestMessage(*messageSize)

	// 创建并启动客户端
	clients := createAndStartClients(testCtx, *numClients, *serverURL, *connectionTimeout, *messagesPerSecond, testMessage, *debugMode)

	// 启动统计信息打印
	go printStatsPeriodically(testCtx)

	// 等待测试结束
	<-testCtx.Done()
	log.Printf("测试结束")

	// 停止所有客户端
	stopAllClients(clients)

	// 打印最终统计信息
	printStats()
	log.Printf("WebSocket压力测试完成")
}

// createAndStartClients 创建并启动所有客户端
func createAndStartClients(ctx context.Context, numClients int, serverURL string, connectionTimeout time.Duration, messagesPerSecond int, testMessage string, debugMode bool) []*internal.WebSocketClient {
	clients := make([]*internal.WebSocketClient, 0, numClients)

	log.Printf("开始建立 %d 个客户端连接...", numClients)
	connectionStart := time.Now()

	for i := 0; i < numClients; i++ {
		userID := int64(10000 + i)
		bizID := int64(9999)

		// 创建客户端
		client := internal.NewWebSocketClient(serverURL, bizID, userID, stats)

		// 设置调试模式
		client.SetDebug(debugMode)

		// 建立连接
		connCtx, connCancel := context.WithTimeout(context.Background(), connectionTimeout)
		if err := client.Connect(connCtx); err != nil {
			log.Printf("客户端 %d 连接失败: %v", i, err)
			connCancel()
			continue
		}
		connCancel()

		// 启动客户端
		if err := client.Start(ctx, messagesPerSecond, testMessage); err != nil {
			log.Printf("客户端 %d 启动失败: %v", i, err)
			client.Stop()
			continue
		}

		clients = append(clients, client)
		log.Printf("客户端 %d 启动成功", i)
	}

	connectionDuration := time.Since(connectionStart)
	log.Printf("连接建立完成，耗时: %v，成功连接: %d", connectionDuration, len(clients))

	return clients
}

// stopAllClients 停止所有客户端
func stopAllClients(clients []*internal.WebSocketClient) {
	log.Printf("正在停止所有客户端...")
	for _, client := range clients {
		client.Stop()
	}
}

// printStatsPeriodically 定期打印统计信息
func printStatsPeriodically(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printStats()
		}
	}
}

// generateTestMessage 生成指定大小的测试消息
func generateTestMessage(size int) string {
	if size <= 0 {
		return "test"
	}

	// 生成指定大小的消息
	message := make([]byte, size)
	for i := range message {
		message[i] = byte('a' + (i % 26))
	}
	return string(message)
}

// printStats 打印统计信息
func printStats() {
	totalConn, activeConn, totalMsg, successMsg, failedMsg, duration := stats.GetStats()

	log.Printf("=== 统计信息 ===")
	log.Printf("总连接数: %d", totalConn)
	log.Printf("活跃连接数: %d", activeConn)
	log.Printf("总消息数: %d", totalMsg)
	log.Printf("成功消息数: %d", successMsg)
	log.Printf("失败消息数: %d", failedMsg)
	log.Printf("运行时间: %v", duration)

	if duration > 0 {
		msgPerSec := float64(totalMsg) / duration.Seconds()
		log.Printf("消息速率: %.2f msg/s", msgPerSec)
	}

	if totalMsg > 0 {
		successRate := float64(successMsg) / float64(totalMsg) * 100
		log.Printf("成功率: %.2f%%", successRate)
	}
	log.Printf("================")
}
