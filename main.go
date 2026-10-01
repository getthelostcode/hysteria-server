// main.go - 程序入口：加载配置、初始化组件、启动 Server、优雅退出
//
// 启动流程：
//  1. 解析命令行参数（-config / -help / -h / -version，及 help / version 子命令）。
//  2. 加载 config.yaml（缺失则用默认值）。
//  3. 校验配置（必要字段非空、duration 可解析）。
//  4. 初始化日志（slog，级别由配置控制）。
//  5. 连接 Redis（异步，不在启动时 PING——首次请求自然验证）。
//  6. 加载 Lua 脚本对象。
//  7. 构建 Gin Server，注册路由。
//  8. 启动掉线扫描协程。
//  9. 启动 HTTP 服务（阻塞）。
// 10. 收到 SIGINT/SIGTERM 后优雅退出。
//
// 优雅退出流程：
//  - Shutdown 信号触发后，HTTP Server 停止接受新连接。
//  - 正在处理的请求有 10 秒窗口完成。
//  - 掉线扫描协程在 ctx 取消后自动退出。
//  - Redis 连接由 go-redis 自动管理，无需手动关闭。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"log/slog"
)

const version = "1.0.0"

// Handler 是各接口的共享宿主，持有 Redis 客户端引用和配置引用。
type Handler struct {
	rdb *Redis
	lua LuaScripts
	cfg *Config // 用于节点存在性检查
}

// NodeExists 检查 node_id 是否在配置的 nodes 列表中。
func (h *Handler) NodeExists(nodeID string) bool {
	if h.cfg == nil {
		return false
	}
	_, ok := h.cfg.Nodes[nodeID]
	return ok
}

func main() {
	// ---- 解析命令行 flag ----
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	help := flag.Bool("help", false, "打印帮助信息")
	helpShort := flag.Bool("h", false, "打印帮助信息（-help 的简写）")
	versionFlag := flag.Bool("version", false, "打印版本信息")
	// flag 解析出错时打印自定义帮助，而非默认的 flag 列表。
	flag.Usage = printHelp
	flag.Parse()

	// 支持子命令写法：hysteria-server help / hysteria-server version
	args := flag.Args()
	subCmd := ""
	if len(args) > 0 {
		subCmd = args[0]
	}

	if *help || *helpShort || subCmd == "help" {
		printHelp()
		os.Exit(0)
	}
	if *versionFlag || subCmd == "version" {
		fmt.Println(version)
		os.Exit(0)
	}
	if subCmd != "" {
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", subCmd)
		printHelp()
		os.Exit(2)
	}

	// ---- 1. 加载配置 ----
	cfg, err := Load(*configPath)
	if err != nil {
		slog.Error("配置加载失败", "path", *configPath, "error", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("配置校验失败", "error", err)
		os.Exit(1)
	}

	// ---- 2. 初始化日志 ----
	// 根据配置级别设置最低日志级别。
	// NOTE: slog.SetDefault 影响全局 logger，所有包使用 slog.Default() 即可。
	setupLogger(cfg.Log.Level)

	slog.Info("配置加载完成", "listen", cfg.Listen, "nodes", len(cfg.Nodes), "config", *configPath)

	// ---- 3. 初始化 Redis ----
	rdb := NewRedis(cfg.Redis, cfg.Heartbeat)

	// ---- 4. 加载 Lua 脚本 ----
	scripts := NewLuaScripts()

	// ---- 5. 构建 Server ----
	server := NewServer(cfg)
	server.Setup(rdb, scripts)

	// ---- 6. 启动掉线扫描协程 ----
	// 创建通知用 channel，传递给 Server。
	shutdownChan := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go rdb.OfflineScanLoop(ctx)
	slog.Info("掉线扫描协程已启动", "interval", cfg.Heartbeat.ScanInterval)

	// ---- 信号监听：Ctrl+C / SIGTERM 触发优雅退出 ----
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-quit
		slog.Info("收到退出信号，开始优雅关闭", "signal", sig.String())
		close(shutdownChan)
		// 等待关闭完成后取消 ctx，停止掉线扫描协程
		<-shutdownChan
		cancel()
	}()

	// ---- 7. 启动 HTTP 服务（阻塞） ----
	if err := server.Serve(cfg.Listen, shutdownChan); err != nil {
		slog.Error("HTTP 服务停止", "error", err)
		os.Exit(1)
	}

	slog.Info("服务器已优雅退出")
}

func printHelp() {
	fmt.Println(`hysteria-server - Hysteria 代理管理系统 Server

用法:
  hysteria-server [选项]
  hysteria-server <命令>

选项:
  -config string
        配置文件路径 (默认: "config.yaml")
  -help, -h
        打印帮助信息
  -version
        打印版本信息

命令:
  help            打印帮助信息（等价于 -help）
  version         打印版本信息（等价于 -version）

信号:
  SIGINT (Ctrl+C) / SIGTERM - 优雅关闭服务器

HTTP 接口:
  POST /api/v1/traffic/report    - 流量上报
  GET  /api/v1/kick/list         - 待踢列表
  POST /api/v1/kick/ack          - 踢人确认
  POST /api/v1/node/heartbeat    - 节点心跳
  POST /api/v1/admin/kick        - 管理员踢人
  GET  /healthz                  - 健康检查（无认证）

认证:
  - HMAC-SHA256 签名（除 /healthz）：X-Signature / X-Timestamp / X-Nonce
  - HTTP Basic Auth（可选）：Authorization: Basic base64(user:pass)
  - 用户 token（可选，流量上报）：每个用户条目携带 token 字段

配置:
  详见 config.yaml 示例或 --config 指定的文件。
`)
}

func setupLogger(levelStr string) {
	var level slog.Level
	switch levelStr {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
}
