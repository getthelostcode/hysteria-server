// main.go - 程序入口：加载配置、初始化组件、启动 Server、优雅退出
//
// 启动流程：
//  1. 加载 config.yaml（缺失则用默认值）。
//  2. 校验配置（必要字段非空、duration 可解析）。
//  3. 初始化日志（slog，级别由配置控制）。
//  4. 连接 Redis（异步，不在启动时 PING——首次请求自然验证）。
//  5. 加载 Lua 脚本对象。
//  6. 构建 Gin Server，注册路由。
//  7. 启动掉线扫描协程。
//  8. 启动 HTTP 服务（阻塞）。
//  9. 收到 SIGINT/SIGTERM 后优雅退出。
//
// 优雅退出流程：
//  - Shutdown 信号触发后，HTTP Server 停止接受新连接。
//  - 正在处理的请求有 10 秒窗口完成。
//  - 掉线扫描协程在 ctx 取消后自动退出。
//  - Redis 连接由 go-redis 自动管理，无需手动关闭。
package main

import (
	"context"
	"os"

	"log/slog"
)

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
	// ---- 1. 加载配置 ----
	cfg, err := Load("config.yaml")
	if err != nil {
		slog.Error("配置加载失败", "error", err)
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

	slog.Info("配置加载完成", "listen", cfg.Listen, "nodes", len(cfg.Nodes))

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

	// ---- 7. 启动 HTTP 服务（阻塞） ----
	if err := server.Serve(cfg.Listen, shutdownChan); err != nil {
		slog.Error("HTTP 服务停止", "error", err)
		os.Exit(1)
	}

	slog.Info("服务器已优雅退出")
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
