// main.go — 程序入口
//
// 部署位置：Linux2（Hysteria 2 服务端所在机器）。
// 本程序不启动 Hysteria 服务端，只做两件事：
//  1. 定时拉取 Linux1 的踢人列表 → 调用 Hysteria 本地 API 踢人
//  2. 定时从 Hysteria 本地 API 采集各用户流量 → 计算增量 → 上报给 Linux1
//
// 用法：
//
//	hysteria-node-agent -config /etc/hysteria-node-agent/config.yaml
//	hysteria-node-agent -config ./config.yaml -once     # 各跑一轮后退出（调试用）
//	hysteria-node-agent -version
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const version = "1.0.0"

func main() {
	configPath := flag.String("config", "/etc/hysteria-node-agent/config.yaml", "配置文件路径")
	once := flag.Bool("once", false, "各执行一轮（踢人 + 流量上报）后退出，用于调试")
	printVersion := flag.Bool("version", false, "打印版本并退出")
	flag.Parse()

	if *printVersion {
		fmt.Printf("hysteria-node-agent %s\n", version)
		return
	}

	cfg, err := Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置加载失败: %v\n", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel)
	logger.Info("hysteria-node-agent 启动",
		"version", version,
		"config", *configPath,
		"linux1", cfg.Linux1APIBase,
		"hysteria_api", cfg.HysteriaAPI.BaseURL,
		"traffic_interval", cfg.TrafficInterval(),
		"kick_interval", cfg.KickInterval(),
	)

	store, err := NewStore(cfg.Store.Path, logger)
	if err != nil {
		logger.Error("本地缓冲初始化失败", "error", err)
		os.Exit(1)
	}

	hy := NewHysteriaClient(cfg.HysteriaAPI.BaseURL, cfg.HysteriaAPI.Secret, cfg.HysteriaAPI.Timeout(), logger)
	l1 := NewLinux1Client(cfg.Linux1APIBase, cfg.Linux1, logger)

	reporter := NewReporter(cfg, hy, l1, store, logger)
	kicker := NewKicker(cfg, hy, l1, logger)

	if *once {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		logger.Info("once 模式：执行一轮踢人检查")
		if err := kicker.RunOnce(ctx); err != nil {
			logger.Error("踢人检查失败", "error", err)
		}
		logger.Info("once 模式：执行一轮流量上报")
		if err := reporter.RunOnce(ctx); err != nil {
			logger.Error("流量上报失败", "error", err)
		}
		logger.Info("once 模式完成")
		return
	}

	reporter.Start()
	kicker.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	logger.Info("收到退出信号，开始优雅退出", "signal", sig.String())

	reporter.Stop()
	kicker.Stop()
	logger.Info("hysteria-node-agent 已退出")
}

// newLogger 按配置的日志级别创建 JSON 日志输出。
func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
	slog.SetDefault(logger)
	return logger
}
