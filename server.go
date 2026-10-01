// server.go - Gin 路由注册 + 中间件挂载 + engine 构建
//
// 职责：
//  - 构建 gin.Engine（禁用控制台输出，由 slog 接管日志）。
//  - 按顺序注册全局中间件：Recovery → RequestLogger → SignatureAuth → RateLimit。
//  - 注册所有路由（按版本分组 /api/v1）。
//  - 挂载 /healthz 作为无签名校验的健康检查端点。
//  - 提供 Shutdown 方法用于优雅退出。
//
// 中间件挂载顺序的 rationale：
//  1. Recovery：捕获 panic，必须最先挂载。
//  2. RequestLogger：记录请求开始/结束，供调试和审计。
//  3. SignatureAuth：校验签名，失败则 short-circuit。
//  4. RateLimit：仅对通过签名的请求计数，避免恶意请求污染限流统计。
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"log/slog"
)

// Server 封装 Gin 引擎和配置。
type Server struct {
	engine *gin.Engine
	cfg    Config
}

// NewServer 从配置构建 Server，不启动监听。
func NewServer(cfg Config) *Server {
	// 禁用 Gin 的默认控制台日志，由 slog 统一处理。
	gin.SetMode(gin.ReleaseMode)

	engine := gin.New()
	engine.Use(gin.Recovery()) // 捕获 panic，返回 500

	return &Server{engine: engine, cfg: cfg}
}

// Setup 注册所有路由和中间件。
func (s *Server) Setup(rdb *Redis, scripts LuaScripts) {
	// ---- 全局中间件 ----
	// RequestLogger 先于 SignatureAuth，以便记录未能通过签名的请求。
	s.engine.Use(RequestLogger())
	s.engine.Use(SignatureAuth(s.cfg.Nodes, rdb.Client()))
	s.engine.Use(RateLimit(rdb.Client()))

	// HTTP Basic Auth（可选，第二层认证）
	if s.cfg.HTTPAuth.Enabled {
		s.engine.Use(BasicAuth(s.cfg.HTTPAuth))
	}

	// ---- 健康检查（不校验签名） ----
	// 必须在通用签名中间件之前挂载，避免健康检查被拒。
	s.engine.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// ---- API v1 路由组 ----
	v1 := s.engine.Group("/api/v1")
	{
		v1.POST("/traffic/report", makeHandler(rdb, scripts, &s.cfg, func(h *Handler) func(*gin.Context) { return h.PostTrafficReport }))
		v1.GET("/kick/list", makeHandler(rdb, scripts, &s.cfg, func(h *Handler) func(*gin.Context) { return h.GetKickList }))
		v1.POST("/kick/ack", makeHandler(rdb, scripts, &s.cfg, func(h *Handler) func(*gin.Context) { return h.PostKickAck }))
		v1.POST("/node/heartbeat", makeHandler(rdb, scripts, &s.cfg, func(h *Handler) func(*gin.Context) { return h.PostHeartbeat }))
		v1.POST("/admin/kick", makeHandler(rdb, scripts, &s.cfg, func(h *Handler) func(*gin.Context) { return h.PostAdminKick }))
	}
}

// makeHandler 辅助函数：创建 Handler 并返回闭包。
// 确保 Handler 持有真实的 Config 引用，以便 NodeExists 能检查节点是否注册。
func makeHandler(rdb *Redis, scripts LuaScripts, cfg *Config, mk func(*Handler) func(*gin.Context)) func(*gin.Context) {
	h := &Handler{rdb: rdb, lua: scripts, cfg: cfg}
	return mk(h)
}

// Serve 启动 HTTP 服务，阻塞至 shutdown。
func (s *Server) Serve(addr string, shutdownChan <-chan struct{}) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.engine,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 优雅启动：监听地址绑定。
	go func() {
		slog.Info("HTTP 服务启动", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP 服务异常退出", "error", err)
		}
	}()

	// 等待关闭信号。
	<-shutdownChan
	slog.Info("收到关闭信号，开始优雅退出")

	// 给正在处理的请求留出时间。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}
