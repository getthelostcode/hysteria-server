// middleware.go - HMAC 签名校验 / 请求日志 / 单节点限流
//
// 三个中间件按以下顺序挂载（server.go）：
//   Recovery → RequestLogger → SignatureAuth → RateLimit → Handler
//
// 顺序 rationale：
//  1. Recovery 必须最先，捕获所有 panic 并记录。
//  2. RequestLogger 记录原始请求信息（不包括 body 和 secret）。
//  3. SignatureAuth 校验签名，失败则短路返回 401。
//  4. RateLimit 最后——仅对通过签名校验的请求计数，避免恶意请求污染限流计数。
//
// 签名校验细节：
//  - 签名字符串拼接：METHOD + "\n" + PATH(不含 Query) + "\n"
//    + hex(SHA256(body)) + "\n" + timestamp_str + "\n" + nonce
//  - 时钟窗口 ±5 分钟，由服务端时钟核验。
//  - nonce 用 SETNX + TTL 10min 去重，防重放攻击。
//  - Body 读取后用 r.Body = bytes.NewReader(buf) 重新放回，
//    否则后续的 gin.ShouldBindJSON 无法读取。
//  - GET/HEAD 等无 body 方法不读取 body（body 为 nil 时 Read 返回 EOF，
//    sha256 为空串，这是安全的）。
//
// 限流细节：
//  - 单节点限流：每个 node_id 独立计数器，冷启动与节点上报频率匹配。
//  - 实现：INCR + EXPIRE(1s) 的滑动窗口简化版。适合防上报风暴，
//    不保证严格的精度（竞争窗口内允许少量超限），但足以防止单节点狂刷。
//  - 限流阈值硬编码为 100 req/s/node（可从配置扩展）。
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"log/slog"
)

const (
	// 签名时钟窗口：±5 分钟
	SignatureWindow = 5 * time.Minute

	// nonce 在 Redis 中的 TTL
	NonceTTL = 10 * time.Minute

	// 单节点限流：每秒最大请求数
	RateLimitRPS = 100
)

// SignatureAuth 返回 Gin 中间件：校验 X-Signature / X-Timestamp / X-Nonce。
// secretMap 是 node_id -> HMAC secret 的映射，来自配置。
// redisClient 用于 nonce 去重。
func SignatureAuth(secretMap map[string]string, rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取 node_id：优先 Header，其次 query 参数。
		nodeID := c.GetHeader("X-Node-ID")
		if nodeID == "" {
			nodeID = c.Query("node_id")
		}

		secret, ok := secretMap[nodeID]
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E004",
				Msg:  fmt.Sprintf("未知节点: %s", nodeID),
			})
			return
		}

		sigHeader := c.GetHeader("X-Signature")
		tsHeader := c.GetHeader("X-Timestamp")
		nonceHeader := c.GetHeader("X-Nonce")
		if sigHeader == "" || tsHeader == "" || nonceHeader == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, ErrorJSON{
				Code: "E001", Msg: "缺少签名 Header (X-Signature / X-Timestamp / X-Nonce)",
			})
			return
		}

		// ---- 1. 校验时间戳窗口 ----
		ts, err := strconv.ParseInt(tsHeader, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, ErrorJSON{
				Code: "E002", Msg: "X-Timestamp 格式无效",
			})
			return
		}
		now := time.Now().Unix()
		if math.Abs(float64(now-ts)) > SignatureWindow.Seconds() {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E002", Msg: "请求时间超出 ±5 分钟窗口",
			})
			return
		}

		// ---- 2. 读取 body 并计算 SHA256 ----
		// 复制 body 以备后用，同时不影响后续 handler 读取。
		var bodyHashHex string
		if c.Request.Body != nil && c.Request.ContentLength > 0 {
			buf, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, ErrorJSON{
					Code: "E005", Msg: "读取请求 body 失败",
				})
				return
			}
			bodyHash := sha256.Sum256(buf)
			bodyHashHex = hex.EncodeToString(bodyHash[:])
			// 重新放回 body（供 handler 使用）。
			// http.MaxBytesReader 第一个参数是 ResponseWriter，第二个是 io.ReadCloser。
			// bytes.Reader 无 Close 方法，需用 io.NopCloser 包装。
			c.Request.Body = http.MaxBytesReader(c.Writer, io.NopCloser(bytes.NewReader(buf)), 10<<20) // 10MB 上限
		} else {
			// 无 body 的请求（GET 等）：SHA256 为空串。
			bodyHashHex = ""
		}

		// ---- 3. 拼接签名字符串并校验 HMAC ----
		signStr := c.Request.Method + "\n" +
			c.FullPath() + "\n" +
			bodyHashHex + "\n" +
			tsHeader + "\n" +
			nonceHeader

		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(signStr))
		expectedSig := hex.EncodeToString(mac.Sum(nil))

		if !hmac.Equal([]byte(strings.TrimSpace(sigHeader)), []byte(expectedSig)) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E002", Msg: "签名校验失败",
			})
			return
		}

		// ---- 4. Nonce 去重（防重放） ----
		// 使用 SETNX + TTL，原子地检查并设置 nonce。
		// Redis 故障时遗憾地允许重放——这是防御性权衡，
		// 毕竟签名本身已足够防篡改，nonce 是第二层防线。
		nonceKey := keyNonce(nodeID, nonceHeader)
		added, err := rdb.SetNX(c.Request.Context(), nonceKey, "1", NonceTTL).Result()
		if err != nil {
			slog.Warn("nonce 去重 Redis 错误，允许请求通过", "node", nodeID, "error", err)
			// 降级：允许通过（签名已校验，风险可控）
		} else if !added {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E003", Msg: "Nonce 重放",
			})
			return
		}

		c.Next()
	}
}

// RequestLogger 返回 Gin 中间件：记录请求摘要（node_id、path、状态码、耗时）。
// 不记录 body 和 secret，避免泄密。
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}
		nodeID := c.GetHeader("X-Node-ID")
		if nodeID == "" {
			nodeID = c.Query("node_id")
		}

		// 处理完成后记录日志。
		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()
		slog.Info("HTTP 请求",
			"node_id", nodeID,
			"method", c.Request.Method,
			"path", path,
			"status", status,
			"duration_ms", duration.Milliseconds(),
			"remote", c.ClientIP(),
		)
	}
}

// RateLimit 返回 Gin 中间件：按 node_id 限流。
// 实现：INCR + EXPIRE 滑动窗口（每秒重置）。
// Redis 故障时跳过限流（不阻断业务）。
func RateLimit(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodeID := c.GetHeader("X-Node-ID")
		if nodeID == "" {
			nodeID = c.Query("node_id")
		}
		if nodeID == "" {
			// 无 node_id 时跳过限流（由 signature middleware 处理）
			c.Next()
			return
		}

		ctx := c.Request.Context()
		key := "ratelimit:" + nodeID

		// 原子递增 + 设置 TTL（若是新 key）。
		// 使用 Pipeline 减少往返。
		cmds, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Incr(ctx, key)
			pipe.Expire(ctx, key, 1*time.Second)
			return nil
		})
		if err != nil {
			slog.Warn("限流计数器失败，跳过限流", "node", nodeID, "error", err)
			c.Next()
			return
		}

		count, err := cmds[0].(*redis.IntCmd).Result()
		if err != nil {
			slog.Warn("限流计数读取失败，跳过限流", "node", nodeID, "error", err)
			c.Next()
			return
		}

		if count > RateLimitRPS {
			slog.Warn("节点限流触发", "node", nodeID, "count", count)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, ErrorJSON{
				Code: "E006",
				Msg:  fmt.Sprintf("单节点限流 (%d req/s)", RateLimitRPS),
			})
			return
		}

		c.Next()
	}
}

// BasicAuth 返回 Gin 中间件：HTTP Basic Authentication。
// 启用后，除 /healthz 外所有接口需提供正确的用户名/密码。
// 适合作为第二层认证（与 HMAC 签名校验叠加使用）。
func BasicAuth(config HTTPAuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 健康检查接口不受 Basic Auth 限制
		if c.Request.URL.Path == "/healthz" {
			c.Next()
			return
		}

		// 未启用时直接通过
		if !config.Enabled || len(config.Users) == 0 {
			c.Next()
			return
		}

		// 检查 Authorization 头
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E007",
				Msg:  "缺少 Basic 认证",
			})
			return
		}

		// 解析 Basic auth: "Basic base64(username:password)"
		if len(authHeader) < 6 || authHeader[:6] != "Basic " {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E007",
				Msg:  "认证格式无效",
			})
			return
		}

		payload, err := base64.StdEncoding.DecodeString(authHeader[6:])
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E007",
				Msg:  "认证解码失败",
			})
			return
		}

		// 格式: "username:password"
		credentials := strings.SplitN(string(payload), ":", 2)
		if len(credentials) != 2 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E007",
				Msg:  "认证格式无效",
			})
			return
		}

		// 从用户 map 中查找密码并验证
		expectedPassword, ok := config.Users[credentials[0]]
		if !ok || expectedPassword != credentials[1] {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorJSON{
				Code: "E007",
				Msg:  "认证失败",
			})
			return
		}

		c.Next()
	}
}
