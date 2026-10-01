// handler.go - HTTP Handler 实现（5 个接口）
//
// 所有 Handler 共享的约定：
//  - 请求体 JSON 绑定使用 gin 的 ShouldBindJSON。
//  - Redis 操作均使用带超时的 context（从 c.Request.Context() 衍生）。
//  - 返回格式统一：{"ok": true} 或 {"code": "...", "msg": "..."}.
//
// Handler 列表：
//  1. PostTrafficReport   POST /api/v1/traffic/report
//  2. GetKickList         GET  /api/v1/kick/list?node_id=xxx
//  3. PostKickAck        POST /api/v1/kick/ack
//  4. PostHeartbeat       POST /api/v1/node/heartbeat
//  5. PostAdminKick       POST /api/v1/admin/kick
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"log/slog"
)

// ---- 请求/响应结构 ----

type TrafficReportReq struct {
	NodeID    string        `json:"node_id" binding:"required"`
	Timestamp int64         `json:"timestamp"`
	Users     []TrafficUser `json:"users" binding:"required,min=1"`
}

type TrafficUser struct {
	UserID   string `json:"user_id" binding:"required"`
	Upload   int64  `json:"upload" binding:"min=0"`
	Download int64  `json:"download" binding:"min=0"`
	Token    string `json:"token"` // 用户认证 token（HMAC，用于用户级认证）
}

type TrafficReportResp struct {
	OK            bool     `json:"ok"`
	QuotaExceeded []string `json:"quota_exceeded"`
}

type KickListResp struct {
	Kick []KickEntry `json:"kick"`
}

type KickEntry struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason"` // quota_exceeded / admin_kick / expired
}

type KickAckReq struct {
	NodeID  string   `json:"node_id" binding:"required"`
	UserIDs []string `json:"user_ids" binding:"required,min=1"`
}

type HeartbeatReq struct {
	NodeID      string `json:"node_id" binding:"required"`
	Version     string `json:"version"`
	OnlineUsers int    `json:"online_users"`
}

type HeartbeatResp struct {
	OK      bool `json:"ok"`
	Interval int `json:"interval"` // 建议上报间隔（秒）
}

type AdminKickReq struct {
	NodeID string `json:"node_id" binding:"required"`
	UserID string `json:"user_id" binding:"required"`
	Reason string `json:"reason"` // 默认 admin_kick，可覆盖
}

// ErrorJSON 统一错误响应体。
type ErrorJSON struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

// OKResp 统一成功响应体。
type OKResp struct {
	OK bool `json:"ok"`
}

// ---- Handler 实现 ----

// PostTrafficReport 处理客户端流量上报。
//
// 设计决策：
//  - 每个用户独立调用 Lua 脚本，保证原子性。
//  - 幂等键基于 (node_id, timestamp, user_id)，SETNX + TTL 1h。
//  - 已 blocked 用户直接计入 quota_exceeded，不调用 Lua 脚本累加流量（也不会有副作用）。
//  - 多个用户批量上报时，逐个处理。若中间某个用户执行失败，不影响其他用户处理，
//    但该用户的结果不会被记录（宁可漏报也不返回错误结果）。
func (h *Handler) PostTrafficReport(c *gin.Context) {
	var req TrafficReportReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E101", Msg: "请求体错误: " + err.Error()})
		return
	}

	// 验证 node_id 已注册（签名校验已保证 secret 存在，此处为双重保险）。
	if !h.cfg.NodeExists(req.NodeID) {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E101", Msg: "未注册节点: " + req.NodeID})
		return
	}

	if req.Timestamp <= 0 {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E102", Msg: "timestamp 必须为正整数"})
		return
	}

	tsStr := strconv.FormatInt(req.Timestamp, 10)

	// quota_exceeded 初始化为空切片，确保 JSON 序列化为 [] 而非 null。
	quotaExceeded := make([]string, 0)
	authFailures := make([]string, 0)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	for _, u := range req.Users {
		if u.UserID == "" {
			continue // 跳过无效 user_id（不中断批处理）
		}

		// ---- 用户认证（如果配置了 users） ----
		// 每个用户的 token 是 HMAC-SHA256(user_secret, node_id + "." + timestamp + "." + user_id) 的 hex 编码。
		// 校验通过后才处理流量，防止节点泄露用户 secret 后伪造上报。
		if len(h.cfg.Users) > 0 {
			if u.Token == "" {
				authFailures = append(authFailures, u.UserID+"缺少token")
				continue
			}
			expectedToken := h.makeUserToken(req.NodeID, u.UserID, req.Timestamp)
			if !hmac.Equal([]byte(u.Token), []byte(expectedToken)) {
				authFailures = append(authFailures, u.UserID+"token无效")
				continue
			}
		}

		// ---- 幂等校验 ----
		// SETNX 返回 1 表示首次插入（未见过此幂等键），0 表示已存在。
		// TTL 1h 保证键最终自清理，防止无限膨胀。
		idemKey := keyIdem(req.NodeID, tsStr, u.UserID)
		added, err := h.rdb.Client().SetNX(ctx, idemKey, "1", 1*time.Hour).Result()
		if err != nil {
			slog.Warn("幂等键设置失败", "user_id", u.UserID, "error", err)
			continue // 降级：跳过此用户
		}
		if !added {
			// 已处理过此 (node, ts, uid) 组合——幂等命中。
			// 不再累加，也不重复检查 quota。
			continue
		}

		// ---- 查询 quota.blocked 状态（不通过 Lua，避免脚本内读取另一 key 的额外复杂度） ----
		// NOTE: 这里存在微小的竞争窗口：检查 blocked 后到 Lua 执行前可能被其他请求修改。
		// 但 Lua 脚本内部也会重新检查 blocked，因此最终结果是正确的。
		// 前置检查的意义在于：已 blocked 用户直接标记，不消耗 Lua 脚本执行。
		blocked, err := h.rdb.Client().HGet(ctx, keyQuota(u.UserID), "blocked").Result()
		if err == redis.Nil {
			blocked = "" // 从未设置过 quota，视为未 blocked
		} else if err != nil {
			slog.Warn("读取 quota.blocked 失败", "user_id", u.UserID, "error", err)
			continue
		}

		if blocked == "1" {
			// 用户已 blocked：直接计入待踢列表，不做流量累加。
			quotaExceeded = append(quotaExceeded, u.UserID)
			continue
		}

		// ---- 调用 Lua 脚本累加流量 + 判断配额 ----
		// 返回 1 = 正常累加，2 = 超限或 blocked（脚本内标记）。
		result, err := h.lua.RunTrafficAccumulate(ctx, h.rdb.Client(),
			u.UserID, strconv.FormatInt(u.Upload, 10), strconv.FormatInt(u.Download, 10))
		if err != nil {
			slog.Error("流量累加脚本执行失败", "user_id", u.UserID, "error", err)
			continue
		}

		if result == 2 {
			// 超限或 blocked：加入 kick:pending 集合 + 确保 blocked 已设置。
			// （Lua 脚本已设置 quota.blocked=1，脚本外再补 SADD 到 kick 集合。）
			err := h.rdb.Client().SAdd(ctx, keyKickPending(req.NodeID), u.UserID).Err()
			if err != nil {
				slog.Error("加入 kick 队列失败", "user_id", u.UserID, "error", err)
			}
			quotaExceeded = append(quotaExceeded, u.UserID)
		}
	}

	c.JSON(http.StatusOK, TrafficReportResp{OK: true, QuotaExceeded: quotaExceeded})
}

// GetKickList 返回指定节点的待踢用户列表及其 reason。
//
// 设计决策：
//  - reason 存储在 quota:{uid} 的 reason 字段中。
//  - Lua 脚本踢人时已设置 reason，admin_kick 设置 reason，掉线扫描设置 reason=expired。
//  - 遍历 kick:pending 集合中的每个用户，从 quota hash 读取 reason。
//  - 集合可能为空，返回 {"kick": []}。
func (h *Handler) GetKickList(c *gin.Context) {
	nodeID := c.Query("node_id")
	if nodeID == "" {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E201", Msg: "node_id 参数缺失"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	members, err := h.rdb.Client().SMembers(ctx, keyKickPending(nodeID)).Result()
	if err != nil {
		slog.Error("读取 kick:pending 失败", "node", nodeID, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorJSON{Code: "E202", Msg: "服务器内部错误"})
		return
	}

	kickList := make([]KickEntry, 0, len(members))
	for _, uid := range members {
		reason, err := h.rdb.Client().HGet(ctx, keyQuota(uid), "reason").Result()
		if err == redis.Nil {
			reason = "unknown" // 防御性默认值
		} else if err != nil {
			slog.Warn("读取用户 reason 失败", "uid", uid, "error", err)
			reason = "unknown"
		}
		kickList = append(kickList, KickEntry{UserID: uid, Reason: reason})
	}

	c.JSON(http.StatusOK, KickListResp{Kick: kickList})
}

// PostKickAck 处理客户端踢人确认。
//
// 语义：
//  - 从 kick:pending:{node_id} 中移除指定用户。
//  - 设置 quota:{uid}.blocked = 1。
//  - Lua 脚本保证两个操作原子（移除 + 设置 blocked）。
//  - 若用户不在 pending 队列中，SREM 是幂等的（无副作用），仍设置 blocked。
func (h *Handler) PostKickAck(c *gin.Context) {
	var req KickAckReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E301", Msg: "请求体错误: " + err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	for _, uid := range req.UserIDs {
		if uid == "" {
			continue
		}
		if err := h.lua.RunKickAck(ctx, h.rdb.Client(), req.NodeID, uid); err != nil {
			slog.Error("踢人确认失败", "node", req.NodeID, "uid", uid, "error", err)
			c.JSON(http.StatusInternalServerError, ErrorJSON{Code: "E302", Msg: "服务器内部错误"})
			return
		}
	}

	c.JSON(http.StatusOK, OKResp{OK: true})
}

// PostHeartbeat 处理节点心跳上报。
//
// 设计决策：
//  - 第一次心跳自动将节点加入 node:set。
//  - 每次心跳刷新 node:hb:{node_id} TTL。
//  - 返回 interval = heartbeat.ttl / 2，建议客户端每半个 TTL 发送一次心跳，
//    保证过期前至少有两次心跳（避免单次延迟导致的误判）。
//  - online_users 是纯信息字段，不入 Redis（未来可存储用于监控）。
func (h *Handler) PostHeartbeat(c *gin.Context) {
	var req HeartbeatReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E401", Msg: "请求体错误: " + err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := h.rdb.Heartbeat(ctx, req.NodeID); err != nil {
		slog.Error("心跳写入失败", "node", req.NodeID, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorJSON{Code: "E402", Msg: "服务器内部错误"})
		return
	}

	// 建议间隔 = TTL 的一半（秒）。
	ttl, _ := time.ParseDuration(h.rdb.cfg.TTL) // cfg 已在启动时校验
	interval := int(ttl.Seconds() / 2)

	c.JSON(http.StatusOK, HeartbeatResp{OK: true, Interval: interval})
}

// PostAdminKick 管理员手动踢用户。
//
// 语义：
//  - 设置 quota.{uid}.blocked = 1。
//  - 设置 quota.{uid}.reason = "admin_kick"（或指定 reason）。
//  - 将用户加入 kick:pending:{node_id}。
//  - 不检查流量配额，也不关心用户是否已在队列中（SADD 幂等）。
//  - 需管理员权限校验（此版本仅校验 node_id 是否已注册，真实环境应基于权限系统）。
func (h *Handler) PostAdminKick(c *gin.Context) {
	var req AdminKickReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E501", Msg: "请求体错误: " + err.Error()})
		return
	}

	if !h.cfg.NodeExists(req.NodeID) {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E501", Msg: "未注册节点: " + req.NodeID})
		return
	}

	if req.UserID == "" {
		c.JSON(http.StatusBadRequest, ErrorJSON{Code: "E501", Msg: "user_id 不能为空"})
		return
	}

	reason := req.Reason
	if reason == "" {
		reason = "admin_kick"
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	// 先设置 quota 标记。
	err := h.rdb.Client().HSet(ctx, keyQuota(req.UserID), "blocked", "1", "reason", reason).Err()
	if err != nil {
		slog.Error("管理员踢人 - 设置 quota 失败", "uid", req.UserID, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorJSON{Code: "E502", Msg: "服务器内部错误"})
		return
	}

	// 再加入 kick 队列。
	err = h.rdb.Client().SAdd(ctx, keyKickPending(req.NodeID), req.UserID).Err()
	if err != nil {
		slog.Error("管理员踢人 - 加入 kick 队列失败", "uid", req.UserID, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorJSON{Code: "E502", Msg: "服务器内部错误"})
		return
	}

	c.JSON(http.StatusOK, OKResp{OK: true})
}

// makeUserToken 返回用户 token：hex(HMAC-SHA256(user_secret, node_id + "." + timestamp + "." + user_id))
func (h *Handler) makeUserToken(nodeID string, userID string, timestamp int64) string {
	secret, ok := h.cfg.Users[userID]
	if !ok {
		return ""
	}
	message := nodeID + "." + strconv.FormatInt(timestamp, 10) + "." + userID
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}
