// redis.go - Redis 客户端封装 + 心跳上报 + 掉线扫描协程
//
// 职责：
//  1. 构建 redis.Client（连接池、超时、读写超时配置）。
//  2. 封装心跳写入：SET node:hb:{node_id} + SADD node:set。
//  3. 启动掉线扫描 goroutine：周期读取 node:set，对每个节点检查 TTL，
//     过期则从 node:set 移除、并将 kick:pending:{node} 中的用户 reason 改为 expired。
//
// 设计决策：
//  - 掉线检测基于 TTL 过期，不使用独立的在线/离线状态位。
//    优点：Server 重启后无需重建状态，客户端重新心跳即可恢复;
//    缺点：重启瞬间所有节点被误判离线（若重启期间未收到心跳）。
//    这是可接受的折衷，因为心跳间隔通常几秒，误判窗口极短。
//  - 扫描线程不阻塞主请求路径。掉线清理是 best-effort，失败则记录日志继续。
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 是对 go-redis.Client 的一次薄封装，持有配置信息。
type Redis struct {
	client *redis.Client
	cfg    HeartbeatConfig
}

// NewRedis 创建 Redis 客户端。没有立即 PING，连接错误将在首次请求时暴露。
// redisCfg 用于构建连接，hbCfg 用于心跳 TTL / 扫描间隔。
func NewRedis(redisCfg RedisConfig, hbCfg HeartbeatConfig) *Redis {
	client := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Addr,
		Password: redisCfg.Password,
		DB:       redisCfg.DB,
		PoolSize: redisCfg.PoolSize,
		// 单次命令超时：防止卡住的 Redis 拖垮整个 Server。
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	return &Redis{client: client, cfg: hbCfg}
}

// Client 暴露底层 client（供 lua.go 等使用）。仅限包内使用。
func (r *Redis) Client() *redis.Client {
	return r.client
}

// Heartbeat 写入节点心跳信号：设置 node:hb:{node_id}，同时确保节点在 node:set 中。
// TTL 由配置决定，过期即节点离线。
// 调用方必须传入带超时的 context。
func (r *Redis) Heartbeat(ctx context.Context, nodeID string) error {
	pipe := r.client.Pipeline()
	// node:hb:{node_id} 设置为当前时间戳 + TTL。
	pipe.Set(ctx, keyNodeHB(nodeID), time.Now().Unix(), r.parseDuration(r.cfg.TTL))
	// 保证节点在 node:set 中（幂等）。
	pipe.SAdd(ctx, "node:set", nodeID)
	_, err := pipe.Exec(ctx)
	return err
}

// parseDuration 将配置中的字符串（如 "60s"）解析为 time.Duration。
func (r *Redis) parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		// 配置已在启动时校验，此处仅防御性处理。
		slog.Warn("心跳 TTL 解析失败，降级为 60s", "error", err)
		return 60 * time.Second
	}
	return d
}

// ScanInterval 返回掉线扫描间隔。
func (r *Redis) ScanInterval() time.Duration {
	d, err := time.ParseDuration(r.cfg.ScanInterval)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

// OfflineScanLoop 启动掉线节点扫描协程。
// 逻辑：
//  1. SMEMBERS node:set 获取所有已知节点。
//  2. 对每个节点 PTTL node:hb:{node_id}，若 ≤0 则节点离线。
//  3. 离线节点从 node:set 移除。
//  4. 遍历 kick:pending:{node_id} 中的用户，将 quota:{uid}.reason 改为 "expired"。
//     （保持队列用于客户端后续查询，避免信息丢失。）。
// 该协程在 Server 整个生命周期运行，ctx 取消时退出。
func (r *Redis) OfflineScanLoop(ctx context.Context) {
	ticker := time.NewTicker(r.ScanInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.scanOffline(ctx)
		}
	}
}

func (r *Redis) scanOffline(ctx context.Context) {
	// 获取节点列表（快照）。期间有新节点加入不会被本次扫描捕捉，
	// 但不影响正确性——新节点会在下次心跳写入时被重新加入 node:set。
	nodes, err := r.client.SMembers(ctx, "node:set").Result()
	if err != nil {
		slog.Error("扫描节点列表失败", "error", err)
		return
	}

	for _, nodeID := range nodes {
		// PTTL 返回毫秒剩余 TTL，≤0 表示已过期或不存在。
		ttl, err := r.client.PTTL(ctx, keyNodeHB(nodeID)).Result()
		if err != nil {
			slog.Warn("检查节点心跳 TTL 失败", "node", nodeID, "error", err)
			continue
		}
		if ttl > 0 {
			continue // 节点在线，跳过
		}

		// ---- 节点离线，执行清理 ----
		slog.Info("检测到节点掉线", "node", nodeID, "ttl", ttl)

		// 从 node:set 移除（删除已过期的心跳 key 由 Redis 自动完成）。
		r.client.SRem(ctx, "node:set", nodeID)

		// 遍历 kick:pending:{node} 中的用户，标记 reason=expired。
		members, err := r.client.SMembers(ctx, keyKickPending(nodeID)).Result()
		if err != nil {
			slog.Warn("读取待踢队列失败", "node", nodeID, "error", err)
			continue
		}
		for _, uid := range members {
			// 不检查是否写入成功——best-effort。即使失败，用户仍在队列中，
			// 下次查询时仍可见；当用户被 ack 后正常清理。
			r.client.HSet(ctx, keyQuota(uid), "reason", "expired")
		}
	}
}

// Key builders（统一管理 key 命名规则，避免散落的字符串字面量）。
func keyTraffic(uid string) string          { return "traffic:" + uid }
func keyQuota(uid string) string            { return "quota:" + uid }
func keyKickPending(nodeID string) string   { return "kick:pending:" + nodeID }
func keyIdem(nodeID, ts, uid string) string { return "idem:traffic:" + nodeID + ":" + ts + ":" + uid }
func keyNodeHB(nodeID string) string        { return "node:hb:" + nodeID }
func keyNonce(nodeID, nonce string) string  { return "nonce:" + nodeID + ":" + nonce }
