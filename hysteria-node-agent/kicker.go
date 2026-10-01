// kicker.go — 踢人轮询循环
//
// 每个周期：
//  1. GET Linux1 的踢人列表
//  2. 过滤掉冷却期内的用户（Hysteria 客户端会自动重连，避免每 10 秒踢同一批人）
//  3. POST Hysteria /kick，body 为 ["用户ID", ...]
//  4. 若 Linux1 支持 /kick/ack，上报确认（Linux1 据此把用户移出待踢队列并标记禁用）
package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Kicker 周期性拉取踢人列表并执行踢人。
type Kicker struct {
	cfg    *Config
	hy     *HysteriaClient
	l1     *Linux1Client
	logger *slog.Logger

	mu       sync.Mutex
	lastKick map[string]time.Time // 用户ID → 上次踢出时间（冷却控制）

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewKicker 创建 Kicker。
func NewKicker(cfg *Config, hy *HysteriaClient, l1 *Linux1Client, logger *slog.Logger) *Kicker {
	return &Kicker{
		cfg:      cfg,
		hy:       hy,
		l1:       l1,
		logger:   logger,
		lastKick: make(map[string]time.Time),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start 启动轮询循环（非阻塞）。
func (k *Kicker) Start() {
	go func() {
		defer close(k.doneCh)

		k.logger.Info("踢人轮询循环启动",
			"interval", k.cfg.KickInterval(),
			"cooldown", k.cfg.KickCooldown(),
			"ack_enabled", k.cfg.Linux1.KickAckEnabled())

		runGuarded(k.logger, "踢人轮询周期", k.runOnce)

		ticker := time.NewTicker(k.cfg.KickInterval())
		defer ticker.Stop()
		for {
			select {
			case <-k.stopCh:
				k.logger.Info("踢人轮询循环已停止")
				return
			case <-ticker.C:
				runGuarded(k.logger, "踢人轮询周期", k.runOnce)
			}
		}
	}()
}

// Stop 停止循环并等待退出。
func (k *Kicker) Stop() {
	k.stopOnce.Do(func() { close(k.stopCh) })
	select {
	case <-k.doneCh:
	case <-time.After(5 * time.Second):
		k.logger.Warn("踢人轮询循环退出超时")
	}
}

// RunOnce 供 -once 模式调用。
func (k *Kicker) RunOnce(ctx context.Context) error {
	return k.runOnce(ctx)
}

func (k *Kicker) runOnce(ctx context.Context) error {
	entries, err := k.l1.FetchKickList(ctx)
	if err != nil {
		k.logger.Error("获取踢人列表失败", "error", err)
		return err
	}
	if len(entries) == 0 {
		k.logger.Debug("踢人列表为空")
		return nil
	}

	// 冷却过滤：Hysteria 会重连，同一用户不在冷却期内重复踢。
	now := time.Now()
	cooldown := k.cfg.KickCooldown()

	k.mu.Lock()
	ids := make([]string, 0, len(entries))
	reasons := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.UserID == "" {
			continue
		}
		if last, ok := k.lastKick[e.UserID]; ok && now.Sub(last) < cooldown {
			continue
		}
		ids = append(ids, e.UserID)
		reasons[e.UserID] = e.Reason
	}
	k.mu.Unlock()

	if len(ids) == 0 {
		k.logger.Info("踢人列表中的用户均在冷却期内，本周期跳过",
			"listed", len(entries), "cooldown", cooldown)
		return nil
	}

	k.logger.Info("开始踢出用户", "count", len(ids), "users", ids, "reasons", formatReasons(reasons))

	if err := k.hy.KickUsers(ctx, ids); err != nil {
		k.logger.Error("调用 Hysteria /kick 失败", "users", ids, "error", err)
		return err
	}

	// 踢出成功才记冷却，失败的用户下个周期立即重试。
	k.mu.Lock()
	for _, id := range ids {
		k.lastKick[id] = now
	}
	k.gcLocked(now, cooldown)
	k.mu.Unlock()

	k.logger.Info("踢出成功", "count", len(ids), "users", ids)

	if k.cfg.Linux1.KickAckEnabled() {
		if err := k.l1.AckKicks(ctx, ids); err != nil {
			k.logger.Warn("踢人确认上报失败（下周期会重复踢，属预期内的幂等操作）",
				"users", ids, "error", err)
		} else {
			k.logger.Info("踢人确认已上报", "users", ids)
		}
	}
	return nil
}

// gcLocked 清理超过冷却期 2 倍的记录，避免 map 无限增长。调用方需持有锁。
func (k *Kicker) gcLocked(now time.Time, cooldown time.Duration) {
	keepAfter := now.Add(-2 * cooldown)
	for uid, t := range k.lastKick {
		if t.Before(keepAfter) {
			delete(k.lastKick, uid)
		}
	}
}

func formatReasons(reasons map[string]string) string {
	parts := make([]string, 0, len(reasons))
	for uid, reason := range reasons {
		if reason == "" {
			reason = "unknown"
		}
		parts = append(parts, uid+"="+reason)
	}
	return strings.Join(parts, ", ")
}
