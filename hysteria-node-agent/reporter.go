// reporter.go — 流量采集上报循环
//
// 每个周期：
//  1. GET Hysteria /traffic（默认 clear=1，服务端返回「上次读取以来的增量」并清零）
//  2. 增量并入本地缓冲（Linux1 挂掉也不丢数据）
//  3. POST 缓冲内容到 Linux1，成功则扣减对应部分；失败则留到下一周期重试
//  4. Linux1 若在响应中返回超限用户，立即调用 Hysteria /kick
package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Reporter 周期性采集并上报流量。
type Reporter struct {
	cfg    *Config
	hy     *HysteriaClient
	l1     *Linux1Client
	store  *Store
	logger *slog.Logger

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewReporter 创建 Reporter。
func NewReporter(cfg *Config, hy *HysteriaClient, l1 *Linux1Client, store *Store, logger *slog.Logger) *Reporter {
	return &Reporter{
		cfg:    cfg,
		hy:     hy,
		l1:     l1,
		store:  store,
		logger: logger,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start 启动上报循环（非阻塞）。
func (r *Reporter) Start() {
	go func() {
		defer close(r.doneCh)

		r.logger.Info("流量上报循环启动",
			"interval", r.cfg.TrafficInterval(),
			"clear_after_read", *r.cfg.HysteriaAPI.ClearAfterRead,
			"format", r.cfg.Linux1.TrafficFormat)

		// 启动后立即跑一次，之后按周期执行。
		runGuarded(r.logger, "流量上报周期", r.runOnce)

		ticker := time.NewTicker(r.cfg.TrafficInterval())
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				r.logger.Info("流量上报循环已停止")
				return
			case <-ticker.C:
				runGuarded(r.logger, "流量上报周期", r.runOnce)
			}
		}
	}()
}

// Stop 停止循环并等待退出。
func (r *Reporter) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	select {
	case <-r.doneCh:
	case <-time.After(5 * time.Second):
		r.logger.Warn("流量上报循环退出超时")
	}
}

// RunOnce 供 -once 模式调用。
func (r *Reporter) RunOnce(ctx context.Context) error {
	return r.runOnce(ctx)
}

func (r *Reporter) runOnce(ctx context.Context) error {
	clear := *r.cfg.HysteriaAPI.ClearAfterRead

	traffic, err := r.hy.GetTraffic(ctx, clear)
	if err != nil {
		r.logger.Error("获取 Hysteria 流量失败", "error", err)
		// 采集失败仍尝试把缓冲里积压的数据推给 Linux1。
		r.flush(ctx)
		return err
	}

	deltas := make([]TrafficDelta, 0, len(traffic))
	var totalTx, totalRx uint64
	for uid, c := range traffic {
		if c.Tx == 0 && c.Rx == 0 {
			continue
		}
		deltas = append(deltas, TrafficDelta{UserID: uid, TxDelta: c.Tx, RxDelta: c.Rx})
		totalTx += c.Tx
		totalRx += c.Rx
	}

	if len(deltas) == 0 {
		r.logger.Debug("本周期无流量增量")
		r.flush(ctx)
		return nil
	}

	r.store.Merge(deltas)
	r.logger.Info("采集到流量增量",
		"users", len(deltas), "tx", totalTx, "rx", totalRx,
		"pending_users", r.store.Pending())

	r.flush(ctx)
	return nil
}

// flush 把缓冲中的流量推送给 Linux1。
func (r *Reporter) flush(ctx context.Context) {
	snapshot := r.store.Snapshot()
	if len(snapshot) == 0 {
		return
	}

	reported, exceeded, err := r.l1.ReportTraffic(ctx, time.Now().Unix(), snapshot)
	if len(reported) > 0 {
		if cErr := r.store.Commit(reported); cErr != nil {
			r.logger.Error("缓冲落盘失败", "error", cErr)
		}
	}

	var tx, rx uint64
	for _, d := range reported {
		tx += d.TxDelta
		rx += d.RxDelta
	}

	if err != nil {
		r.logger.Error("上报流量到 Linux1 失败，数据保留待重试",
			"error", err, "reported_users", len(reported), "pending_users", r.store.Pending())
	} else {
		r.logger.Info("流量上报成功",
			"users", len(reported), "tx", tx, "rx", rx, "pending_users", r.store.Pending())
	}

	// Linux1 提示配额超限的用户：立刻踢下线。
	if len(exceeded) > 0 {
		r.logger.Warn("Linux1 返回超限用户，立即踢出", "users", exceeded)
		if kErr := r.hy.KickUsers(ctx, exceeded); kErr != nil {
			r.logger.Error("踢出超限用户失败", "users", exceeded, "error", kErr)
		}
	}
}

// runGuarded 执行 fn 并把 panic 转成日志，避免单个周期异常导致进程退出。
func runGuarded(logger *slog.Logger, name string, fn func(context.Context) error) {
	defer func() {
		if rec := recover(); rec != nil {
			logger.Error(name+" panic", "panic", rec)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := fn(ctx); err != nil {
		// 具体错误已在 fn 内部记录，这里只保证不中断循环。
		_ = err
	}
}
