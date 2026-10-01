// store.go — 未上报流量的本地缓冲
//
// Linux1 不可用时，流量增量不能丢：先进内存缓冲，落盘为 JSON 文件，
// 上报成功后按「已上报的量」精确扣减（不是清空），因此新采集的数据不受影响。
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type storeFile struct {
	Pending []TrafficDelta `json:"pending"`
}

// Store 保存尚未成功上报的流量增量。
type Store struct {
	path    string
	mu      sync.Mutex
	pending map[string]*TrafficDelta
	logger  *slog.Logger
}

// NewStore 打开（或创建）本地缓冲文件。path 为空时只在内存中缓冲。
func NewStore(path string, logger *slog.Logger) (*Store, error) {
	s := &Store{
		path:    path,
		pending: make(map[string]*TrafficDelta),
		logger:  logger,
	}
	if path == "" {
		return s, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("读取缓冲文件 %s: %w", path, err)
	}
	if len(raw) == 0 {
		return s, nil
	}

	var f storeFile
	if err := json.Unmarshal(raw, &f); err != nil {
		// 文件损坏不应让程序起不来：备份后从空缓冲继续。
		backup := path + ".corrupt"
		_ = os.Rename(path, backup)
		logger.Warn("缓冲文件损坏，已备份并重新开始", "backup", backup, "error", err)
		return s, nil
	}
	for _, d := range f.Pending {
		if d.UserID == "" {
			continue
		}
		s.pending[d.UserID] = &TrafficDelta{UserID: d.UserID, TxDelta: d.TxDelta, RxDelta: d.RxDelta}
	}
	if len(s.pending) > 0 {
		logger.Info("已从缓冲文件恢复未上报流量", "users", len(s.pending))
	}
	return s, nil
}

// Merge 把新采集到的增量累加进缓冲，并立即落盘。
// 必须落盘：否则进程重启（或掉电）会丢掉尚未上报的流量。
func (s *Store) Merge(deltas []TrafficDelta) {
	s.mu.Lock()

	changed := false
	for _, d := range deltas {
		if d.UserID == "" || (d.TxDelta == 0 && d.RxDelta == 0) {
			continue
		}
		if cur, ok := s.pending[d.UserID]; ok {
			cur.TxDelta += d.TxDelta
			cur.RxDelta += d.RxDelta
		} else {
			s.pending[d.UserID] = &TrafficDelta{UserID: d.UserID, TxDelta: d.TxDelta, RxDelta: d.RxDelta}
		}
		changed = true
	}

	var err error
	if changed {
		err = s.persistLocked()
	}
	s.mu.Unlock()

	if err != nil {
		s.logger.Error("缓冲落盘失败（数据仍在内存中）", "error", err)
	}
}

// Snapshot 返回当前待上报的快照（按用户 ID 排序，跳过全 0 条目）。
func (s *Store) Snapshot() []TrafficDelta {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]TrafficDelta, 0, len(s.pending))
	for _, d := range s.pending {
		if d.TxDelta == 0 && d.RxDelta == 0 {
			continue
		}
		out = append(out, TrafficDelta{UserID: d.UserID, TxDelta: d.TxDelta, RxDelta: d.RxDelta})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

// Pending 返回待上报的用户数。
func (s *Store) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// Commit 按已成功上报的量扣减缓冲，并落盘。
// 只扣减上报出去的部分，期间新累加的增量保持不变。
func (s *Store) Commit(reported []TrafficDelta) error {
	if len(reported) == 0 {
		return nil
	}

	s.mu.Lock()
	for _, d := range reported {
		cur, ok := s.pending[d.UserID]
		if !ok {
			continue
		}
		if cur.TxDelta >= d.TxDelta {
			cur.TxDelta -= d.TxDelta
		} else {
			cur.TxDelta = 0
		}
		if cur.RxDelta >= d.RxDelta {
			cur.RxDelta -= d.RxDelta
		} else {
			cur.RxDelta = 0
		}
		if cur.TxDelta == 0 && cur.RxDelta == 0 {
			delete(s.pending, d.UserID)
		}
	}
	err := s.persistLocked()
	s.mu.Unlock()
	return err
}

// persistLocked 原子写盘（临时文件 + rename）。调用方需持有锁。
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}

	f := storeFile{Pending: make([]TrafficDelta, 0, len(s.pending))}
	ids := make([]string, 0, len(s.pending))
	for uid := range s.pending {
		ids = append(ids, uid)
	}
	sort.Strings(ids)
	for _, uid := range ids {
		d := s.pending[uid]
		f.Pending = append(f.Pending, TrafficDelta{UserID: d.UserID, TxDelta: d.TxDelta, RxDelta: d.RxDelta})
	}

	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化缓冲: %w", err)
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("创建目录 %s: %w", dir, err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o640); err != nil {
		return fmt.Errorf("写入临时文件 %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("替换缓冲文件 %s: %w", s.path, err)
	}
	return nil
}
