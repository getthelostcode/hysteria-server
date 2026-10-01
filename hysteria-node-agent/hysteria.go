// hysteria.go — Hysteria 2 服务端本地 API 客户端
//
// 只使用 Hysteria 官方 Traffic Stats API：
//
//	GET  /traffic[?clear=1]   → {"用户ID": {"tx": 123, "rx": 456}, ...}
//	POST /kick                → body: ["用户ID1", "用户ID2"]
//	GET  /online              → {"用户ID": 设备数, ...}
//
// 认证：请求头 Authorization: <secret>（Hysteria 官方文档：
// “If an API secret is set in your configuration, you will need to add the
// Authorization header”）。POST /kick 额外把 secret 放进 query，
// 以兼容部分只在 URL 上校验 secret 的构建版本。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TrafficCounter 是单个用户的累计（或清零后的增量）字节数。
type TrafficCounter struct {
	Tx uint64 `json:"tx"` // 上传（客户端发送）
	Rx uint64 `json:"rx"` // 下载（客户端接收）
}

// HysteriaClient 访问 Hysteria 2 的本地 HTTP API。
type HysteriaClient struct {
	baseURL string
	secret  string
	http    *http.Client
	logger  *slog.Logger
}

// NewHysteriaClient 创建客户端。baseURL 形如 http://127.0.0.1:8080。
func NewHysteriaClient(baseURL, secret string, timeout time.Duration, logger *slog.Logger) *HysteriaClient {
	return &HysteriaClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		secret:  secret,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				IdleConnTimeout:     30 * time.Second,
				DisableCompression:  true,
				MaxIdleConnsPerHost: 4,
			},
		},
		logger: logger,
	}
}

// GetTraffic 读取流量统计。
// clear 为 true 时使用 ?clear=1，返回的是「自上次读取以来的增量」，服务端随后清零。
func (h *HysteriaClient) GetTraffic(ctx context.Context, clear bool) (map[string]TrafficCounter, error) {
	endpoint := h.baseURL + "/traffic"
	if clear {
		endpoint += "?clear=1"
	}

	body, err := h.do(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	// 主流格式：扁平的 user → {tx, rx} 映射。
	var flat map[string]TrafficCounter
	if err := json.Unmarshal(body, &flat); err == nil && flat != nil {
		// 过滤掉 tx/rx 全为 0 的空条目，减少无谓的上报。
		out := make(map[string]TrafficCounter, len(flat))
		for uid, c := range flat {
			if uid == "" {
				continue
			}
			out[uid] = c
		}
		return out, nil
	}

	// 兼容包装格式：{"userids": {"user": {"tx":..,"rx":..}}}
	var wrapped struct {
		UserIDs map[string]TrafficCounter `json:"userids"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.UserIDs != nil {
		return wrapped.UserIDs, nil
	}

	return nil, fmt.Errorf("无法解析 /traffic 响应: %s", truncate(string(body), 200))
}

// Online 返回在线用户及其连接（设备）数。
func (h *HysteriaClient) Online(ctx context.Context) (map[string]int, error) {
	body, err := h.do(ctx, http.MethodGet, h.baseURL+"/online", nil)
	if err != nil {
		return nil, err
	}
	var out map[string]int
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("无法解析 /online 响应: %w", err)
	}
	return out, nil
}

// KickUsers 踢出指定用户（断开其连接）。
//
// 注意：Hysteria 客户端自带重连逻辑，被踢后会立刻重连；
// 真正阻止登录需要 Linux1 侧的认证后端把用户标记为禁用。
func (h *HysteriaClient) KickUsers(ctx context.Context, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}

	payload, err := json.Marshal(userIDs) // Hysteria 期望裸 JSON 数组
	if err != nil {
		return fmt.Errorf("序列化踢人请求: %w", err)
	}

	endpoint := h.baseURL + "/kick"
	if h.secret != "" {
		endpoint += "?secret=" + url.QueryEscape(h.secret)
	}

	if _, err := h.do(ctx, http.MethodPost, endpoint, payload); err != nil {
		return err
	}
	return nil
}

// do 发送请求并返回响应体，非 2xx 返回错误。
func (h *HysteriaClient) do(ctx context.Context, method, endpoint string, payload []byte) ([]byte, error) {
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, fmt.Errorf("构造请求 %s %s: %w", method, endpoint, err)
	}
	if h.secret != "" {
		req.Header.Set("Authorization", h.secret)
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := h.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("调用 Hysteria %s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("读取 Hysteria 响应: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Hysteria %s %s 返回 %d: %s",
			method, endpoint, resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
