// linux1.go — Linux1 HTTP API 客户端（拉取踢人列表 / 上报流量 / 踢人确认）
//
// 默认接口（可用 config.yaml 的 linux1 段覆盖）：
//
//	GET  {linux1_api_base}/api/v1/kick/list          → 待踢用户列表（JSON）
//	POST {linux1_api_base}/api/v1/traffic/report     → 上报流量增量
//	POST {linux1_api_base}/api/v1/kick/ack           → 确认踢人（默认关闭）
//
// 可选 HMAC 认证（配置 linux1.node_id + linux1.hmac_secret 后启用），
// 签名串与 hysteria-server 的中间件一致：
//
//	METHOD + "\n" + PATH + "\n" + hex(SHA256(body)) + "\n" + timestamp + "\n" + nonce
//	X-Signature = hex(HMAC-SHA256(secret, 上述字符串))
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// KickEntry 是 Linux1 下发的待踢用户条目。
type KickEntry struct {
	UserID string
	Reason string
}

// TrafficDelta 是上报给 Linux1 的单个用户流量增量。
type TrafficDelta struct {
	UserID  string `json:"user_id"`
	TxDelta uint64 `json:"tx_delta"`
	RxDelta uint64 `json:"rx_delta"`
	// 兼容 hysteria-server（Go 版）的字段名：upload / download
	Upload   uint64 `json:"upload,omitempty"`
	Download uint64 `json:"download,omitempty"`
	Token    string `json:"token,omitempty"`
}

// Linux1Client 访问 Linux1 的 HTTP API。
type Linux1Client struct {
	baseURL string
	cfg     Linux1Config
	http    *http.Client
	logger  *slog.Logger
}

// NewLinux1Client 创建客户端。
func NewLinux1Client(baseURL string, cfg Linux1Config, logger *slog.Logger) *Linux1Client {
	return &Linux1Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		cfg:     cfg,
		http: &http.Client{
			Timeout: cfg.Timeout(),
			Transport: &http.Transport{
				MaxIdleConns:        10,
				IdleConnTimeout:     30 * time.Second,
				MaxIdleConnsPerHost: 4,
			},
		},
		logger: logger,
	}
}

// ---- 1. 拉取踢人列表 ----

// FetchKickList 从 Linux1 拉取待踢用户列表。
func (c *Linux1Client) FetchKickList(ctx context.Context) ([]KickEntry, error) {
	query := url.Values{}
	if c.cfg.HMACSecret != "" {
		query.Set("node_id", c.cfg.NodeID)
	}

	body, err := c.do(ctx, http.MethodGet, c.cfg.KickListPath, query, nil)
	if err != nil {
		return nil, err
	}
	return parseKickList(body)
}

// parseKickList 兼容多种列表结构：
//
//	["u1","u2"]
//	{"kick":[{"user_id":"u1","reason":"quota_exceeded"}]}
//	{"users":["u1"]} / {"user_ids":[...]} / {"data":[...]} / {"list":[...]}
//	{"users":{"u1":{"reason":"x"}}}
func parseKickList(body []byte) ([]KickEntry, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, nil
	}

	var raw interface{}
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return nil, fmt.Errorf("解析踢人列表 JSON: %w (原文: %s)", err, truncate(string(trimmed), 200))
	}

	switch v := raw.(type) {
	case []interface{}:
		return entriesFromSlice(v), nil
	case map[string]interface{}:
		for _, key := range []string{"kick", "users", "user_ids", "userids", "data", "list", "items", "pending"} {
			inner, ok := v[key]
			if !ok {
				continue
			}
			switch iv := inner.(type) {
			case []interface{}:
				return entriesFromSlice(iv), nil
			case map[string]interface{}:
				return entriesFromMap(iv), nil
			}
		}
		// 单个用户对象：{"user_id":"u1","reason":"..."}
		if e, ok := entryFromObject(v); ok {
			return []KickEntry{e}, nil
		}
		return nil, fmt.Errorf("无法识别的踢人列表结构: %s", truncate(string(trimmed), 200))
	default:
		return nil, fmt.Errorf("无法识别的踢人列表结构: %s", truncate(string(trimmed), 200))
	}
}

func entriesFromSlice(items []interface{}) []KickEntry {
	out := make([]KickEntry, 0, len(items))
	for _, it := range items {
		switch v := it.(type) {
		case string:
			if v != "" {
				out = append(out, KickEntry{UserID: v})
			}
		case map[string]interface{}:
			if e, ok := entryFromObject(v); ok {
				out = append(out, e)
			}
		}
	}
	return out
}

func entriesFromMap(m map[string]interface{}) []KickEntry {
	ids := make([]string, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Strings(ids)

	out := make([]KickEntry, 0, len(ids))
	for _, uid := range ids {
		e := KickEntry{UserID: uid}
		if obj, ok := m[uid].(map[string]interface{}); ok {
			e.Reason = stringField(obj, "reason", "msg", "message")
		}
		out = append(out, e)
	}
	return out
}

func entryFromObject(obj map[string]interface{}) (KickEntry, bool) {
	uid := stringField(obj, "user_id", "userid", "id", "user", "username", "uid")
	if uid == "" {
		return KickEntry{}, false
	}
	return KickEntry{UserID: uid, Reason: stringField(obj, "reason", "msg", "message")}, true
}

func stringField(obj map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := obj[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// ---- 2. 上报流量 ----

// ReportTraffic 上报一批用户流量增量。
// 返回值：
//
//	reported — 服务端已确认接收的增量（调用方据此精确扣减本地缓冲）
//	exceeded — 服务端判定为超限/待踢的用户
//	err      — 失败原因；此时 reported 中仍是已成功上报的部分
func (c *Linux1Client) ReportTraffic(ctx context.Context, timestamp int64, deltas []TrafficDelta) ([]TrafficDelta, []string, error) {
	if len(deltas) == 0 {
		return nil, nil, nil
	}

	if c.cfg.TrafficFormat == FormatPerUser {
		// Linux1 一次只接收一个用户：逐个上报，逐个记账。
		reported := make([]TrafficDelta, 0, len(deltas))
		var exceeded []string
		for _, d := range deltas {
			body, err := json.Marshal(map[string]interface{}{
				"user_id":  d.UserID,
				"tx_delta": d.TxDelta,
				"rx_delta": d.RxDelta,
			})
			if err != nil {
				return reported, exceeded, fmt.Errorf("序列化流量上报: %w", err)
			}
			respBody, err := c.do(ctx, http.MethodPost, c.cfg.TrafficReportPath, nil, body)
			if err != nil {
				return reported, exceeded, err
			}
			reported = append(reported, d)
			exceeded = append(exceeded, parseQuotaExceeded(respBody)...)
		}
		return reported, exceeded, nil
	}

	users := make([]map[string]interface{}, 0, len(deltas))
	for _, d := range deltas {
		u := map[string]interface{}{
			"user_id":  d.UserID,
			"tx_delta": d.TxDelta,
			"rx_delta": d.RxDelta,
			// 兼容 hysteria-server（Go 版）的字段名，多余字段会被忽略。
			"upload":   d.TxDelta,
			"download": d.RxDelta,
		}
		if d.Token != "" {
			u["token"] = d.Token
		}
		users = append(users, u)
	}

	payload, err := json.Marshal(map[string]interface{}{
		"node_id":   c.cfg.NodeID,
		"timestamp": timestamp,
		"users":     users,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("序列化流量上报: %w", err)
	}

	respBody, err := c.do(ctx, http.MethodPost, c.cfg.TrafficReportPath, nil, payload)
	if err != nil {
		return nil, nil, err
	}
	return deltas, parseQuotaExceeded(respBody), nil
}

// parseQuotaExceeded 从响应体中提取超限用户（不同服务端字段名不同，全部兼容）。
func parseQuotaExceeded(respBody []byte) []string {
	if len(bytes.TrimSpace(respBody)) == 0 {
		return nil
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(respBody, &obj); err != nil {
		return nil
	}

	out := make([]string, 0)
	for _, key := range []string{"quota_exceeded", "exceeded", "kick", "kick_users", "users_to_kick"} {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		arr, ok := raw.([]interface{})
		if !ok {
			continue
		}
		for _, it := range arr {
			switch v := it.(type) {
			case string:
				if v != "" {
					out = append(out, v)
				}
			case map[string]interface{}:
				if uid := stringField(v, "user_id", "userid", "id", "user"); uid != "" {
					out = append(out, uid)
				}
			}
		}
	}
	return out
}

// ---- 3. 踢人确认 ----

// AckKicks 通知 Linux1 已踢出这些用户（可选功能）。
func (c *Linux1Client) AckKicks(ctx context.Context, userIDs []string) error {
	if !c.cfg.KickAckEnabled() || len(userIDs) == 0 {
		return nil
	}

	body, err := json.Marshal(map[string]interface{}{
		"node_id":  c.cfg.NodeID,
		"user_ids": userIDs,
	})
	if err != nil {
		return fmt.Errorf("序列化踢人确认: %w", err)
	}

	_, err = c.do(ctx, http.MethodPost, c.cfg.KickAckPath, nil, body)
	return err
}

// ---- HTTP 底层：签名 / 重试 ----

func (c *Linux1Client) do(ctx context.Context, method, path string, query url.Values, payload []byte) ([]byte, error) {
	attempts := c.cfg.MaxRetries
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			backoff := time.Duration(1<<uint(i-1)) * time.Second
			if backoff > 8*time.Second {
				backoff = 8 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		body, retryable, err := c.attempt(ctx, method, path, query, payload)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
		c.logger.Warn("Linux1 请求失败，准备重试",
			"method", method, "path", path, "attempt", i+1, "max", attempts, "error", err)
	}
	return nil, lastErr
}

func (c *Linux1Client) attempt(ctx context.Context, method, path string, query url.Values, payload []byte) ([]byte, bool, error) {
	endpoint := c.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, false, fmt.Errorf("构造请求 %s %s: %w", method, endpoint, err)
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range c.cfg.ExtraHeaders {
		req.Header.Set(k, v)
	}
	if c.cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.BearerToken)
	}
	if c.cfg.HMACSecret != "" {
		signRequest(req, path, payload, c.cfg.NodeID, c.cfg.HMACSecret)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("请求 %s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if readErr != nil {
		return nil, true, fmt.Errorf("读取响应: %w", readErr)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, false, nil
	}

	// 4xx 是客户端问题（路径/签名/参数），重试无意义；5xx 与 429 可重试。
	retryable := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
	return nil, retryable, fmt.Errorf("Linux1 %s %s 返回 %d: %s",
		method, endpoint, resp.StatusCode, truncate(string(body), 200))
}

// signRequest 附加 HMAC 签名头（与 hysteria-server 中间件兼容）。
func signRequest(req *http.Request, path string, payload []byte, nodeID, secret string) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := randomNonce()

	bodyHash := ""
	if len(payload) > 0 {
		sum := sha256.Sum256(payload)
		bodyHash = hex.EncodeToString(sum[:])
	}

	signStr := req.Method + "\n" + path + "\n" + bodyHash + "\n" + ts + "\n" + nonce
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signStr))

	req.Header.Set("X-Node-ID", nodeID)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", hex.EncodeToString(mac.Sum(nil)))
}

func randomNonce() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf)
}
