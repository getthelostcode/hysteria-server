// config.go — config.yaml 加载 / 默认值 / 校验
package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ---- 默认值 ----
const (
	DefaultTrafficInterval = 60 * time.Second
	DefaultKickInterval    = 10 * time.Second
	DefaultKickCooldown    = 30 * time.Second
	DefaultHTTPTimeout     = 10 * time.Second
	DefaultMaxRetries      = 3
	DefaultStorePath       = "/var/lib/hysteria-node-agent/pending.json"

	DefaultKickListPath      = "/api/v1/kick/list"
	DefaultTrafficReportPath = "/api/v1/traffic/report"
	DefaultKickAckPath       = "/api/v1/kick/ack"

	FormatBatch   = "batch"    // {"node_id":..,"timestamp":..,"users":[{"user_id":..,"upload":..,"download":..}]}
	FormatPerUser = "per_user" // {"user_id":..,"tx_delta":..,"rx_delta":..}
)

// HysteriaAPIConfig 是 Hysteria 2 服务端本地 API 的配置。
type HysteriaAPIConfig struct {
	BaseURL        string `yaml:"base_url"`
	Secret         string `yaml:"secret"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
	// ClearAfterRead 为 true 时使用 /traffic?clear=1：
	// Hysteria 每次返回「自上次读取以来的增量」并在服务端清零。
	// 为 false 时返回累计值，由本程序在本地计算差值。
	ClearAfterRead *bool `yaml:"clear_after_read"`
}

// Linux1Config 是 Linux1 HTTP API 的可选精细配置（不写则用默认值）。
type Linux1Config struct {
	KickListPath      string            `yaml:"kick_list_path"`
	TrafficReportPath string            `yaml:"traffic_report_path"`
	KickAckPath       string            `yaml:"kick_ack_path"`
	TrafficFormat     string            `yaml:"traffic_format"` // batch | per_user
	NodeID            string            `yaml:"node_id"`
	HMACSecret        string            `yaml:"hmac_secret"` // 非空则签名（X-Node-ID / X-Timestamp / X-Nonce / X-Signature）
	BearerToken       string            `yaml:"bearer_token"`
	ExtraHeaders      map[string]string `yaml:"extra_headers"`
	TimeoutSeconds    int               `yaml:"timeout_seconds"`
	MaxRetries        int               `yaml:"max_retries"`
	KickAckRaw        *bool             `yaml:"kick_ack_enabled"` // 是否上报踢人确认，默认 false
}

// StoreConfig 未上报流量的本地落盘配置。
type StoreConfig struct {
	Path string `yaml:"path"`
}

// Config 是 config.yaml 的完整结构。
type Config struct {
	Linux1APIBase           string            `yaml:"linux1_api_base"`
	HysteriaAPI             HysteriaAPIConfig `yaml:"hysteria_api"`
	TrafficIntervalSeconds  int               `yaml:"traffic_interval_seconds"`
	KickPollIntervalSeconds int               `yaml:"kick_poll_interval_seconds"`
	KickCooldownSeconds     int               `yaml:"kick_cooldown_seconds"`
	Linux1                  Linux1Config      `yaml:"linux1"`
	Store                   StoreConfig       `yaml:"store"`
	LogLevel                string            `yaml:"log_level"`
}

// Load 读取并校验配置文件。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(false)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("解析 YAML %s: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	c.Linux1APIBase = strings.TrimRight(strings.TrimSpace(c.Linux1APIBase), "/")
	c.HysteriaAPI.BaseURL = strings.TrimRight(strings.TrimSpace(c.HysteriaAPI.BaseURL), "/")
	c.HysteriaAPI.Secret = strings.TrimSpace(c.HysteriaAPI.Secret)

	if c.TrafficIntervalSeconds <= 0 {
		c.TrafficIntervalSeconds = int(DefaultTrafficInterval / time.Second)
	}
	if c.KickPollIntervalSeconds <= 0 {
		c.KickPollIntervalSeconds = int(DefaultKickInterval / time.Second)
	}
	if c.KickCooldownSeconds <= 0 {
		c.KickCooldownSeconds = int(DefaultKickCooldown / time.Second)
	}
	if c.HysteriaAPI.TimeoutSeconds <= 0 {
		c.HysteriaAPI.TimeoutSeconds = int(DefaultHTTPTimeout / time.Second)
	}
	if c.HysteriaAPI.ClearAfterRead == nil {
		v := true
		c.HysteriaAPI.ClearAfterRead = &v
	}

	l1 := &c.Linux1
	if l1.KickListPath == "" {
		l1.KickListPath = DefaultKickListPath
	}
	if l1.TrafficReportPath == "" {
		l1.TrafficReportPath = DefaultTrafficReportPath
	}
	if l1.KickAckPath == "" {
		l1.KickAckPath = DefaultKickAckPath
	}
	if l1.TrafficFormat == "" {
		l1.TrafficFormat = FormatBatch
	}
	if l1.TimeoutSeconds <= 0 {
		l1.TimeoutSeconds = int(DefaultHTTPTimeout / time.Second)
	}
	if l1.MaxRetries < 0 {
		l1.MaxRetries = 0
	}
	if l1.MaxRetries == 0 {
		l1.MaxRetries = DefaultMaxRetries
	}
	if l1.NodeID == "" {
		l1.NodeID = "node-01"
	}
	if l1.ExtraHeaders == nil {
		l1.ExtraHeaders = map[string]string{}
	}

	if c.Store.Path == "" {
		c.Store.Path = DefaultStorePath
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
}

func (c *Config) validate() error {
	if c.Linux1APIBase == "" {
		return fmt.Errorf("配置项 linux1_api_base 不能为空")
	}
	if u, err := url.Parse(c.Linux1APIBase); err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("linux1_api_base 不是合法 URL: %q（示例 http://1.2.3.4:8000）", c.Linux1APIBase)
	}
	if c.HysteriaAPI.BaseURL == "" {
		return fmt.Errorf("配置项 hysteria_api.base_url 不能为空")
	}
	if u, err := url.Parse(c.HysteriaAPI.BaseURL); err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("hysteria_api.base_url 不是合法 URL: %q", c.HysteriaAPI.BaseURL)
	}
	switch c.Linux1.TrafficFormat {
	case FormatBatch, FormatPerUser:
	default:
		return fmt.Errorf("linux1.traffic_format 只能是 %q 或 %q，当前为 %q",
			FormatBatch, FormatPerUser, c.Linux1.TrafficFormat)
	}
	if c.Linux1.HMACSecret != "" && c.Linux1.NodeID == "" {
		return fmt.Errorf("启用 linux1.hmac_secret 时必须配置 linux1.node_id")
	}
	return nil
}

// ---- Duration 便捷方法 ----

func (c *Config) TrafficInterval() time.Duration {
	return time.Duration(c.TrafficIntervalSeconds) * time.Second
}

func (c *Config) KickInterval() time.Duration {
	return time.Duration(c.KickPollIntervalSeconds) * time.Second
}

func (c *Config) KickCooldown() time.Duration {
	return time.Duration(c.KickCooldownSeconds) * time.Second
}

func (h HysteriaAPIConfig) Timeout() time.Duration {
	return time.Duration(h.TimeoutSeconds) * time.Second
}

func (l Linux1Config) Timeout() time.Duration {
	return time.Duration(l.TimeoutSeconds) * time.Second
}

// KickAckEnabled 是否在上报踢人成功后调用 Linux1 的确认接口。
func (l Linux1Config) KickAckEnabled() bool {
	if l.KickAckRaw == nil {
		return false
	}
	return *l.KickAckRaw && l.KickAckPath != ""
}
