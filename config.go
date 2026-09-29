// config.go - 配置结构与加载逻辑
//
// 职责：解析 config.yaml，填充默认值，校验必要字段。
// 配置文件缺失或格式错误时记录错误并退出（启动失败是致命的）。
package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是全局配置单例，启动时加载一次，运行期间只读。
type Config struct {
	Listen       string            `yaml:"listen"`
	Redis        RedisConfig       `yaml:"redis"`
	Nodes        map[string]string `yaml:"nodes"`        // node_id -> HMAC secret
	Heartbeat    HeartbeatConfig   `yaml:"heartbeat"`
	Log          LogConfig         `yaml:"log"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	PoolSize int    `yaml:"pool_size"`
}

type HeartbeatConfig struct {
	TTL          string `yaml:"ttl"`               // 心跳过期时间，格式如 "60s"
	ScanInterval string `yaml:"scan_interval"`     // 掉线扫描间隔，格式如 "30s"
}

type LogConfig struct {
	Level string `yaml:"level"` // debug|info|warn|error
}

// DefaultConfig 返回带默认值的配置（供合并使用）。
func DefaultConfig() Config {
	return Config{
		Listen: ":8080",
		Redis: RedisConfig{
			Addr:     "127.0.0.1:6379",
			Password: "",
			DB:       0,
			PoolSize: 50,
		},
		Heartbeat: HeartbeatConfig{
			TTL:          "60s",
			ScanInterval: "30s",
		},
		Log: LogConfig{
			Level: "info",
		},
	}
}

// Load 从路径读取 YAML 配置，未指定字段回退默认值。
// 调用方需保证填入必要字段（nodes 非空、redis.addr 非空）。
func Load(path string) (Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// 配置文件不存在时使用纯默认值（方便开发环境）
			return cfg, nil
		}
		return cfg, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// yaml: 反序列化时未出现在 YAML 中的字段保留为默认值。
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("解析 YAML 失败: %w", err)
	}

	// 填补从 YAML 丢失的默认字段（嵌套 struct 的零值覆盖默认值问题）
	if cfg.Heartbeat.TTL == "" {
		cfg.Heartbeat.TTL = "60s"
	}
	if cfg.Heartbeat.ScanInterval == "" {
		cfg.Heartbeat.ScanInterval = "30s"
	}
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info"
	}

	return cfg, nil
}

// Validate 检查配置的必要条件。
func (c *Config) Validate() error {
	if c.Listen == "" {
		return fmt.Errorf("listen 不能为空")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr 不能为空")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("nodes 至少配置一个节点")
	}
	// 解析时长字符串，尽早发现配置错误
	if _, err := time.ParseDuration(c.Heartbeat.TTL); err != nil {
		return fmt.Errorf("heartbeat.ttl 无效: %w", err)
	}
	if _, err := time.ParseDuration(c.Heartbeat.ScanInterval); err != nil {
		return fmt.Errorf("heartbeat.scan_interval 无效: %w", err)
	}
	return nil
}

// NodeExists 检查 node_id 是否在配置的 nodes 列表中。
func (c *Config) NodeExists(nodeID string) bool {
	if c == nil {
		return false
	}
	_, ok := c.Nodes[nodeID]
	return ok
}
