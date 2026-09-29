// lua.go - Lua 脚本源码字符串 + redis.NewScript 封装
//
// 所有 Lua 脚本注入使用 redis.NewScript，避免裸 EVAL。
// 脚本加载是懒惰的：首次执行时自动缓存到 Redis，不影响启动速度。
//
// 脚本一（TrafficAccumulate）：
//   原子完成：读取 quota.blocked → 若已 blocked 返回 2 →
//   计算 used = upload + download → 若超限则设 blocked=1 并返回 2 →
//   否则 HINCRBY 累加并返回 1。
//
//   KEYS[1] = traffic:{user_id}       (HASH, 永久)
//   KEYS[2] = quota:{user_id}         (HASH, 永久)
//   ARGV[1] = upload_delta  (整数)
//   ARGV[2] = download_delta (整数)
//
//   返回值：
//     1 = 正常累加完成
//     2 = 用户已 blocked 或已超限（需要踢）
//
// 注意事项：
//   - quota.limit 可能是空字符串（从未设置过 limit）——此时 limit=-1，
//     视为无限额，永不触发超限。
//   - blocked 的比较用字符串 '1'，因为 HSET 存的是字符串。
//   - 脚本内部不接触 kick:pending 集合——由 Go 端根据返回值决定是否 SADD，
//     保持 Lua 脚本专注单一职责。
package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// TrafficAccumulateLua 单用户流量累加 + 配额判断 Lua 脚本。
// 原子执行，避免并发上报导致超限后不一致。
const TrafficAccumulateLua = `
local traffic_key = KEYS[1]
local quota_key    = KEYS[2]
local upload_delta = tonumber(ARGV[1])
local download_delta = tonumber(ARGV[2])

-- 1. 检查是否已被标记为 blocked
local blocked = redis.call('HGET', quota_key, 'blocked')
if blocked == '1' then
    -- 已 blocked：不累加，直接告知调用方该用户需要踢
    return 2
end

-- 2. 计算当前已用流量
local upload   = tonumber(redis.call('HGET', traffic_key, 'upload')   or '0')
local download = tonumber(redis.call('HGET', traffic_key, 'download') or '0')
local used = upload + download

-- 3. 读取限额，未配置则视为无限额（limit = -1）
local limit_str = redis.call('HGET', quota_key, 'limit')
local limit = -1
if limit_str and limit_str ~= '' then
    limit = tonumber(limit_str)
    if not limit then limit = -1 end
end

-- 4. 判断是否超限
if limit >= 0 and used >= limit then
    -- 超限：标记 blocked 并记录原因
    redis.call('HSET', quota_key, 'blocked', '1')
    redis.call('HSET', quota_key, 'reason', 'quota_exceeded')
    return 2
end

-- 5. 未超限：累加流量
redis.call('HINCRBY', traffic_key, 'upload', upload_delta)
redis.call('HINCRBY', traffic_key, 'download', download_delta)

return 1
`

// KickAckLua 从 kick:pending:{node_id} 移除用户，并标记 quota blocked。
// 被 POST /kick/ack 调用。
//
// KEYS[1] = kick:pending:{node_id}  (SET)
// KEYS[2] = quota:{user_id}          (HASH)
// ARGV[1] = user_id
const KickAckLua = `
local kick_key = KEYS[1]
local quota_key = KEYS[2]
local user_id   = ARGV[1]

-- 从待踢队列移除
redis.call('SREM', kick_key, user_id)
-- 标记 blocked（幂等：即使已是 1 也无害）
redis.call('HSET', quota_key, 'blocked', '1')
return 1
`

// AdminKickLua 管理员手动踢人。
// 设置 quota.blocked=1 + reason=admin_kick，并加入 kick:pending:{node_id}。
// 不经过流量配额判断。
//
// KEYS[1] = quota:{user_id}          (HASH)
// KEYS[2] = kick:pending:{node_id}   (SET)
// ARGV[1] = user_id
const AdminKickLua = `
local quota_key  = KEYS[1]
local kick_key   = KEYS[2]
local user_id    = ARGV[1]

redis.call('HSET', quota_key, 'blocked', '1')
redis.call('HSET', quota_key, 'reason', 'admin_kick')
redis.call('SADD', kick_key, user_id)
return 1
`

// scripts 持有所有 redis.NewScript 实例，供 Handler 调用。
type LuaScripts struct {
	TrafficAccumulate *redis.Script
	KickAck           *redis.Script
	AdminKick         *redis.Script
}

// NewLuaScripts 构造所有 Lua 脚本对象。
func NewLuaScripts() LuaScripts {
	return LuaScripts{
		TrafficAccumulate: redis.NewScript(TrafficAccumulateLua),
		KickAck:           redis.NewScript(KickAckLua),
		AdminKick:         redis.NewScript(AdminKickLua),
	}
}

// RunTrafficAccumulate 执行流量累加 Lua 脚本。
// ctx 必须带超时。返回值：1=累加成功，2=blocked/超限。
func (s LuaScripts) RunTrafficAccumulate(ctx context.Context, r *redis.Client, uid, uploadDelta, downloadDelta string) (int, error) {
	keys := []string{keyTraffic(uid), keyQuota(uid)}
	vals := []interface{}{uploadDelta, downloadDelta}
	return s.TrafficAccumulate.Run(ctx, r, keys, vals...).Int()
}

// RunKickAck 执行踢人确认 Lua 脚本。
func (s LuaScripts) RunKickAck(ctx context.Context, r *redis.Client, nodeID, uid string) error {
	keys := []string{keyKickPending(nodeID), keyQuota(uid)}
	_, err := s.KickAck.Run(ctx, r, keys, uid).Result()
	return err
}

// RunAdminKick 执行管理员踢人 Lua 脚本。
func (s LuaScripts) RunAdminKick(ctx context.Context, r *redis.Client, nodeID, uid string) error {
	keys := []string{keyQuota(uid), keyKickPending(nodeID)}
	_, err := s.AdminKick.Run(ctx, r, keys, uid).Result()
	return err
}
