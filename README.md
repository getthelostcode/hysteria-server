# Hysteria 代理管理系统 — Server

对外唯一的数据入口，负责所有 HTTP 接口 + 唯一负责与 Redis 交互。除 Redis 外无状态，可水平扩展。

## 功能

- 接收 Client 上报的流量增量，累加到 Redis
- 判断用户是否超配额，超限则加入待踢队列
- 提供待踢列表给 Client 拉取
- 接收 Client 的踢人确认，标记用户 blocked
- 记录节点心跳，识别掉线节点
- 管理员手动踢人接口

## 技术栈

- Go 1.22+
- HTTP 框架：Gin
- Redis：github.com/redis/go-redis/v9
- 配置：gopkg.in/yaml.v3
- 日志：log/slog
- 部署：systemd + 静态二进制（不使用 Docker）
- 不使用 ORM，不引入除上述外的重依赖

## 快速开始

### 编译

```bash
cd hysteria-server
go mod tidy
go build -o hysteria-server -ldflags="-s -w" -trimpath .
```

### 配置

编辑 `config.yaml`：

```yaml
listen: ":8080"

redis:
  addr: "127.0.0.1:6379"
  password: ""
  db: 0
  pool_size: 50

nodes:                    # 每节点独立 HMAC secret
  node-01: "s3cr3t-f0r-n0d3-01"
  node-02: "s3cr3t-f0r-n0d3-02"

heartbeat:
  ttl: "60s"             # 心跳过期时间
  scan_interval: "30s"   # 掉线扫描间隔

log:
  level: info            # debug | info | warn | error
```

### 运行

```bash
./hysteria-server
```

默认加载当前目录下的 `config.yaml`。

### systemd 部署

```bash
# 创建系统用户
sudo useradd -r -s /sbin/nologin hysteria

# 安装二进制 + 配置
sudo mkdir -p /opt/hysteria-server
sudo cp hysteria-server config.yaml /opt/hysteria-server/
sudo chown -R hysteria:hysteria /opt/hysteria-server

# 安装 systemd unit
sudo cp hysteria-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hysteria-server

# 查看状态
systemctl status hysteria-server
```

## Redis 依赖

Server 启动时不检查 Redis 连接，首次请求时自然验证。Redis 不可用时所有接口返回 `503 Service Unavailable`。

### Redis Key 设计

| Key                            | 类型   | TTL  | 用途                      |
|--------------------------------|--------|------|---------------------------|
| traffic:{user_id}              | HASH   | 永久 | upload/download 累计值    |
| quota:{user_id}                | HASH   | 永久 | limit / blocked / reason  |
| kick:pending:{node_id}         | SET    | 永久 | 待踢用户                  |
| idem:traffic:{node}:{ts}:{uid} | STRING | 1h   | 幂等去重                  |
| node:hb:{node_id}              | STRING | 60s  | 心跳，过期即离线          |
| node:set                       | SET    | 永久 | 所有已知节点，用于掉线扫描 |
| nonce:{node_id}:{nonce}        | STRING | 10m  | 防重放                    |

## HTTP 接口

### 签名校验（除 /healthz 外全部接口）

所有接口（除 `/healthz`）均需 HMAC-SHA256 签名校验。

**请求头：**

```
X-Signature: hex(HMAC-SHA256(secret, sign_string))
X-Timestamp: Unix 秒
X-Nonce:      随机字符串 (建议 16 字节 base64)
X-Node-ID:    节点标识符（与 config.yaml 中 nodes 键匹配）
```

**签名串：**

```
METHOD + "\n" +
PATH (不含 Query) + "\n" +
hex(SHA256(request body)) + "\n" +
timestamp + "\n" +
nonce
```

**校验规则：**

- 时钟窗口 ±5 分钟（拒绝过期请求）
- nonce 用 Redis SETNX + TTL 10min 去重，防重放
- 每节点独立 secret（配置中给一个 map：node_id -> secret）

### 接口列表

| 方法 | 路径 | 签名 | 说明 |
|------|------|------|------|
| POST | /api/v1/traffic/report | 需要 | 客户端上报流量增量 |
| GET | /api/v1/kick/list | 需要 | 拉取待踢用户列表 |
| POST | /api/v1/kick/ack | 需要 | 确认踢人 |
| POST | /api/v1/node/heartbeat | 需要 | 节点心跳上报 |
| POST | /api/v1/admin/kick | 需要 | 管理员手动踢人 |
| GET | /healthz | 不需要 | 健康检查 |

### POST /api/v1/traffic/report

**请求：**
```json
{
  "node_id":   "node-01",
  "timestamp": 1730000000,
  "users": [
    {"user_id": "u1", "upload": 1024, "download": 2048}
  ]
}
```

**响应：**
```json
{
  "ok": true,
  "quota_exceeded": ["u1", "u2"]
}
```

**要求：**
- 幂等：用 (node_id, timestamp, user_id) 作为去重键，Redis SETNX + TTL 1h
- 累加流量 + 判断配额 + 入待踢队列必须用 Lua 脚本保证原子
- 已 blocked 的用户直接返回需踢，不累加

### GET /api/v1/kick/list?node_id=xxx

**响应：**
```json
{
  "kick": [
    {"user_id": "u1", "reason": "quota_exceeded"},
    {"user_id": "u2", "reason": "admin_kick"},
    {"user_id": "u3", "reason": "expired"}
  ]
}
```

**reason 取值：** quota_exceeded / admin_kick / expired

### POST /api/v1/kick/ack

**请求：**
```json
{
  "node_id": "node-01",
  "user_ids": ["u1", "u2"]
}
```

**响应：** `{ "ok": true }`

**要求：** 从 kick:pending:{node_id} 移除，并设置 quota:{uid}.blocked=1

### POST /api/v1/node/heartbeat

**请求：**
```json
{
  "node_id": "node-01",
  "version": "1.0.0",
  "online_users": 150
}
```

**响应：**
```json
{
  "ok": true,
  "interval": 15
}
```

`interval` 是 Server 根据配置建议的上报间隔（秒），Client 可据此调整上报频率。

### POST /api/v1/admin/kick

**请求：**
```json
{
  "node_id": "node-01",
  "user_id": "u1",
  "reason": "admin_kick"
}
```

**响应：** `{ "ok": true }`

**说明：** 管理员手动踢用户，设置 quota.blocked=1 + reason，并加入 kick:pending:{node_id}。

## 错误码

| 代码 | HTTP | 含义 |
|------|------|------|
| E001 | 400 | 缺少签名 Header |
| E002 | 401 | 签名校验失败（时钟窗口 ±5min） |
| E003 | 401 | Nonce 重放（已使用过） |
| E004 | 401 | 未知节点（node_id 不在配置中） |
| E005 | 400 | Body SHA256 不一致（篡改检测） |
| E006 | 429 | 单节点限流过载 |
| E101 | 400 | node_id 缺失 / 用户数组为空 |
| E102 | 400 | timestamp 为空或非数值 |
| E103 | 400 | user_id / upload / download 不合法（负数等） |
| E104 | 500 | 服务器内部错误 |
| E201 | 400 | node_id 缺失 |
| E202 | 500 | 服务器内部错误 |
| E301 | 400 | node_id / user_ids 缺失 |
| E302 | 500 | 服务器内部错误 |
| E401 | 400 | node_id 缺失 |
| E402 | 500 | 服务器内部错误 |
| E501 | 400 | 参数缺失 / 节点未注册 |
| E502 | 500 | 服务器内部错误 |

## 关键边界情况

- **Redis 故障时接口返回什么？是否降级？**
  所有接口在 Redis 操作失败时返回 500（或 503）+ 对应错误码。不降级——流量计量和踢人是核心职责，静默丢数据比返回错误更危险。签名中间件中的 nonce 去重若遇 Redis 错误，会记录警告并允许请求通过（签名已校验，风险可控）。

- **两个并发请求同时上报同一用户是否会超限？**
  不会。Lua 脚本 `TrafficAccumulate` 的 HGET/HINCRBY/HSET 全部在一个 EVAL 调用内执行，Redis 单线程串行处理 EVAL。两个并发请求按顺序执行，先到的累加后判断是否超限，后到的看到已标记 blocked 则直接返回 2。配额不会被突破。

- **节点掉线后 kick:pending 队列如何处理？**
  掉线扫描线程检测到节点心跳过期后：
  - 从 `node:set` 移除该节点。
  - 遍历 `kick:pending:{node_id}` 中的用户，将 `quota:{uid}.reason` 更新为 `"expired"`。
  - 不删除用户的 pending 记录——队列保留，方便客户端重连后查询和 ack。
  - 若客户端重新上线心跳，新的流量上报会基于当前 quota 状态（blocked=1 已生效）正确处理。

- **Server 重启后心跳状态如何重建？**
  `node:hb:{node_id}` 是 TTL STRING，重启后存量清零。`node:set` 是 SET，重启后也空。客户端重新心跳过来时，`Heartbeat()` 方法通过 SADD 重新将其加入 `node:set`。短暂的扫描窗口内所有节点可能被误判为离线（若重启期间无心跳抵达），但这个窗口最多几秒（取决于 scan_interval）。无数据丢失。

- **时钟漂移如何影响签名校验？**
  服务端以自己系统时钟为准，校验窗口 ±5 分钟。客户端时钟快 5 分钟以上会被拒绝（E002）。若服务端时钟本身有漂移，窗口会偏移。建议所有节点运行 NTP（chronyd / systemd-timesyncd）。

## 项目结构

```
hysteria-server/
├── main.go                 # 入口：加载配置、建链、优雅退出
├── config.go               # config.yaml 解析 + 默认值 + 校验
├── server.go               # Gin 路由注册 + 中间件挂载 + engine 构建
├── middleware.go           # HMAC 签名校验 / 请求日志 / 单节点限流
├── handler.go              # 5 个接口的实现
├── redis.go                # Redis 客户端封装、心跳、掉线清理
├── lua.go                  # Lua 脚本与执行封装
├── config.yaml             # 配置示例
├── hysteria-server.service # systemd unit
└── README.md               # 本文件
```

## 许可证

MIT
