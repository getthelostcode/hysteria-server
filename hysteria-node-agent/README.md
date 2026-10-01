# hysteria-node-agent

运行在 **Linux2**（Hysteria 2 服务端所在机器）的常驻程序。

它 **不启动也不管理 Hysteria 进程**，只通过 Hysteria 2 官方 Traffic Stats API 做两件事：

1. **踢人**：定时从 Linux1 的 HTTP API 拉取待踢用户列表 → 调用 Hysteria 本地 API `POST /kick` 踢下线（可选：回执给 Linux1）。
2. **上报流量**：定时从 Hysteria 本地 API `GET /traffic` 采集各用户流量 → 计算增量 → 上报给 Linux1 的 HTTP API。

```
        Linux1 (业务/管理侧)                      Linux2 (本程序所在机器)
┌───────────────────────────────┐        ┌───────────────────────────────────────┐
│  HTTP API                     │        │  hysteria-node-agent                  │
│   GET  /api/v1/kick/list  ◄───┼────────┼── kicker  每 10s 拉列表 → 踢人 → ack    │
│   POST /api/v1/traffic/report │        │                                       │
│   POST /api/v1/kick/ack   ◄───┼────────┼── reporter 每 60s 采集 → 上报          │
└───────────────────────────────┘        │      │                ▲               │
                                         │      ▼                │               │
                                         │  本地缓冲 pending.json │               │
                                         │      │                │               │
                                         │      ▼  HTTP           │               │
                                         │  Hysteria 2 本地 API ──┘               │
                                         │   GET /traffic?clear=1                 │
                                         │   POST /kick                           │
                                         └───────────────────────────────────────┘
```

---

## 1. 快速开始

### 一键安装（推荐）

```bash
git clone https://github.com/getthelostcode/hysteria-node-agent.git
cd hysteria-node-agent
sudo ./install.sh http://<linux1_ip>:<port>

# 带 Hysteria 本地 API 地址与 secret（secret 为空时会提示补填）
sudo ./install.sh http://<linux1_ip>:<port> http://127.0.0.1:8080 'your_secret' node-01
```

脚本会：安装二进制到 `/usr/local/bin/`、生成 `/etc/hysteria-node-agent/config.yaml`、
创建 `/var/lib/hysteria-node-agent/`、安装并启动 `systemd` 服务，最后打印连通性验证结果。

### 手动运行

```bash
go build -o hysteria-node-agent .
./hysteria-node-agent -config ./config.yaml          # 常驻
./hysteria-node-agent -config ./config.yaml -once    # 各跑一轮后退出（排错用）
./hysteria-node-agent -version
```

### 常用命令

```bash
journalctl -u hysteria-node-agent -f        # 看日志
systemctl restart hysteria-node-agent       # 改完配置重启
/usr/local/bin/hysteria-node-agent -config /etc/hysteria-node-agent/config.yaml -once
```

---

## 2. 配置文件

最小配置（其余项都有默认值，完整带注释的模板见仓库根目录 `config.yaml`）：

```yaml
# Linux1 HTTP API 基础地址
linux1_api_base: "http://1.2.3.4:8000"

# Hysteria API 配置
hysteria_api:
  base_url: "http://127.0.0.1:8080"
  secret: "your_secret"

# 上报间隔（秒），默认 60
traffic_interval_seconds: 60

# 获取踢人列表的间隔（秒），默认 10
kick_poll_interval_seconds: 10
```

### 全部配置项

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `linux1_api_base` | 必填 | Linux1 HTTP API 地址 |
| `hysteria_api.base_url` | 必填 | Hysteria 本地 API，如 `http://127.0.0.1:8080` |
| `hysteria_api.secret` | 空 | Hysteria `trafficStats.secret`，作为 `Authorization` 头 |
| `hysteria_api.clear_after_read` | `true` | `true` 时用 `/traffic?clear=1`，返回「上次读取以来的增量」并清零 |
| `traffic_interval_seconds` | `60` | 流量采集上报周期 |
| `kick_poll_interval_seconds` | `10` | 踢人列表轮询周期 |
| `kick_cooldown_seconds` | `30` | 同一用户在冷却期内不重复踢（客户端会自动重连） |
| `linux1.kick_list_path` | `/api/v1/kick/list` | 拉取待踢列表 |
| `linux1.traffic_report_path` | `/api/v1/traffic/report` | 上报流量 |
| `linux1.kick_ack_path` | `/api/v1/kick/ack` | 踢人确认 |
| `linux1.kick_ack_enabled` | `false` | 是否上报踢人确认 |
| `linux1.traffic_format` | `batch` | `batch`（一次报一批）或 `per_user`（一次一个用户） |
| `linux1.node_id` | `node-01` | 节点标识 |
| `linux1.hmac_secret` | 空 | 非空则启用 HMAC-SHA256 请求签名 |
| `linux1.bearer_token` | 空 | 非空则附加 `Authorization: Bearer <token>` |
| `linux1.extra_headers` | `{}` | 自定义请求头 |
| `linux1.timeout_seconds` | `10` | 请求超时 |
| `linux1.max_retries` | `3` | 失败重试次数（指数退避；4xx 不重试） |
| `store.path` | `/var/lib/hysteria-node-agent/pending.json` | 未上报流量本地缓冲 |
| `log_level` | `info` | `debug` / `info` / `warn` / `error` |

---

## 3. 接口契约

### 3.1 Linux1 ← 本程序调用

**GET `{linux1_api_base}{linux1.kick_list_path}`** — 待踢用户列表。以下结构都能解析（大小写/字段名做了兼容）：

```json
["user-a", "user-b"]
{"kick": [{"user_id": "user-a", "reason": "quota_exceeded"}]}
{"users": ["user-a"]}          // 也支持 {"user_ids": [...]} / {"data": [...]} / {"list": [...]}
{"users": {"user-a": {"reason": "expired"}}}
```

**POST `{linux1_api_base}{linux1.traffic_report_path}`** — 流量增量。

`traffic_format: batch`（默认，一次上报全部用户）：

```json
{
  "node_id": "node-01",
  "timestamp": 1730000000,
  "users": [
    {"user_id": "user-a", "tx_delta": 1024, "rx_delta": 2048, "upload": 1024, "download": 2048}
  ]
}
```

`traffic_format: per_user`（每个用户一次请求，字段与常见 Python/FastAPI 服务端一致）：

```json
{"user_id": "user-a", "tx_delta": 1024, "rx_delta": 2048}
```

> batch 格式同时带上 `tx_delta/rx_delta` 与 `upload/download` 两套字段名，兼容只认其中一套的
> 服务端（多余的 JSON 字段会被忽略）。

响应中若包含超限用户，本程序会立刻调用 Hysteria `/kick`。支持的字段名：
`quota_exceeded` / `exceeded` / `kick` / `kick_users` / `users_to_kick`。

```json
{"ok": true, "quota_exceeded": ["user-a"]}
```

**POST `{linux1_api_base}{linux1.kick_ack_path}`**（需 `kick_ack_enabled: true`）— 踢人确认：

```json
{"node_id": "node-01", "user_ids": ["user-a"]}
```

### 3.2 Hysteria 2 ← 本程序调用

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/traffic[?clear=1]` | `{"用户ID": {"tx": 上传字节, "rx": 下载字节}}` |
| POST | `/kick` | body 为**裸 JSON 数组** `["user-a","user-b"]` |
| GET | `/online` | 在线用户与设备数（仅用于日志/诊断） |

鉴权：请求头 `Authorization: <secret>`（Hysteria 官方 Traffic Stats API 的约定）；
`POST /kick` 额外把 secret 放进 query（`?secret=...`），以兼容只在 URL 上校验 secret 的构建版本。

---

## 4. 工作方式与可靠性设计

**流量增量**
`clear_after_read: true`（默认）时每次读 `/traffic?clear=1`，Hysteria 直接返回「自上次读取以来的增量」并清零，
本程序无需维护累计基线，服务器重启导致的计数器归零也不会产生负数增量。
若设为 `false`，则由服务端返回累计值、本程序在本地做差值（同样对归零做保护）。

**缓冲补报（不丢流量）**
采集到的增量先写入 `/var/lib/hysteria-node-agent/pending.json`（原子写，临时文件 + rename），
上报成功后按「已上报的量」精确扣减，而不是清空——因此上报期间新采集的数据不受影响。
Linux1 宕机 / 网络中断时数据留在缓冲里，恢复后自动补报；进程重启会从文件恢复。
`per_user` 模式下逐个上报，也逐个记账：失败时只保留未确认的那部分。

**踢人**
- 踢出成功后才记冷却时间，失败的用户下个周期立即重试。
- Hysteria 客户端被踢后会自动重连，这是协议行为。**真正封禁用户必须在 Linux1 的认证后端
  把该用户标记为禁用**，否则只会看到「踢掉—重连」的循环。`kick_ack_enabled: true` 就是把这个
  结果回执给 Linux1（Linux1 据此把用户移出待踢队列并置为 blocked）。
- 冷却时间内的重复请求会被跳过并打日志，避免每 10 秒踢同一批人。

**健壮性**
每个周期都有 panic 兜底（单周期异常不会让进程退出）；HTTP 请求带超时与指数退避重试
（4xx 不重试）；收到 `SIGTERM/SIGINT` 时停止两个循环并落盘缓冲后退出（配合 systemd 的 `TimeoutStopSec=20`）。

---

## 5. 联调验证结果

用一对本地假服务（`mock_servers.py` / `optional_paths_mock.py`，仅标准库）跑通了以下场景：

| 场景 | 结果 |
|---|---|
| 拉取列表 → Hysteria `/kick` | Hysteria 侧实际收到 `["bob"]`，`?secret=` 与 `Authorization` 均校验通过 |
| 踢人确认 | Linux1 实际收到 `{"node_id":"node-01","user_ids":["bob"]}` |
| 流量采集 → 增量上报 | 第一周期 `alice` 1000/2000 + `bob` 5/5 上报成功；第二周期只报到 `alice` 的新增 3000/4000 |
| Linux1 宕机 | 两个周期的数据留在 `pending.json`（`alice` 4000/6000、`bob` 5/5），日志打印重试中 |
| Linux1 恢复 + 进程重启 | 启动即打印「已从缓冲文件恢复未上报流量 users=2」，补报 `tx=4005 rx=6005` 后缓冲清空 |
| `per_user` 格式 | Linux1 侧严格校验 `{user_id,tx_delta,rx_delta}` 通过 |
| HMAC 签名 | Linux1 侧按独立实现重算签名串 `METHOD\nPATH\nSHA256(body)\nTIMESTAMP\nNONCE`，与实际请求头一致 |
| 响应含 `quota_exceeded` | 立即触发一次 `/kick`（`alice`） |
| 优雅退出 | `SIGINT` → 两个循环停止 → 缓冲落盘 → 退出码 0 |

---

## 6. 排错

| 现象 | 原因与处理 |
|---|---|
| `Hysteria /traffic 返回 401` | `hysteria_api.secret` 与 Hysteria 配置里的 `trafficStats.secret` 不一致 |
| `Hysteria /traffic 返回 404` | Hysteria 未开启 `trafficStats`，需在服务端配置中启用 |
| 日志一直「上报流量到 Linux1 失败」 | Linux1 不可达或路径/鉴权不对；数据已缓冲，恢复后自动补报 |
| `无法识别的踢人列表结构` | Linux1 返回的 JSON 结构不在兼容列表内；按第 3.1 节调整服务端，或改动 `parseKickList` |
| 用户被踢后马上回来 | 协议行为；需在 Linux1 认证后端封禁该用户，并用 `kick_ack_enabled` 回执 |
| `linux1.traffic_format` 校验失败 | 只能是 `batch` 或 `per_user` |

---

## 7. 目录结构

```
hysteria-node-agent/
├── main.go                     # 入口：配置加载、两个循环、信号处理
├── config.go                   # config.yaml 解析、默认值、校验
├── hysteria.go                 # Hysteria 2 API 客户端（/traffic /kick /online）
├── linux1.go                   # Linux1 API 客户端（列表/上报/确认 + HMAC 签名 + 重试）
├── reporter.go                 # 流量采集上报循环
├── kicker.go                   # 踢人轮询循环 + 冷却
├── store.go                    # 未上报流量的本地缓冲（原子写盘）
├── config.yaml                 # 带注释的配置模板
├── install.sh                  # 一键安装脚本
├── hysteria-node-agent.service # systemd 单元
└── README.md
```

依赖：Go 1.21+，仅 `gopkg.in/yaml.v3`（HTTP/JSON 全用标准库）。
