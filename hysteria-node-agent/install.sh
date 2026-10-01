#!/usr/bin/env bash
# ============================================================
# hysteria-node-agent 一键安装脚本
#
# 用途：在 Linux2（Hysteria 2 服务端所在机器）安装本管理客户端。
#       程序不启动/不管理 Hysteria 进程，只调用它的本地 API。
#
# 用法：
#   ./install.sh <linux1_api_base> [hysteria_api_base] [hysteria_secret] [node_id]
#
# 例：
#   ./install.sh http://1.2.3.4:8000
#   ./install.sh http://1.2.3.4:8000 http://127.0.0.1:8080 'my_secret' node-01
#
# 也可用环境变量覆盖：
#   LINUX1_API_BASE / HYSTERIA_API_BASE / HYSTERIA_SECRET / NODE_ID
#   TRAFFIC_INTERVAL_SECONDS / KICK_POLL_INTERVAL_SECONDS / KICK_ACK_ENABLED
#   TRAFFIC_FORMAT / HMAC_SECRET / BINARY_URL
# ============================================================
set -euo pipefail

SERVICE_NAME="hysteria-node-agent"
INSTALL_BIN="/usr/local/bin/${SERVICE_NAME}"
CONFIG_DIR="/etc/${SERVICE_NAME}"
CONFIG_FILE="${CONFIG_DIR}/config.yaml"
STATE_DIR="/var/lib/${SERVICE_NAME}"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BLUE=$'\033[34m'; NC=$'\033[0m'
info()  { echo "${BLUE}[INFO]${NC} $*"; }
ok()    { echo "${GREEN}[ OK ]${NC} $*"; }
warn()  { echo "${YELLOW}[WARN]${NC} $*"; }
die()   { echo "${RED}[FAIL]${NC} $*" >&2; exit 1; }

usage() {
  sed -n '3,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 1
}

# ---------- 1. 参数解析 ----------
LINUX1_API_BASE="${1:-${LINUX1_API_BASE:-}}"
HYSTERIA_API_BASE="${2:-${HYSTERIA_API_BASE:-http://127.0.0.1:8080}}"
HYSTERIA_SECRET="${3:-${HYSTERIA_SECRET:-}}"
NODE_ID="${4:-${NODE_ID:-node-01}}"
TRAFFIC_INTERVAL_SECONDS="${TRAFFIC_INTERVAL_SECONDS:-60}"
KICK_POLL_INTERVAL_SECONDS="${KICK_POLL_INTERVAL_SECONDS:-10}"
KICK_ACK_ENABLED="${KICK_ACK_ENABLED:-false}"
TRAFFIC_FORMAT="${TRAFFIC_FORMAT:-batch}"
HMAC_SECRET="${HMAC_SECRET:-}"

[[ -z "${LINUX1_API_BASE}" ]] && usage
[[ "${LINUX1_API_BASE}" =~ ^https?:// ]] || die "linux1_api_base 必须以 http:// 或 https:// 开头，当前：${LINUX1_API_BASE}"
[[ "${HYSTERIA_API_BASE}" =~ ^https?:// ]] || die "hysteria_api_base 必须以 http:// 或 https:// 开头，当前：${HYSTERIA_API_BASE}"

# ---------- 2. 权限检查 ----------
[[ "$(id -u)" -eq 0 ]] || die "请用 root 运行（或 sudo $0 $*）"

# ---------- 3. 依赖检查 ----------
command -v systemctl >/dev/null || die "未找到 systemctl（本脚本需要 systemd）"
command -v curl >/dev/null || die "未找到 curl，请先安装：apt install -y curl"
ARCH="$(uname -m)"
[[ "${ARCH}" == "x86_64" ]] || warn "当前架构 ${ARCH}，预编译二进制是 amd64，请自行编译：go build -o ${SERVICE_NAME} ."

# ---------- 4. 安装二进制 ----------
TMP_BIN="$(mktemp)"
if [[ -x "${SCRIPT_DIR}/${SERVICE_NAME}" ]]; then
  info "使用脚本同目录下的二进制：${SCRIPT_DIR}/${SERVICE_NAME}"
  cp "${SCRIPT_DIR}/${SERVICE_NAME}" "${TMP_BIN}"
elif [[ -n "${BINARY_URL:-}" ]]; then
  info "下载二进制：${BINARY_URL}"
  curl -fL --retry 3 -o "${TMP_BIN}" "${BINARY_URL}" || die "二进制下载失败"
elif command -v go >/dev/null && [[ -f "${SCRIPT_DIR}/go.mod" ]]; then
  info "未找到预编译二进制，使用本机 go 现场编译"
  ( cd "${SCRIPT_DIR}" && go build -o "${TMP_BIN}" . ) || die "go build 失败"
elif command -v /usr/local/go/bin/go >/dev/null && [[ -f "${SCRIPT_DIR}/go.mod" ]]; then
  info "未找到预编译二进制，使用 /usr/local/go/bin/go 现场编译"
  ( cd "${SCRIPT_DIR}" && /usr/local/go/bin/go build -o "${TMP_BIN}" . ) || die "go build 失败"
else
  die "找不到二进制。请把编译好的 ${SERVICE_NAME} 放到脚本同目录，或设置 BINARY_URL 指向下载地址"
fi

install -m 0755 "${TMP_BIN}" "${INSTALL_BIN}"
rm -f "${TMP_BIN}"
ok "已安装二进制：${INSTALL_BIN}（$(${INSTALL_BIN} -version)）"

# ---------- 5. 目录创建 ----------
install -d -m 0755 "${CONFIG_DIR}"
install -d -m 0750 "${STATE_DIR}"

# ---------- 6. 生成配置文件 ----------
if [[ -f "${CONFIG_FILE}" ]]; then
  BACKUP="${CONFIG_FILE}.bak.$(date +%Y%m%d%H%M%S)"
  cp "${CONFIG_FILE}" "${BACKUP}"
  warn "已存在配置文件，原文件备份到 ${BACKUP}"
fi

render_config() {
  cat <<EOF
# 由 install.sh 于 $(date '+%Y-%m-%d %H:%M:%S') 生成

linux1_api_base: "${LINUX1_API_BASE}"

hysteria_api:
  base_url: "${HYSTERIA_API_BASE}"
  secret: "${HYSTERIA_SECRET}"
  clear_after_read: true
  timeout_seconds: 10

traffic_interval_seconds: ${TRAFFIC_INTERVAL_SECONDS}
kick_poll_interval_seconds: ${KICK_POLL_INTERVAL_SECONDS}
kick_cooldown_seconds: 30

linux1:
  kick_list_path: "/api/v1/kick/list"
  traffic_report_path: "/api/v1/traffic/report"
  kick_ack_path: "/api/v1/kick/ack"
  kick_ack_enabled: ${KICK_ACK_ENABLED}
  traffic_format: "${TRAFFIC_FORMAT}"
  node_id: "${NODE_ID}"
  hmac_secret: "${HMAC_SECRET}"
  timeout_seconds: 10
  max_retries: 3

store:
  path: "${STATE_DIR}/pending.json"

log_level: "info"
EOF
}

render_config > "${CONFIG_FILE}"
chmod 600 "${CONFIG_FILE}"
ok "已生成配置文件：${CONFIG_FILE}"
if [[ -z "${HYSTERIA_SECRET}" ]]; then
  warn "hysteria_api.secret 为空：请确认 Hysteria 的 trafficStats.secret，填好后 systemctl restart ${SERVICE_NAME}"
fi

# ---------- 7. 安装 systemd 单元 ----------
if [[ -f "${SCRIPT_DIR}/${SERVICE_NAME}.service" ]]; then
  install -m 0644 "${SCRIPT_DIR}/${SERVICE_NAME}.service" "${UNIT_FILE}"
else
  cat > "${UNIT_FILE}" <<EOF
[Unit]
Description=Hysteria 2 Node Agent (kick relay + traffic reporter)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${INSTALL_BIN} -config ${CONFIG_FILE}
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=20
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths=${STATE_DIR}

[Install]
WantedBy=multi-user.target
EOF
fi
ok "已安装 systemd 单元：${UNIT_FILE}"

# ---------- 8. 启动 ----------
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"
sleep 2
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  ok "服务已启动"
else
  die "服务启动失败，请查看：journalctl -u ${SERVICE_NAME} -n 50 --no-pager"
fi

# ---------- 9. 验证 ----------
echo
echo "==================== 验证 ===================="
echo "--- 1) Hysteria 本地 API 连通性（流量接口）"
HY_HEADER=()
[[ -n "${HYSTERIA_SECRET}" ]] && HY_HEADER=(-H "Authorization: ${HYSTERIA_SECRET}")
if curl -fsS -m 5 "${HY_HEADER[@]}" "${HYSTERIA_API_BASE}/traffic" >/dev/null 2>&1; then
  ok "Hysteria API 可访问：${HYSTERIA_API_BASE}/traffic"
else
  warn "Hysteria API 不可访问，检查 trafficStats.listen 与 secret（当前 secret 是否为空：$([[ -z "${HYSTERIA_SECRET}" ]] && echo 是 || echo 否)）"
fi

echo "--- 2) Linux1 API 连通性（踢人列表）"
if curl -fsS -m 5 "${LINUX1_API_BASE}/api/v1/kick/list" >/dev/null 2>&1; then
  ok "Linux1 API 可访问：${LINUX1_API_BASE}/api/v1/kick/list"
else
  warn "Linux1 API 不可访问（路径或鉴权可能不同，可在 ${CONFIG_FILE} 的 linux1 段调整）"
fi

echo "--- 3) 服务状态"
systemctl status "${SERVICE_NAME}" --no-pager -n 12 || true

echo "--- 4) 最近日志"
journalctl -u "${SERVICE_NAME}" -n 12 --no-pager || true

echo
ok "安装完成"
echo "  配置文件：${CONFIG_FILE}"
echo "  缓冲目录：${STATE_DIR}"
echo "  日志查看：journalctl -u ${SERVICE_NAME} -f"
echo "  手动跑一轮（排错用）：${INSTALL_BIN} -config ${CONFIG_FILE} -once"
