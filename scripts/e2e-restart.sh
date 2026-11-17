#!/usr/bin/env bash
# LOBE-185 一键 e2e 入口：编译真实 gateway 二进制，跑 e2e 测试。
# 失败时保留日志与数据库文件（E2E_RUN_DIR），成功时清理临时目录。
# 退出码反映成败，可直接用于合并前门禁。
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_DIR="$REPO_ROOT/agent-gateway-go"
BIN="$APP_DIR/agent-gateway-go-e2e"
RUN_ROOT="${TMPDIR:-/tmp}/lobehub-gateway-e2e-$$"

echo "==> 编译真实二进制"
(cd "$APP_DIR" && go build -o "$BIN" ./cmd/agent-gateway-go) || {
  echo "编译失败" >&2
  exit 1
}

cleanup() {
  status=$?
  if [ "$status" -eq 0 ]; then
    echo "==> 全部场景通过，清理临时目录 $RUN_ROOT"
    rm -rf "$RUN_ROOT"
    rm -f "$BIN"
  else
    echo "==> e2e 失败（退出码 $status），保留现场: $RUN_ROOT" >&2
    echo "    日志与数据库文件在上述目录的各场景子目录中，可直接排查" >&2
  fi
  pkill -f "$(basename "$BIN")" 2>/dev/null || true
  exit "$status"
}
trap cleanup EXIT

echo "==> 运行 e2e 场景（日志与数据库目录: $RUN_ROOT）"
mkdir -p "$RUN_ROOT"
cd "$APP_DIR"
E2E_GATEWAY_BIN="$BIN" E2E_RUN_DIR="$RUN_ROOT" \
  go test -tags e2e -count=1 -timeout 5m ./e2e/ -v
