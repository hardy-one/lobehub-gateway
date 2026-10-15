# LobeHub Gateway Go

[English](./README.md)

`lobehub-gateway-go` 是 LobeHub 网关服务的 Go 语言重新实现版本。它面向自托管部署场景，目标是在不绑定 Cloudflare 平台、也不依赖外部基础设施的前提下，提供简单的单实例网关服务。

原始网关实现仍然是协议参考。本仓库关注在尽量保持公开 HTTP 与 WebSocket 行为兼容的同时，让网关可以作为普通 Go 服务运行。

## 目标

- 使用 Go 实现 LobeHub 网关协议。
- 采用单实例设计，便于自托管部署。
- 不依赖 Cloudflare Workers 或 Durable Objects。
- 不依赖 Redis、PostgreSQL、NATS 或其他外部运行时服务。
- 在可行范围内保持与参考实现的协议行为兼容。

## 对齐目标

本仓库的协议基准是两份独立参考实现：

- `device-gateway`：`b0bbeab8fef29f7607f432f0cfa481409410dca8`
- `agent-gateway`：`2986083b5c2e570ed3df0242c97532e556b62a70`

对于已经实现的 gateway 服务，对齐目标是兼容这些参考实现暴露的公开 HTTP 路由、WebSocket 消息、请求和响应结构、认证行为、路由语义以及超时行为。同时，Go 版本仍然保持面向自托管的单实例运行时定位。

支持设备健康采样（`device_metrics`）和历史查询（`POST /api/device/metrics`），使用有容量限制的内存存储，最多保留最近 48 小时数据。网关重启后历史数据丢失。

该对齐目标不包含以下能力：

- 管理 API 和管理侧上报。
- 网关管理指标。
- Durable Object 存储和 WebSocket 休眠。
- 多实例协调。
- 地理位置上报。
- LobeHub Workspace（团队工作区）共享设备、工作区设备池及工作区 JWT 路由。仅支持按 `userId` 路由的个人设备。
- 被上游标记为 alpha 的实验室功能。

消息网关没有可对齐的独立参考实现：它的基准是 LobeHub 自身使用的客户端契约（`packages/env` 中的网关配置与服务端 `MessageGatewayClient`），该契约固定了路由、载荷、凭据与状态取值。契约只是隐含的 Durable Object 语义——休眠、park/backoff、统计缓存窗口——有意不复刻：连接失败后仍保持注册，等待下一轮 reconcile 重新下发，而不是被 park 挂起。

## 项目

| 项目 | 状态 | 说明 |
| --- | --- | --- |
| [`device-gateway-go`](./device-gateway-go/README.md) | 已实现 | 设备网关，用于路由 LobeHub 设备连接、状态查询、工具调用、系统信息和 agent 运行请求。 |
| [`agent-gateway-go`](./agent-gateway-go/README.md) | 已实现 | Agent 网关，用于中转浏览器 WebSocket 会话和服务端推送的 agent 事件流。 |
| [`asr-gateway-go`](./asr-gateway-go/README.md) | 已实现 | ASR 语音听写网关，用于桥接 LobeHub 前端实时语音 PCM 流与后端流式语音识别服务（如火山引擎）。 |
| [`message-gateway-go`](./message-gateway-go/README.md) | 已实现 | 消息网关，用于代 LobeHub 持有 IM 平台连接（Discord WebSocket、微信 iLink 长轮询），并把平台事件转发回 LobeHub。 |
| [`oss-center-go`](./oss-center-go/README.md) | 已实现 | 可选的 SystemBot（Messenger）供给器：从自身环境变量声明各平台，经 LobeHub 的管理端点收敛 `system_bot_providers`。 |

## 统一二进制

根模块（`go.mod`）提供统一入口，可以在单个进程中同时启动全部网关服务：

```text
lobehub-gateway-go/
  go.mod                          # umbrella module，通过 replace 指令引用子模块
  cmd/gateway/main.go             # 统一入口：同时运行 agent + device + asr + message（启用后含 oss-center）
  agent-gateway-go/
    gateway.go                    # facade，重导出 internal/gateway 的符号
    go.mod                        # 独立 module（保持不变）
    internal/gateway/
  device-gateway-go/
    gateway.go                    # facade，重导出 internal/gateway 的符号
    go.mod                        # 独立 module（保持不变）
    internal/gateway/
  asr-gateway-go/
    gateway.go                    # facade，重导出 internal/gateway 的符号
    internal/gateway/
    internal/provider/            # ASR 提供商实现（volcengine）
  message-gateway-go/
    gateway.go                    # facade，重导出 internal/gateway 的符号
    internal/gateway/             # 契约端点、连接注册表、状态机、LobeHub 集成
    internal/provider/            # 平台 runtime（discord / wechat / slack / telegram）
  oss-center-go/
    oss_center.go                 # facade，重导出 internal 的符号
    internal/config/              # 环境变量 → 平台声明
    internal/lobehub/             # LobeHub 管理端点客户端
    internal/reconcile/           # 声明式收敛循环
    internal/server/              # 只读 /healthz + /status
```

每个子项目仍然是独立的 Go module，可以单独 build/test。根模块通过 `replace` 指令引用子模块，经由 facade 文件（`gateway.go`）重导出 internal 包的符号。

### 构建与运行

```bash
# 构建统一二进制
 go build -o gateway ./cmd/gateway

# 同时运行全部服务
 SERVICE_TOKEN=your-token ./gateway

# 默认端口：agent :8787，device :8788，asr :8789，message :8790，oss-center :8791（可选）
# 通过环境变量覆盖端口
 AGENT_PORT=9000 DEVICE_PORT=9001 MESSAGE_PORT=9002 SERVICE_TOKEN=your-token ./gateway
```

### 环境变量

| 变量 | 默认值 | 共享 | 说明 |
| --- | --- | --- | --- |
| `SERVICE_TOKEN` | — | 是 | 服务认证令牌（必填） |
| `JWKS_PUBLIC_KEY` | — | 是 | JWT 验证用的 JWKS 公钥 |
| `LOBE_API_BASE_URL` | `https://app.lobehub.com` | 仅 Agent | LobeHub API 基础地址 |
| `AGENT_PORT` | `8787` | 仅 Agent | Agent 网关监听端口 |
| `DEVICE_PORT` | `8788` | 仅 Device | Device 网关监听端口 |
| `ASR_PORT` | `8789` | 仅 ASR | ASR 语音听写网关监听端口 |
| `ASR_PROVIDER` | `volcengine` | 仅 ASR | 实时 ASR 提供商（目前支持 `volcengine`） |
| `VOLCENGINE_VOICE_API_KEY` | — | 仅 ASR | 火山引擎语音识别 API Key |
| `VOLCENGINE_RESOURCE_ID` | `volc.seedasr.sauc.duration` | 仅 ASR | 火山引擎模型资源 ID（默认豆包流式 2.0） |
| `MESSAGE_PORT` | `8790` | 仅 Message | 消息网关监听端口 |
| `OSS_CENTER_ENABLED` | — | 仅 oss-center | 可选：启用 SystemBot 供给器（`MESSENGER_*` 声明见 [`oss-center-go`](./oss-center-go/README.md)） |
| `OSS_CENTER_PORT` | `8791` | 仅 oss-center | oss-center 状态监听端口 |
| `OSS_CENTER_INTERVAL` | `60s` | 仅 oss-center | 收敛 LobeHub 已存 provider 的间隔 |
| `OSS_CENTER_ONESHOT` | — | 仅 oss-center | 启动时只跑一轮，不进入循环 |
| `MESSAGE_GATEWAY_URL` | — | 仅 oss-center | 消息网关地址，用于注册平台级连接 |
| `PULL_TOKEN` | — | 仅 Message | 拉取连接清单用的凭据（对应 LobeHub 的 `MESSAGE_GATEWAY_NODE_PULL_TOKEN`）；留空则只能被推送，重启后无法自愈 |
| `PULL_INTERVAL` | `5m` | 仅 Message | 重新拉取连接清单的间隔 |
| `TYPING_INTERVAL` | `8s` | 仅 Message | 平台「输入中」指示的重发间隔 |
| `READ_TIMEOUT` | `30s` | 是 | HTTP 读取超时 |
| `WRITE_TIMEOUT` | `1m` | 是 | HTTP 写入超时 |
| `REQUEST_LOG_ENABLED` | `false` | 是 | 是否输出每个 HTTP 请求的结构化访问日志 |
| `SHUTDOWN_TIMEOUT` | `10s` | 是 | 优雅关闭超时 |

统一二进制不读取 `PORT`；各服务端口只由 `AGENT_PORT` / `DEVICE_PORT` / `ASR_PORT` / `MESSAGE_PORT` 控制。独立二进制仍像以前一样使用 `PORT`（默认 8787）。

### Release 产物

Linux Release 为 amd64 和 arm64 提供统一二进制及两个独立二进制：

```text
gateway-linux-amd64
agent-gateway-go-linux-amd64
device-gateway-go-linux-amd64
```

ARM 系统请将 `amd64` 替换为 `arm64`。独立二进制的 Release 产物名称包含 `-go`。

### Docker

在本地构建并运行统一镜像：

```bash
docker build -f docker/Dockerfile -t lobehub-gateway-go .
docker run --rm \
  -p 8787:8787 \
  -p 8788:8788 \
  -p 8789:8789 \
  -p 8790:8790 \
  -e SERVICE_TOKEN=your-token \
  lobehub-gateway-go
```

也可以启动仓库提供的 Compose 服务：

```bash
SERVICE_TOKEN=your-token docker compose -f docker/docker-compose.yml up -d --build
```

Compose 文件使用统一二进制的默认监听端口，并透传 `SERVICE_TOKEN`、`JWKS_PUBLIC_KEY`、`LOBE_API_BASE_URL`、`PULL_TOKEN` 和 `REQUEST_LOG_ENABLED`。消息网关只有配置了 `PULL_TOKEN` 才能在重启后自行恢复连接；不配置时仍可被 LobeHub 推送连接，但需要 LobeHub 重新下发。如需使用自定义监听端口或超时，请在 Compose 服务的 `environment` 中添加上表对应变量；修改 `AGENT_PORT`、`DEVICE_PORT`、`ASR_PORT` 或 `MESSAGE_PORT` 时还需同步更新 `ports` 映射。

CD 工作流会发布 linux/amd64 和 linux/arm64 多架构镜像。默认地址为 `ghcr.io/lobehub/lobehub-gateway:<version>`；仓库维护者可以通过 Actions secret `DOCKER_IMAGE` 覆盖目标地址。被选为默认版本的稳定 Release 还会发布 `latest` 标签。

## 架构

本仓库按多个 gateway 服务组织。每个 gateway 都位于独立目录中，便于后续继续添加其他 gateway，而不会和现有服务强耦合。根模块提供统一二进制，可以同时运行所有网关。

当前实现风格刻意保持最小化：状态保存在内存中，服务作为普通 HTTP/WebSocket 服务器运行，部署方式可以是 systemd、Docker、Kubernetes 或任意进程管理器。

## 与 LobeHub Gateway 的关系

相比原始 LobeHub gateway 项目，本仓库有几个有意的差异：

| 维度 | 原始 gateway | 本仓库 |
| --- | --- | --- |
| 编写语言 | TypeScript | Go |
| 运行时 | Cloudflare Workers | 标准 Go 进程 |
| 协调机制 | Durable Objects | 单实例内存状态 |
| 平台绑定 | Cloudflare 相关 | 平台无关 |
| 运行时依赖 | Cloudflare 服务 | 无外部服务依赖 |

由于本仓库面向单实例网关设计，默认不提供分布式会话协调。如果需要水平扩展，请为每个路由域运行一个活动网关实例，或显式引入共享协调层。

## 开发

### 统一二进制

```bash
# 构建
 go build -o gateway ./cmd/gateway

# 从根目录测试根模块（统一二进制）
 go test ./...
```

### 单独网关

每个网关仍然是独立 module，可以单独 build/test：

```bash
cd agent-gateway-go && go test ./...
cd device-gateway-go && go test ./...
cd message-gateway-go && go test ./...
cd oss-center-go && go test ./...
```

各 gateway 的配置、API 路由和本地运行方式请查看对应 README。
