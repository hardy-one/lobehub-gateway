# ASR Gateway Go

`asr-gateway-go` 是 LobeHub 实时语音听写网关的 Go 语言实现版本。它面向自托管部署场景，负责桥接 LobeHub 前端的实时语音 PCM 流与后端第三方流式语音识别服务（如火山引擎豆包大模型流式语音识别）。

## 架构职责

1. **会话握手**：处理 `POST /webapi/asr/realtime/session` 请求，签发临时 Session ID 与 WebSocket 地址。
2. **协议适配与推流**：在 `GET /api/v1/realtime/ws` 接收前端 16kHz PCM 音频分片，按对应第三方 ASR 协议封包推流。
3. **事件转换回显**：接收 ASR 引擎识别结果，转回 LobeHub 标准的 `transcript.partial`（临时结果）与 `transcript.final`（分句确认结果）。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ASR_PORT` | `8789` | ASR 网关监听端口 |
| `ASR_PROVIDER` | `volcengine` | ASR 后端提供商，支持 `volcengine` |
| `VOLCENGINE_VOICE_API_KEY` | — | 火山引擎 API Key |
| `VOLCENGINE_RESOURCE_ID` | `volc.seedasr.sauc.duration` | 模型资源 ID，默认为豆包流式 2.0 |
| `VOLCENGINE_ENDPOINT` | `wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async` | 火山引擎 WebSocket 端点 |
