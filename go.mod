module github.com/lobehub/lobehub/apps/gateway-go

go 1.26

require (
	github.com/lobehub/lobehub/apps/agent-gateway-go v0.0.0-00010101000000-000000000000
	github.com/lobehub/lobehub/apps/asr-gateway-go v0.0.0-00010101000000-000000000000
	github.com/lobehub/lobehub/apps/device-gateway-go v0.0.0-00010101000000-000000000000
	github.com/lobehub/lobehub/apps/message-gateway-go v0.0.0-00010101000000-000000000000
	github.com/lobehub/lobehub/apps/oss-center-go v0.0.0-00010101000000-000000000000
)

require (
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.15.4 // indirect
	github.com/bytedance/sonic/loader v0.5.2 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/sys v0.37.0 // indirect
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.44.3 // indirect
)

replace (
	github.com/lobehub/lobehub/apps/agent-gateway-go => ./agent-gateway-go
	github.com/lobehub/lobehub/apps/asr-gateway-go => ./asr-gateway-go
	github.com/lobehub/lobehub/apps/device-gateway-go => ./device-gateway-go
	github.com/lobehub/lobehub/apps/message-gateway-go => ./message-gateway-go
	github.com/lobehub/lobehub/apps/oss-center-go => ./oss-center-go
)
