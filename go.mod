module github.com/lobehub/lobehub/apps/gateway-go

go 1.26

require (
	github.com/lobehub/lobehub/apps/agent-gateway-go v0.0.0-00010101000000-000000000000
	github.com/lobehub/lobehub/apps/asr-gateway-go v0.0.0-00010101000000-000000000000
	github.com/lobehub/lobehub/apps/device-gateway-go v0.0.0-00010101000000-000000000000
)

replace (
	github.com/lobehub/lobehub/apps/agent-gateway-go => ./agent-gateway-go
	github.com/lobehub/lobehub/apps/asr-gateway-go => ./asr-gateway-go
	github.com/lobehub/lobehub/apps/device-gateway-go => ./device-gateway-go
)
