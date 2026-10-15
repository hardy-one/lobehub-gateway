package gateway

// Platform runtimes are wired in here. A provider package registers itself from
// its own init, so importing it is what makes that platform servable — and what
// makes it appear in `GET /api/admin/capabilities`, which is how LobeHub learns
// it may route the platform to this gateway.
import (
	_ "github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider/discord"
	_ "github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider/typingonly"
	_ "github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider/wechat"
)
