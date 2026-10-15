// Package provider defines the platform runtimes a message gateway can hold:
// one Provider per held connection, selected by platform id.
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"sync"
)

// Connection modes as LobeHub assigns them to a connection.
const (
	ModePolling   = "polling"
	ModeWebhook   = "webhook"
	ModeWebsocket = "websocket"
)

// Connection states reported back to LobeHub. LobeHub maps `connected`,
// `disconnected`, `dormant` and `error` onto its own runtime status and treats
// `connecting` as "no news yet".
const (
	StateConnected    = "connected"
	StateConnecting   = "connecting"
	StateDisconnected = "disconnected"
	StateDormant      = "dormant"
	StateError        = "error"
)

// ErrUnsupportedPlatform is returned when no provider is registered for a
// connection's platform.
var ErrUnsupportedPlatform = errors.New("unsupported platform")

// ErrUnsupportedMode is returned when a platform is servable, but not over the
// transport the connection asked for. Refusing beats connecting and silently
// receiving nothing.
var ErrUnsupportedMode = errors.New("unsupported connection mode")

// CodedError lets a provider name a failure the way the platform does, so
// LobeHub's channel UI shows the platform's own code instead of an opaque
// message.
type CodedError interface {
	error
	Code() string
}

// Code extracts the platform error code from a provider failure, or "".
func Code(err error) string {
	var coded CodedError
	if errors.As(err, &coded) {
		return coded.Code()
	}
	return ""
}

// Config is the connect payload LobeHub posts to `POST /api/connections`.
// Field names match the wire format exactly.
type Config struct {
	ApplicationID  string          `json:"applicationId,omitempty"`
	Capabilities   json.RawMessage `json:"capabilities,omitempty"`
	ConnectionID   string          `json:"connectionId"`
	ConnectionMode string          `json:"connectionMode,omitempty"`
	Credentials    map[string]any  `json:"credentials,omitempty"`
	Platform       string          `json:"platform"`
	UserID         string          `json:"userId,omitempty"`
	WebhookPath    string          `json:"webhookPath,omitempty"`
}

// Credential returns a string credential, or "" when absent or not a string.
func (c Config) Credential(key string) string {
	value, ok := c.Credentials[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return text
}

// WebhookToken is the credential LobeHub expects on forwarded inbound events.
// WeChat carries it inside the connect payload; every other platform validates
// the shared service token instead.
func (c Config) WebhookToken(serviceToken string) string {
	if token := c.Credential("webhookToken"); token != "" {
		return token
	}
	return serviceToken
}

// SameAs reports whether two payloads describe the same connection: the same
// platform, transport and endpoint, with identical credentials. LobeHub's
// reconcile sends `ensure: true` for connections it expects to already exist,
// and an identical config must not tear the live connection down.
func (c Config) SameAs(other Config) bool {
	if c.Platform != other.Platform ||
		c.ConnectionMode != other.ConnectionMode ||
		c.WebhookPath != other.WebhookPath ||
		c.ApplicationID != other.ApplicationID ||
		c.UserID != other.UserID {
		return false
	}
	if len(c.Credentials) != len(other.Credentials) {
		return false
	}
	for key, value := range c.Credentials {
		if !reflect.DeepEqual(value, other.Credentials[key]) {
			return false
		}
	}
	return true
}

// StateChange is one connection state transition on its way to LobeHub.
type StateChange struct {
	State string
	// Code and Message carry the platform's own failure text; both are
	// surfaced in the LobeHub channel UI.
	Code    string
	Message string
}

// Inbound is one raw platform event on its way to LobeHub. Body is forwarded
// byte-for-byte: the LobeHub-side handler owns the platform payload shape.
type Inbound struct {
	Body    []byte
	Headers map[string]string
}

// Env carries the deployment-level endpoints a provider needs, so provider
// packages never read the environment themselves and stay testable.
type Env struct {
	DiscordAPIBaseURL  string
	DiscordGatewayURL  string
	SlackAPIBaseURL    string
	TelegramAPIBaseURL string
	WechatBaseURL      string
}

// Deps are the collaborators a provider uses to reach the outside world.
type Deps struct {
	Env Env
	// Forward delivers one inbound platform event to the connection's webhook.
	Forward func(ctx context.Context, event Inbound) error
	// Report publishes a state change; it must not block the caller.
	Report func(change StateChange)
	Log    *slog.Logger
}

// Provider is one held platform connection.
type Provider interface {
	// Start establishes the link and returns once LobeHub can be told the
	// connection is up. Persistent transports return as soon as the link is
	// proven live and keep maintaining it in the background.
	Start(ctx context.Context) error
	// Stop tears the link down and releases every background worker.
	Stop(ctx context.Context) error
	// SetTyping turns the platform typing indicator on or off for a thread id.
	SetTyping(ctx context.Context, platformThreadID string, active bool) error
}

// Factory builds the provider for one connection.
type Factory func(cfg Config, deps Deps) (Provider, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register makes a platform servable. Providers register themselves from
// their package init.
func Register(platform string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := factories[platform]; exists {
		panic(fmt.Sprintf("provider already registered for platform %q", platform))
	}
	factories[platform] = factory
}

// New builds the provider for a connection.
func New(cfg Config, deps Deps) (Provider, error) {
	mu.RLock()
	factory, ok := factories[cfg.Platform]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedPlatform, cfg.Platform)
	}
	return factory(cfg, deps)
}

// Platforms lists every servable platform, sorted. This is what the gateway
// declares through `GET /api/admin/capabilities`.
func Platforms() []string {
	mu.RLock()
	defer mu.RUnlock()
	platforms := make([]string, 0, len(factories))
	for platform := range factories {
		platforms = append(platforms, platform)
	}
	sort.Strings(platforms)
	return platforms
}
