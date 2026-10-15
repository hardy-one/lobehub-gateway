// Package typingonly serves the platforms LobeHub holds no persistent link
// for: their inbound traffic reaches LobeHub through the platform's own
// webhook, so the gateway's whole job is the outbound typing surface LobeHub
// calls into while it generates a reply.
package typingonly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

const (
	defaultSlackAPIBaseURL    = "https://slack.com/api"
	defaultTelegramAPIBaseURL = "https://api.telegram.org"

	requestTimeout = 15 * time.Second
)

func init() {
	provider.Register("slack", newFactory("slack"))
	provider.Register("telegram", newFactory("telegram"))
}

// Provider holds the outbound typing surface for one webhook-mode connection.
type Provider struct {
	cfg        provider.Config
	deps       provider.Deps
	httpClient *http.Client
	platform   string
}

func newFactory(platform string) provider.Factory {
	return func(cfg provider.Config, deps provider.Deps) (provider.Provider, error) {
		// The websocket transport means an inbound link only this platform's
		// own runtime can hold (Slack Socket Mode). This provider has no
		// inbound path, so accepting the connection would report a bot as
		// online while nothing ever reaches LobeHub.
		if cfg.ConnectionMode == provider.ModeWebsocket {
			return nil, fmt.Errorf("%w: %s over %s; this gateway serves %s over %s — switch the channel to %s or run the connection on a gateway that implements the websocket transport",
				provider.ErrUnsupportedMode, platform, provider.ModeWebsocket, platform, provider.ModeWebhook, provider.ModeWebhook)
		}
		if cfg.Credential("botToken") == "" {
			return nil, errors.New("credential botToken is required")
		}
		return &Provider{
			cfg:        cfg,
			deps:       deps,
			httpClient: &http.Client{Timeout: requestTimeout},
			platform:   platform,
		}, nil
	}
}

func (p *Provider) Start(context.Context) error {
	// Nothing to establish: LobeHub registers the platform webhook itself, and
	// inbound events go straight to LobeHub rather than through here.
	return nil
}

func (p *Provider) Stop(context.Context) error { return nil }

// SetTyping shows or clears the typing indicator for one thread. Slack has no
// explicit "done" state — its indicator clears when the reply is posted — so
// the inactive call is a no-op there.
func (p *Provider) SetTyping(ctx context.Context, platformThreadID string, active bool) error {
	switch p.platform {
	case "slack":
		if !active {
			return nil
		}
		return p.setSlackTyping(ctx, platformThreadID)
	case "telegram":
		return p.setTelegramTyping(ctx, platformThreadID, active)
	default:
		return fmt.Errorf("%w: %s", provider.ErrUnsupportedPlatform, p.platform)
	}
}

// setSlackTyping drives Slack's assistant status, the API the chat SDK uses as
// its typing indicator. It needs a thread: a bare channel has no indicator.
func (p *Provider) setSlackTyping(ctx context.Context, platformThreadID string) error {
	channel, threadTS, err := parseSlackThreadID(platformThreadID)
	if err != nil {
		return err
	}
	if threadTS == "" {
		return nil
	}

	endpoint := p.baseURL(p.deps.Env.SlackAPIBaseURL, defaultSlackAPIBaseURL) + "/assistant.threads.setStatus"
	return p.call(ctx, endpoint, map[string]any{
		"channel_id":       channel,
		"loading_messages": []string{"Typing..."},
		"status":           "Typing...",
		"thread_ts":        threadTS,
	}, map[string]string{"Authorization": "Bearer " + p.cfg.Credential("botToken")})
}

// setTelegramTyping drives sendChatAction: "typing" expires after about five
// seconds and "cancel" clears the indicator explicitly.
func (p *Provider) setTelegramTyping(ctx context.Context, platformThreadID string, active bool) error {
	chatID, messageThreadID, err := parseTelegramThreadID(platformThreadID)
	if err != nil {
		return err
	}

	action := "typing"
	if !active {
		action = "cancel"
	}

	payload := map[string]any{"action": action, "chat_id": chatID}
	if messageThreadID != "" {
		payload["message_thread_id"] = messageThreadID
	}

	endpoint := fmt.Sprintf("%s/bot%s/sendChatAction",
		p.baseURL(p.deps.Env.TelegramAPIBaseURL, defaultTelegramAPIBaseURL),
		p.cfg.Credential("botToken"))

	return p.call(ctx, endpoint, payload, nil)
}

func (p *Provider) baseURL(override string, fallback string) string {
	if override != "" {
		return strings.TrimRight(override, "/")
	}
	return strings.TrimRight(fallback, "/")
}

func (p *Provider) call(ctx context.Context, endpoint string, payload map[string]any, headers map[string]string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	response, err := p.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("typing request failed (%d): %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	// Both platforms answer 200 with `ok: false` for a rejected call, so the
	// HTTP status alone is not the outcome.
	if ok, isJSON := okFlag(responseBody); isJSON && !ok {
		return fmt.Errorf("platform rejected typing: %s", strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func okFlag(body []byte) (bool, bool) {
	var payload struct {
		OK *bool `json:"ok"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.OK == nil {
		return false, false
	}
	return *payload.OK, true
}

// parseSlackThreadID reads `slack:<channel>[:<threadTs>]`.
func parseSlackThreadID(platformThreadID string) (string, string, error) {
	parts := strings.Split(platformThreadID, ":")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "slack" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid Slack thread id: %s", platformThreadID)
	}
	if len(parts) == 3 {
		return parts[1], parts[2], nil
	}
	return parts[1], "", nil
}

// parseTelegramThreadID reads `telegram:<chatId>[:<messageThreadId>]`.
func parseTelegramThreadID(platformThreadID string) (string, string, error) {
	parts := strings.Split(platformThreadID, ":")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "telegram" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid Telegram thread id: %s", platformThreadID)
	}
	if len(parts) == 3 {
		if _, err := strconv.Atoi(parts[2]); err != nil {
			return "", "", fmt.Errorf("invalid Telegram thread id: %s", platformThreadID)
		}
		return parts[1], parts[2], nil
	}
	return parts[1], "", nil
}
