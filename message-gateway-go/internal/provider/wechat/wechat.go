// Package wechat holds a WeChat connection through the iLink Bot API: a
// long-polling loop that hands each finished user message to LobeHub and keeps
// the per-user context token needed to drive the typing indicator.
package wechat

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

const (
	defaultBaseURL = "https://ilinkai.weixin.qq.com"
	channelVersion = "1.0.0"

	// pollTimeout sits above the ~35s the iLink server holds an empty long
	// poll, so a normal hold is not mistaken for a dead link.
	pollTimeout = 40 * time.Second
	// probeTimeout bounds the readiness probe: a link that is up answers
	// quickly, and Start must not block a gateway API call for a full hold.
	probeTimeout   = 3 * time.Second
	requestTimeout = 15 * time.Second

	retryDelayInitial = time.Second
	retryDelayMax     = 10 * time.Second
	// sessionExpiredBackoff matches the refresh window the in-process client
	// uses for the same condition: the QR-issued session is gone and only a new
	// scan brings it back.
	sessionExpiredBackoff = time.Hour

	// iLink message enums.
	messageTypeBot   = 2
	messageStateDone = 2
)

var errSessionExpired = errors.New("wechat session expired")

func init() {
	provider.Register("wechat", newFactory)
}

type getUpdatesResponse struct {
	Errmsg               string            `json:"errmsg"`
	Errcode              int               `json:"errcode"`
	GetUpdatesBuf        string            `json:"get_updates_buf"`
	LongpollingTimeoutMs int               `json:"longpolling_timeout_ms"`
	Msgs                 []json.RawMessage `json:"msgs"`
	Ret                  int               `json:"ret"`
}

// messageEnvelope reads only the routing fields off an inbound message; the
// bytes are forwarded untouched.
type messageEnvelope struct {
	ContextToken string `json:"context_token"`
	FromUserID   string `json:"from_user_id"`
	MessageState *int   `json:"message_state"`
	MessageType  *int   `json:"message_type"`
}

// Provider is one held WeChat bot session.
type Provider struct {
	cfg          provider.Config
	deps         provider.Deps
	baseURL      string
	botToken     string
	webhookToken string

	httpClient *http.Client

	mu            sync.Mutex
	contextTokens map[string]string

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

func newFactory(cfg provider.Config, deps provider.Deps) (provider.Provider, error) {
	botToken := strings.TrimSpace(cfg.Credential("botToken"))
	if botToken == "" {
		return nil, errors.New("credential botToken is required")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(cfg.Credential("baseUrl")), "/")
	if baseURL == "" {
		baseURL = strings.TrimRight(deps.Env.WechatBaseURL, "/")
	}
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	return &Provider{
		cfg:           cfg,
		deps:          deps,
		baseURL:       baseURL,
		botToken:      botToken,
		contextTokens: map[string]string{},
		httpClient:    &http.Client{Timeout: pollTimeout + 5*time.Second},
		// LobeHub writes the shared service token into the connect payload as
		// `webhookToken`; the gateway adds its own default when a payload
		// carries none.
		webhookToken: strings.TrimSpace(cfg.Credential("webhookToken")),
	}, nil
}

// Start proves the session works with a short poll, hands over whatever that
// poll already delivered, then keeps long-polling in the background.
func (p *Provider) Start(ctx context.Context) error {
	probeCtx, cancelProbe := context.WithTimeout(ctx, probeTimeout)
	response, err := p.getUpdates(probeCtx, "")
	cancelProbe()

	switch {
	case err == nil:
		p.processMessages(ctx, response.Msgs)
		p.start(ctx, response.GetUpdatesBuf)
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		// A hold that outlives the probe means the link is up and simply has
		// nothing to deliver yet.
		p.deps.Log.Debug("wechat readiness probe timed out, continuing")
		p.start(ctx, "")
	default:
		return err
	}

	return nil
}

func (p *Provider) Stop(context.Context) error {
	p.lifecycleMu.Lock()
	cancel, done := p.cancel, p.done
	p.cancel, p.done = nil, nil
	p.lifecycleMu.Unlock()

	if cancel == nil {
		return nil
	}
	cancel()
	<-done
	return nil
}

func (p *Provider) start(parent context.Context, cursor string) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})

	p.lifecycleMu.Lock()
	p.cancel, p.done = cancel, done
	p.lifecycleMu.Unlock()

	go func() {
		defer close(done)
		p.pollLoop(ctx, cursor)
	}()
}

func (p *Provider) pollLoop(ctx context.Context, cursor string) {
	retryDelay := retryDelayInitial

	for {
		if ctx.Err() != nil {
			return
		}

		response, err := p.getUpdates(ctx, cursor)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errSessionExpired) {
				p.deps.Report(provider.StateChange{
					Code:    "-14",
					Message: "WeChat session expired; a new QR scan is required",
					State:   provider.StateDormant,
				})
				if !sleep(ctx, sessionExpiredBackoff) {
					return
				}
				continue
			}

			p.deps.Log.Warn("wechat poll failed", "error", err)
			if !sleep(ctx, retryDelay) {
				return
			}
			retryDelay = min(retryDelay*2, retryDelayMax)
			continue
		}

		retryDelay = retryDelayInitial
		if response.GetUpdatesBuf != "" {
			cursor = response.GetUpdatesBuf
		}
		p.deps.Report(provider.StateChange{State: provider.StateConnected})
		p.processMessages(ctx, response.Msgs)
	}
}

// processMessages forwards every finished user message and remembers the
// per-user context token the typing indicator needs. Bot-authored and
// still-generating messages are skipped, matching the reply rules.
func (p *Provider) processMessages(ctx context.Context, messages []json.RawMessage) {
	for _, raw := range messages {
		var envelope messageEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			p.deps.Log.Warn("wechat message could not be read", "error", err)
			continue
		}
		if envelope.MessageType != nil && *envelope.MessageType == messageTypeBot {
			continue
		}
		if envelope.MessageState != nil && *envelope.MessageState != messageStateDone {
			continue
		}

		if envelope.FromUserID != "" && envelope.ContextToken != "" {
			p.mu.Lock()
			p.contextTokens[envelope.FromUserID] = envelope.ContextToken
			p.mu.Unlock()
		}

		p.forward(ctx, raw)
	}
}

func (p *Provider) forward(ctx context.Context, raw json.RawMessage) {
	headers := map[string]string{}
	if p.webhookToken != "" {
		headers["Authorization"] = "Bearer " + p.webhookToken
	}

	if err := p.deps.Forward(ctx, provider.Inbound{Body: raw, Headers: headers}); err != nil {
		p.deps.Log.Warn("wechat message forward failed", "error", err)
	}
}

// SetTyping shows or clears the WeChat typing indicator for one user. iLink
// issues a per-conversation typing ticket, so the indicator is best-effort: a
// user we have never heard from has no context token to ask for one.
func (p *Provider) SetTyping(ctx context.Context, platformThreadID string, active bool) error {
	userID := parseThreadID(platformThreadID)
	if userID == "" {
		return nil
	}

	p.mu.Lock()
	contextToken := p.contextTokens[userID]
	p.mu.Unlock()
	if contextToken == "" {
		return nil
	}

	var config struct {
		TypingTicket string `json:"typing_ticket"`
	}
	err := p.post(ctx, "/ilink/bot/getconfig", map[string]any{
		"base_info":     baseInfo(),
		"context_token": contextToken,
		"ilink_user_id": userID,
	}, &config)
	if err != nil {
		return err
	}
	if config.TypingTicket == "" {
		return nil
	}

	status := 1
	if !active {
		status = 2
	}
	return p.post(ctx, "/ilink/bot/sendtyping", map[string]any{
		"base_info":     baseInfo(),
		"ilink_user_id": userID,
		"status":        status,
		"typing_ticket": config.TypingTicket,
	}, nil)
}

func (p *Provider) getUpdates(ctx context.Context, cursor string) (*getUpdatesResponse, error) {
	requestCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	body, err := json.Marshal(map[string]any{
		"base_info":       baseInfo(),
		"get_updates_buf": cursor,
	})
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, p.baseURL+"/ilink/bot/getupdates", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	p.setHeaders(request)

	response, err := p.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<22))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("getupdates failed (%d): %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}

	var parsed getUpdatesResponse
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &parsed); err != nil {
			return nil, fmt.Errorf("invalid getupdates response: %w", err)
		}
	}
	if parsed.Ret != 0 {
		if parsed.Errcode == -14 {
			return nil, errSessionExpired
		}
		return nil, fmt.Errorf("getupdates rejected (ret=%d errcode=%d): %s", parsed.Ret, parsed.Errcode, parsed.Errmsg)
	}
	return &parsed, nil
}

func (p *Provider) post(ctx context.Context, path string, payload any, target any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, p.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	p.setHeaders(request)

	response, err := p.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%s failed (%d): %s", path, response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	if target == nil || len(responseBody) == 0 {
		return nil
	}
	return json.Unmarshal(responseBody, target)
}

func (p *Provider) setHeaders(request *http.Request) {
	request.Header.Set("Authorization", "Bearer "+p.botToken)
	request.Header.Set("AuthorizationType", "ilink_bot_token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-WECHAT-UIN", randomUin())
}

func baseInfo() map[string]any {
	return map[string]any{"channel_version": channelVersion}
}

// randomUin builds the per-request X-WECHAT-UIN the iLink API expects: the
// base64 of a random uint32 rendered as decimal text.
func randomUin() string {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return base64.StdEncoding.EncodeToString([]byte("0"))
	}
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(uint64(binary.BigEndian.Uint32(buf[:])), 10)))
}

// parseThreadID reads `wechat:<type>:<userId>`: the user id is everything after
// the second separator, because it may itself contain colons.
func parseThreadID(platformThreadID string) string {
	parts := strings.SplitN(platformThreadID, ":", 3)
	if len(parts) < 3 || parts[0] != "wechat" {
		return ""
	}
	return parts[2]
}

func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
