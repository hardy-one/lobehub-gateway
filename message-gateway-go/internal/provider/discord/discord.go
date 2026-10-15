// Package discord holds a Discord connection over the Gateway WebSocket: it
// keeps the session alive (identify, heartbeat, resume) and posts every
// dispatch event to LobeHub in the envelope its Discord webhook expects.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

const (
	defaultAPIBaseURL = "https://discord.com/api/v10"
	defaultGatewayURL = "wss://gateway.discord.gg"
	gatewayVersion    = "10"

	// GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT | DIRECT_MESSAGES |
	// GUILD_MESSAGE_REACTIONS | DIRECT_MESSAGE_REACTIONS — the intents the
	// chat SDK identifies with, so LobeHub sees the same event stream it would
	// see running the connection itself.
	gatewayIntents = 46593

	opDispatch     = 0
	opHeartbeat    = 1
	opIdentify     = 2
	opResume       = 6
	opReconnect    = 7
	opInvalidSess  = 9
	opHello        = 10
	opHeartbeatAck = 11

	readyTimeout   = 20 * time.Second
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
	// healthySession is how long a session must survive before the next
	// attempt gets a fresh backoff budget.
	healthySession = time.Minute

	requestTimeout = 15 * time.Second

	userAgent = "DiscordBot (https://github.com/lobehub/lobehub-gateway, 1.0)"

	closeAuthFailed        = 4004
	closeInvalidSeq        = 4007
	closeSessionTimedOut   = 4009
	closeInvalidShard      = 4010
	closeShardingRequired  = 4011
	closeInvalidAPIVersion = 4012
	closeInvalidIntents    = 4013
	closeDisallowedIntents = 4014
)

var errReconnect = errors.New("discord asked for a reconnect")

func init() {
	provider.Register("discord", newFactory)
}

// gatewayError carries the platform's own code so LobeHub's channel UI can
// show it, and marks failures that no retry can fix (a rejected token, or
// intents the application is not allowed to request).
type gatewayError struct {
	code    string
	fatal   bool
	message string
}

func (e *gatewayError) Error() string { return e.message }

func (e *gatewayError) Code() string { return e.code }

type gatewayPayload struct {
	D  json.RawMessage `json:"d"`
	T  string          `json:"t"`
	Op int             `json:"op"`
	S  *int            `json:"s"`
}

type helloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type readyData struct {
	SessionID string `json:"session_id"`
}

// sessionState is what a later attempt needs to resume instead of identifying
// again; nil means "start a fresh session".
type sessionState struct {
	id        string
	seq       int
	resumable bool
}

// Provider is one held Discord connection.
type Provider struct {
	cfg        provider.Config
	deps       provider.Deps
	token      string
	apiBaseURL string
	gatewayURL string

	httpClient *http.Client

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

func newFactory(cfg provider.Config, deps provider.Deps) (provider.Provider, error) {
	token := strings.TrimSpace(cfg.Credential("botToken"))
	if token == "" {
		return nil, errors.New("credential botToken is required")
	}

	apiBaseURL := strings.TrimRight(deps.Env.DiscordAPIBaseURL, "/")
	if apiBaseURL == "" {
		apiBaseURL = defaultAPIBaseURL
	}
	gatewayURL := deps.Env.DiscordGatewayURL
	if gatewayURL == "" {
		gatewayURL = defaultGatewayURL
	}

	return &Provider{
		cfg:        cfg,
		deps:       deps,
		token:      token,
		apiBaseURL: apiBaseURL,
		gatewayURL: gatewayURL,
		httpClient: &http.Client{Timeout: requestTimeout},
	}, nil
}

// Start opens the gateway session and returns once Discord has accepted it, so
// LobeHub only records a connection that is actually online.
func (p *Provider) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	first := make(chan error, 1)

	p.lifecycleMu.Lock()
	p.cancel, p.done = cancel, done
	p.lifecycleMu.Unlock()

	go func() {
		defer close(done)
		p.run(runCtx, first)
	}()

	select {
	case err := <-first:
		if err != nil {
			cancel()
			<-done
			return err
		}
		return nil
	case <-ctx.Done():
		cancel()
		<-done
		return ctx.Err()
	}
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

// run keeps a session up for as long as the connection is held. It reports the
// outcome of the first attempt on `first` and then retries on its own.
func (p *Provider) run(ctx context.Context, first chan<- error) {
	var (
		session  *sessionState
		backoff  = initialBackoff
		reported bool
	)

	for {
		if ctx.Err() != nil {
			return
		}

		startedAt := time.Now()
		next, err := p.session(ctx, session, func() { reportFirst(first, &reported, nil) })
		lasted := time.Since(startedAt)
		reportFirst(first, &reported, err)

		if err != nil {
			if code := provider.Code(err); code != "" && !retryableCode(code) {
				p.deps.Log.Error("discord session failed permanently", "code", code, "error", err)
				p.deps.Report(provider.StateChange{
					State:   provider.StateError,
					Code:    code,
					Message: err.Error(),
				})
				return
			}
			p.deps.Log.Warn("discord session ended", "error", err)
		}

		if err == nil || lasted > healthySession {
			backoff = initialBackoff
		}
		session = next

		if ctx.Err() != nil {
			return
		}
		if !sleep(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// session runs one gateway connection from dial to disconnect, and returns the
// state a following attempt may resume from. `established` fires once the
// session is accepted, which is what Start waits for.
func (p *Provider) session(ctx context.Context, resume *sessionState, established func()) (*sessionState, error) {
	client, err := dialWebSocket(ctx, p.gatewayURL+"?v="+gatewayVersion+"&encoding=json")
	if err != nil {
		return resume, err
	}
	defer client.Close()

	// An established session deliberately has no read deadline — it waits as
	// long as the link lives — so this watcher is what unblocks the read when
	// the connection is torn down.
	stopWatching := watchContext(ctx, client)
	defer stopWatching()

	interval, err := p.greet(ctx, client)
	if err != nil {
		return resume, err
	}

	state := &sessionState{}
	if resume != nil && resume.resumable && resume.id != "" {
		state = &sessionState{id: resume.id, seq: resume.seq, resumable: true}
		if err := writePayload(client, map[string]any{
			"op": opResume,
			"d": map[string]any{
				"token":      p.token,
				"session_id": state.id,
				"seq":        state.seq,
			},
		}); err != nil {
			return state, err
		}
	} else if err := p.identify(client); err != nil {
		return nil, err
	}

	if _, err := p.awaitEstablished(ctx, client, state); err != nil {
		return state, err
	}

	established()
	p.deps.Report(provider.StateChange{State: provider.StateConnected})

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()

	connection := newHeartbeat(client, interval, state)
	go connection.run(heartbeatCtx)

	return p.read(ctx, client, state, connection)
}

// reportFirst delivers the outcome of the first attempt exactly once.
func reportFirst(first chan<- error, reported *bool, err error) {
	if *reported {
		return
	}
	*reported = true
	first <- err
}

// greet performs the handshake: it reads HELLO and answers with the heartbeat
// interval Discord will expect.
func (p *Provider) greet(ctx context.Context, client *wsClient) (time.Duration, error) {
	payload, err := p.readPayload(ctx, client, readyTimeout)
	if err != nil {
		return 0, err
	}
	if payload.Op != opHello {
		return 0, fmt.Errorf("discord sent op %d before HELLO", payload.Op)
	}

	var hello helloData
	if err := json.Unmarshal(payload.D, &hello); err != nil {
		return 0, fmt.Errorf("invalid HELLO payload: %w", err)
	}

	interval := time.Duration(hello.HeartbeatInterval) * time.Millisecond
	if interval <= 0 {
		interval = 45 * time.Second
	}
	return interval, nil
}

func (p *Provider) identify(client *wsClient) error {
	return writePayload(client, map[string]any{
		"op": opIdentify,
		"d": map[string]any{
			"token":   p.token,
			"intents": gatewayIntents,
			"properties": map[string]string{
				"os":      "linux",
				"browser": "lobehub-gateway-go",
				"device":  "lobehub-gateway-go",
			},
		},
	})
}

// awaitEstablished reads until Discord confirms the session with READY or
// RESUMED.
func (p *Provider) awaitEstablished(ctx context.Context, client *wsClient, state *sessionState) (bool, error) {
	deadline := time.Now().Add(readyTimeout)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, errors.New("timed out waiting for the discord session to be accepted")
		}

		payload, err := p.readPayload(ctx, client, remaining)
		if err != nil {
			return false, err
		}
		p.trackSequence(state, payload)

		switch payload.Op {
		case opHello:
			continue
		case opDispatch:
			switch payload.T {
			case "READY":
				var ready readyData
				if err := json.Unmarshal(payload.D, &ready); err != nil {
					return false, fmt.Errorf("invalid READY payload: %w", err)
				}
				state.id = ready.SessionID
				state.resumable = true
				return true, nil
			case "RESUMED":
				state.resumable = true
				return true, nil
			}
		case opReconnect:
			return false, errReconnect
		case opInvalidSess:
			return false, p.invalidSession(payload)
		case opHeartbeat:
			// Discord can ask for an immediate heartbeat while a session is
			// being established.
			if err := p.sendHeartbeat(client, state); err != nil {
				return false, err
			}
		}
	}
}

// read consumes the established session: it forwards dispatches to LobeHub and
// keeps the heartbeat bookkeeping current until the session ends.
func (p *Provider) read(ctx context.Context, client *wsClient, state *sessionState, connection *heartbeat) (*sessionState, error) {
	for {
		payload, err := p.readPayload(ctx, client, 0)
		if err != nil {
			keepResumable(state, err)
			return state, err
		}
		p.trackSequence(state, payload)

		switch payload.Op {
		case opDispatch:
			p.forwardDispatch(ctx, payload)
		case opHeartbeatAck:
			connection.acked()
		case opHeartbeat:
			if err := p.sendHeartbeat(client, state); err != nil {
				return state, err
			}
		case opReconnect:
			return state, errReconnect
		case opInvalidSess:
			return nil, p.invalidSession(payload)
		}
	}
}

// forwardDispatch posts one gateway event to the connection's webhook. The
// envelope and header are the ones LobeHub's Discord webhook handler reads.
func (p *Provider) forwardDispatch(ctx context.Context, payload gatewayPayload) {
	if payload.T == "" {
		return
	}

	envelope, err := json.Marshal(map[string]any{
		"type":      "GATEWAY_" + payload.T,
		"timestamp": time.Now().UnixMilli(),
		"data":      payload.D,
	})
	if err != nil {
		p.deps.Log.Warn("discord event could not be encoded", "type", payload.T, "error", err)
		return
	}

	if err := p.deps.Forward(ctx, provider.Inbound{
		Body:    envelope,
		Headers: map[string]string{"x-discord-gateway-token": p.token},
	}); err != nil {
		p.deps.Log.Warn("discord event forward failed", "type", payload.T, "error", err)
	}
}

// readPayload reads one gateway frame. A zero timeout means "wait as long as
// the connection lives".
func (p *Provider) readPayload(ctx context.Context, client *wsClient, timeout time.Duration) (gatewayPayload, error) {
	var payload gatewayPayload

	if timeout > 0 {
		if err := client.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return payload, err
		}
		defer func() { _ = client.SetReadDeadline(time.Time{}) }()
	}

	opcode, frame, err := client.Read()
	if err != nil {
		if errors.Is(err, errClosed) {
			return payload, closeError(client.CloseCode(), client.CloseReason())
		}
		return payload, err
	}
	if opcode != opText {
		return payload, fmt.Errorf("unexpected websocket frame type %d", opcode)
	}
	if err := json.Unmarshal(frame, &payload); err != nil {
		return payload, fmt.Errorf("invalid gateway payload: %w", err)
	}
	return payload, nil
}

// watchContext unblocks a bounded read when the caller's context ends; the
// read itself only unblocks on a deadline, a frame, or a closed connection.
func watchContext(ctx context.Context, client *wsClient) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = client.Close()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func (p *Provider) sendHeartbeat(client *wsClient, state *sessionState) error {
	var sequence any
	if state.resumable {
		sequence = state.seq
	}
	return writePayload(client, map[string]any{"op": opHeartbeat, "d": sequence})
}

// trackSequence keeps the resume cursor current: every dispatched event carries
// the sequence number a resumed session replays from.
func (p *Provider) trackSequence(state *sessionState, payload gatewayPayload) {
	if payload.S != nil {
		state.seq = *payload.S
	}
}

func (p *Provider) invalidSession(payload gatewayPayload) error {
	var resumable bool
	_ = json.Unmarshal(payload.D, &resumable)
	if !resumable {
		return &gatewayError{code: "invalid_session", message: "discord rejected the session; a fresh identify is required"}
	}
	return &gatewayError{code: "invalid_session_resumable", message: "discord invalidated the session; it can be resumed"}
}

// SetTyping shows Discord's typing indicator for a channel. Discord expires it
// after ~10 seconds, so clearing is a no-op.
func (p *Provider) SetTyping(ctx context.Context, platformThreadID string, active bool) error {
	if !active {
		return nil
	}

	channelID := parseChannelID(platformThreadID)
	if channelID == "" {
		return nil
	}

	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	endpoint := fmt.Sprintf("%s/channels/%s/typing", p.apiBaseURL, channelID)
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bot "+p.token)
	request.Header.Set("User-Agent", userAgent)

	response, err := p.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("discord typing failed (%d): %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// parseChannelID reads `discord:<guildId>:<channelId>[:<threadId>]`, preferring
// the thread when the message came from one.
func parseChannelID(platformThreadID string) string {
	parts := strings.Split(platformThreadID, ":")
	if len(parts) < 3 || parts[0] != "discord" {
		return ""
	}
	if len(parts) > 3 && parts[3] != "" {
		return parts[3]
	}
	return parts[2]
}

func writePayload(client *wsClient, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return client.WriteText(encoded)
}

// closeError turns a close code into the failure LobeHub should see.
func closeError(code int, reason string) error {
	message := fmt.Sprintf("discord closed the connection (%d)", code)
	if reason != "" {
		message += ": " + reason
	}
	return &gatewayError{
		code:    fmt.Sprintf("%d", code),
		fatal:   !retryableCode(fmt.Sprintf("%d", code)),
		message: message,
	}
}

// retryableCode reports whether a failure is worth another attempt. A rejected
// token or intents only a new configuration can fix; everything else (network
// blips, rate limits, expired sessions) heals by retrying.
func retryableCode(code string) bool {
	switch code {
	case fmt.Sprintf("%d", closeAuthFailed),
		fmt.Sprintf("%d", closeInvalidShard),
		fmt.Sprintf("%d", closeShardingRequired),
		fmt.Sprintf("%d", closeInvalidAPIVersion),
		fmt.Sprintf("%d", closeInvalidIntents),
		fmt.Sprintf("%d", closeDisallowedIntents),
		"invalid_session":
		return false
	default:
		return true
	}
}

// keepResumable clears the resume cursor when the close code says the session
// is gone: a missed sequence window or a timed-out session must start over with
// a fresh identify.
func keepResumable(state *sessionState, err error) {
	var coded *gatewayError
	if !errors.As(err, &coded) {
		return
	}
	switch coded.code {
	case fmt.Sprintf("%d", closeInvalidSeq), fmt.Sprintf("%d", closeSessionTimedOut):
		state.resumable = false
	}
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

// heartbeat keeps a session alive: one beat per interval, with a missed
// acknowledgement treated as a dead connection.
type heartbeat struct {
	client   *wsClient
	interval time.Duration
	state    *sessionState

	mu           sync.Mutex
	acknowledged bool
	pending      bool
}

func newHeartbeat(client *wsClient, interval time.Duration, state *sessionState) *heartbeat {
	return &heartbeat{client: client, interval: interval, state: state}
}

func (h *heartbeat) run(ctx context.Context) {
	// Discord expects the first beat after a random fraction of the interval.
	firstDelay := time.Duration(rand.Int63n(int64(h.interval)))
	if !sleep(ctx, firstDelay) {
		return
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		if !h.beat() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// beat sends one heartbeat. A beat that is still unacknowledged when the next
// one is due means the connection is gone: close it so the read loop notices.
func (h *heartbeat) beat() bool {
	h.mu.Lock()
	if h.pending && !h.acknowledged {
		h.mu.Unlock()
		_ = h.client.Close()
		return false
	}
	h.pending = true
	h.acknowledged = false
	state := h.state
	h.mu.Unlock()

	var sequence any
	if state.resumable {
		sequence = state.seq
	}
	if err := writePayload(h.client, map[string]any{"op": opHeartbeat, "d": sequence}); err != nil {
		_ = h.client.Close()
		return false
	}
	return true
}

func (h *heartbeat) acked() {
	h.mu.Lock()
	h.acknowledged = true
	h.mu.Unlock()
}
