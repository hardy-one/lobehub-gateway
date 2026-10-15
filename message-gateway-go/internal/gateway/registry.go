package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

// ErrConnectionNotFound reports a route that addresses a connection this
// gateway does not hold.
var ErrConnectionNotFound = errors.New("connection not found")

// heldConnection is one connection this gateway keeps alive.
type heldConnection struct {
	cfg      provider.Config
	provider provider.Provider
	typing   *typingScheduler

	cancel context.CancelFunc

	// Guarded by registry.mu.
	state       provider.StateChange
	connectedAt time.Time

	// Last state change already pushed to LobeHub, so an unchanged state does
	// not produce repeated callbacks.
	reported provider.StateChange
}

// registry owns every connection this gateway holds: creation and teardown,
// the state it reports, and the inbound forwarding path back to LobeHub.
type registry struct {
	cfg  Config
	lobe *lobeClient
	log  *slog.Logger

	mu       sync.Mutex
	conns    map[string]*heldConnection
	starting map[string]struct{}
}

func newRegistry(cfg Config, lobe *lobeClient, log *slog.Logger) *registry {
	return &registry{
		cfg:      cfg,
		lobe:     lobe,
		log:      log,
		conns:    map[string]*heldConnection{},
		starting: map[string]struct{}{},
	}
}

// Connect creates or rebuilds a connection and returns the status LobeHub
// should record. A provider's Start only returns once the platform link is
// proven live, so a successful connect is `connected` for every transport;
// links that drop later report through the state callback.
func (r *registry) Connect(ctx context.Context, cfg provider.Config, ensure bool) (string, error) {
	r.mu.Lock()
	if _, inFlight := r.starting[cfg.ConnectionID]; inFlight {
		r.mu.Unlock()
		return provider.StateConnecting, nil
	}

	var previous *heldConnection
	if existing, held := r.conns[cfg.ConnectionID]; held {
		if ensure && existing.cfg.SameAs(cfg) &&
			(existing.state.State == provider.StateConnected || existing.state.State == provider.StateConnecting) {
			// Reconcile against a connection that is already live and identical:
			// leave it running rather than churn the platform link.
			status := existing.state.State
			r.mu.Unlock()
			return status, nil
		}
		previous = existing
		delete(r.conns, cfg.ConnectionID)
	}
	r.starting[cfg.ConnectionID] = struct{}{}
	r.mu.Unlock()

	if previous != nil {
		r.teardown(ctx, previous)
	}

	conn := &heldConnection{
		cfg:    cfg,
		state:  provider.StateChange{State: provider.StateConnecting},
		typing: newTypingScheduler(r.cfg.TypingInterval),
	}

	log := r.log.With("platform", cfg.Platform, "connection_id", cfg.ConnectionID)

	instance, err := provider.New(cfg, provider.Deps{
		Env: provider.Env{
			DiscordAPIBaseURL:  r.cfg.DiscordAPIBaseURL,
			DiscordGatewayURL:  r.cfg.DiscordGatewayURL,
			SlackAPIBaseURL:    r.cfg.SlackAPIBaseURL,
			TelegramAPIBaseURL: r.cfg.TelegramAPIBaseURL,
			WechatBaseURL:      r.cfg.WechatBaseURL,
		},
		Forward: func(ctx context.Context, event provider.Inbound) error {
			headers := event.Headers
			if _, hasAuthorization := headers["Authorization"]; !hasAuthorization {
				// Every forwarded event is a gateway-originated call into
				// LobeHub, so the shared service token is the default
				// credential; a provider that needs its own (Discord's gateway
				// token header, WeChat's webhook token) sets it instead.
				headers = map[string]string{"Authorization": "Bearer " + r.cfg.ServiceToken}
				for key, value := range event.Headers {
					headers[key] = value
				}
			}
			return r.lobe.forward(ctx, cfg.WebhookPath, event.Body, headers)
		},
		Log: log,
		Report: func(change provider.StateChange) {
			r.applyState(conn, change)
		},
	})
	if err != nil {
		r.mu.Lock()
		delete(r.starting, cfg.ConnectionID)
		r.mu.Unlock()
		return "", err
	}
	conn.provider = instance

	startCtx, cancel := context.WithCancel(context.Background())
	conn.cancel = cancel

	r.mu.Lock()
	r.conns[cfg.ConnectionID] = conn
	delete(r.starting, cfg.ConnectionID)
	r.mu.Unlock()

	if err := instance.Start(startCtx); err != nil {
		// Keep the failed connection registered: LobeHub's reconcile still sees
		// the id, reports the failure in the channel UI, and re-arms it with a
		// fresh payload once the user saves new credentials.
		log.Warn("connection start failed", "error", err)
		r.applyState(conn, provider.StateChange{
			State:   provider.StateError,
			Code:    provider.Code(err),
			Message: err.Error(),
		})
		return "", err
	}

	r.applyState(conn, provider.StateChange{State: provider.StateConnected})
	return provider.StateConnected, nil
}

// Disconnect tears one connection down. Deliberately silent towards LobeHub:
// the caller that disconnects (a credential save, an unlink, a reconcile
// sweep) already owns the resulting runtime status, and a callback racing it
// would rewrite a status the user just caused.
func (r *registry) Disconnect(ctx context.Context, connectionID string) (string, bool) {
	r.mu.Lock()
	conn, held := r.conns[connectionID]
	delete(r.conns, connectionID)
	r.mu.Unlock()

	if !held {
		return "", false
	}
	r.teardown(ctx, conn)
	return provider.StateDisconnected, true
}

// DisconnectAll drops every connection; used when LobeHub retires the gateway.
func (r *registry) DisconnectAll(ctx context.Context) int {
	r.mu.Lock()
	conns := make([]*heldConnection, 0, len(r.conns))
	for _, conn := range r.conns {
		conns = append(conns, conn)
	}
	r.conns = map[string]*heldConnection{}
	r.mu.Unlock()

	for _, conn := range conns {
		r.teardown(ctx, conn)
	}
	return len(conns)
}

func (r *registry) teardown(ctx context.Context, conn *heldConnection) {
	conn.typing.StopAll()
	conn.cancel()
	if err := conn.provider.Stop(ctx); err != nil {
		r.log.Warn("connection stop failed",
			"connection_id", conn.cfg.ConnectionID, "platform", conn.cfg.Platform, "error", err)
	}
}

// StartTyping keeps the platform typing indicator alive for one thread. An
// empty thread id means the caller has no thread to address; LobeHub sends one
// on every real run, so it is skipped rather than guessed.
func (r *registry) StartTyping(connectionID string, platformThreadID string) error {
	conn, ok := r.lookup(connectionID)
	if !ok {
		return ErrConnectionNotFound
	}
	if platformThreadID == "" {
		return nil
	}

	instance := conn.provider
	conn.typing.Start(platformThreadID, func(ctx context.Context) error {
		return instance.SetTyping(ctx, platformThreadID, true)
	})
	return nil
}

// StopTyping ends the indicator and tells the platform it is over.
func (r *registry) StopTyping(ctx context.Context, connectionID string, platformThreadID string) error {
	conn, ok := r.lookup(connectionID)
	if !ok {
		return ErrConnectionNotFound
	}

	conn.typing.Stop(platformThreadID)
	if platformThreadID == "" {
		return nil
	}
	return conn.provider.SetTyping(ctx, platformThreadID, false)
}

func (r *registry) lookup(connectionID string) (*heldConnection, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn, ok := r.conns[connectionID]
	return conn, ok
}

// Status answers `GET /api/connections/{id}/status`.
func (r *registry) Status(connectionID string) (ConnectionStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn, ok := r.conns[connectionID]
	if !ok {
		return ConnectionStatus{}, false
	}
	return ConnectionStatus{
		Config: &ConnectionStatusConfig{
			ConnectionID: conn.cfg.ConnectionID,
			Platform:     conn.cfg.Platform,
		},
		State: stateSnapshot(conn),
	}, true
}

// Stats answers `GET /api/admin/stats`.
func (r *registry) Stats() GatewayStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := GatewayStats{
		ByPlatform:  map[string]int{},
		Connections: make([]GatewayStatsListItem, 0, len(r.conns)),
		Total:       len(r.conns),
	}
	for _, conn := range r.conns {
		stats.ByPlatform[conn.cfg.Platform]++
		stats.Connections = append(stats.Connections, GatewayStatsListItem{
			ConnectionID: conn.cfg.ConnectionID,
			Platform:     conn.cfg.Platform,
			State:        stateSnapshot(conn),
			UserID:       conn.cfg.UserID,
		})
	}
	sort.Slice(stats.Connections, func(i, j int) bool {
		return stats.Connections[i].ConnectionID < stats.Connections[j].ConnectionID
	})
	return stats
}

// RegisteredIDs answers `GET /api/admin/registered-ids`: every id this gateway
// holds, including connections sitting in an error state. LobeHub uses it as
// the authoritative existence set for its reconciliation.
func (r *registry) RegisteredIDs() []string {
	r.mu.Lock()
	ids := make([]string, 0, len(r.conns))
	for id := range r.conns {
		ids = append(ids, id)
	}
	r.mu.Unlock()

	sort.Strings(ids)
	return ids
}

// Desired summarises the held set for logs and the pull path.
func (r *registry) Desired() map[string]provider.Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	desired := make(map[string]provider.Config, len(r.conns))
	for id, conn := range r.conns {
		desired[id] = conn.cfg
	}
	return desired
}

// Shutdown stops everything without reporting: LobeHub learns about the gap
// from its own reconcile once this gateway is back or gone.
func (r *registry) Shutdown(ctx context.Context) {
	r.DisconnectAll(ctx)
}

// applyState records a state change and, when it is news, publishes it. The
// first transition out of `connecting` is what LobeHub's channel UI shows, so
// unchanged repeats are suppressed.
func (r *registry) applyState(conn *heldConnection, change provider.StateChange) {
	r.mu.Lock()
	conn.state = change
	if change.State == provider.StateConnected {
		conn.connectedAt = time.Now()
	}
	shouldReport := change != conn.reported
	if shouldReport {
		conn.reported = change
	}
	platform := conn.cfg.Platform
	applicationID := conn.cfg.ApplicationID
	connectionID := conn.cfg.ConnectionID
	r.mu.Unlock()

	// LobeHub ignores callbacks without an applicationId, so a connection that
	// carries none has no status surface to update.
	if !shouldReport || applicationID == "" {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), lobeRequestTimeout)
		defer cancel()

		err := r.lobe.report(ctx, StateCallback{
			ApplicationID: applicationID,
			ConnectionID:  connectionID,
			Platform:      platform,
			State: StateCallbackStatus{
				Error:     change.Message,
				ErrorCode: change.Code,
				Status:    change.State,
			},
		})
		if err != nil {
			r.log.Warn("state callback failed",
				"connection_id", connectionID, "platform", platform, "state", change.State, "error", err)
		}
	}()
}

func stateSnapshot(conn *heldConnection) ConnectionStatusState {
	snapshot := ConnectionStatusState{
		Error:     conn.state.Message,
		ErrorCode: conn.state.Code,
		Platform:  conn.cfg.Platform,
		Status:    conn.state.State,
	}
	if !conn.connectedAt.IsZero() {
		snapshot.ConnectedAt = conn.connectedAt.UnixMilli()
	}
	return snapshot
}
