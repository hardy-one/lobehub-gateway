// Package reconcile converges LobeHub's stored SystemBot providers onto the
// platforms declared in oss-center's environment, and registers the
// platform-level gateway connections those platforms need.
//
// The direction is one-way and non-destructive: declarations are upserted, and
// anything LobeHub stores that nobody declares is reported and left alone. A
// typo in an environment variable must never be able to take a platform
// offline, so deleting is a deliberate operator action rather than a
// consequence of absence.
package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/oss-center-go/internal/config"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/lobehub"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/messagegw"
)

// errNoGateway means a WebSocket singleton was declared with nowhere to
// register it — a configuration the config package already rejects at startup.
var errNoGateway = errors.New("no message gateway configured")

// maxInterval caps the retry backoff so a long outage still recovers promptly
// once the endpoints return.
const maxInterval = 10 * time.Minute

// systemUserID is recorded on the platform-level connection. The singleton
// belongs to the deployment rather than to any one user — the gateway keeps the
// field for bookkeeping and never shards on it for a singleton.
const systemUserID = "system"

// Store is the slice of LobeHub this package needs.
type Store interface {
	UpsertPlatform(ctx context.Context, platform lobehub.Platform) error
	ListPlatforms(ctx context.Context) ([]lobehub.Summary, error)
}

// Gateway is the slice of the message gateway this package needs.
type Gateway interface {
	Ensure(ctx context.Context, config messagegw.ConnectionConfig) error
	Disconnect(ctx context.Context, connectionID string) error
	Status(ctx context.Context, connectionID string) (messagegw.ConnectionStatus, error)
}

// PlatformResult is the outcome for one declared platform in one pass.
type PlatformResult struct {
	// Connection is the gateway connection this platform uses, and ConnectionState
	// its reported state. Empty for platforms LobeHub connects on its own.
	Connection      string `json:"connection,omitempty"`
	ConnectionState string `json:"connectionState,omitempty"`
	Enabled         bool   `json:"enabled"`
	Error           string `json:"error,omitempty"`
	ID              string `json:"id"`
}

// Result is a completed pass. It exists so /status can answer "is provisioning
// working?" without oss-center having a UI.
type Result struct {
	FinishedAt time.Time        `json:"finishedAt"`
	StartedAt  time.Time        `json:"startedAt"`
	Error      string           `json:"error,omitempty"`
	Platforms  []PlatformResult `json:"platforms"`
	// Undeclared names platforms LobeHub still stores that this deployment no
	// longer declares. Reported, never removed.
	Undeclared []string `json:"undeclared"`
}

// Reconciler owns the convergence loop and the last pass's outcome.
type Reconciler struct {
	gateway  Gateway
	interval time.Duration
	log      *slog.Logger
	mutex    sync.Mutex
	result   *Result
	store    Store
	// platforms is the declared set, held in the order the config listed them.
	platforms []config.Platform
}

func New(
	store Store,
	gateway Gateway,
	platforms []config.Platform,
	interval time.Duration,
	log *slog.Logger,
) *Reconciler {
	return &Reconciler{
		gateway:   gateway,
		interval:  interval,
		log:       log,
		platforms: platforms,
		store:     store,
	}
}

// Last returns the most recent pass, or nil when none has finished yet.
func (r *Reconciler) Last() *Result {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.result == nil {
		return nil
	}
	copied := *r.result
	return &copied
}

// RunOnce performs one full pass and records it. It never returns early on a
// failure: a platform that failed is reported next to the ones that did not, so
// one bad credential cannot hide the state of the rest.
func (r *Reconciler) RunOnce(ctx context.Context) Result {
	result := Result{
		Platforms:  make([]PlatformResult, 0, len(r.platforms)),
		StartedAt:  time.Now(),
		Undeclared: []string{},
	}

	for _, platform := range r.platforms {
		outcome := PlatformResult{Enabled: platform.Enabled, ID: platform.ID}

		err := r.store.UpsertPlatform(ctx, lobehub.Platform{
			ApplicationID: platform.ApplicationID,
			Credentials:   platform.Credentials,
			Enabled:       platform.Enabled,
			Platform:      platform.ID,
		})
		if err != nil {
			outcome.Error = err.Error()
			result.Error = "one or more platforms failed to provision"
			r.log.Error("provision failed", "platform", platform.ID, "error", err)
			result.Platforms = append(result.Platforms, outcome)
			continue
		}
		r.log.Info("provisioned", "platform", platform.ID, "enabled", platform.Enabled)

		// The connection is only brought in line once the row behind it is
		// right: a live socket whose platform LobeHub cannot route is worse than
		// no socket, because it looks healthy while dropping every message.
		if platform.Singleton {
			outcome.Connection = singletonConnectionID(platform.ID)
			if state, err := r.syncConnection(ctx, platform); err != nil {
				outcome.Error = err.Error()
				result.Error = "one or more platform connections failed"
				r.log.Error("connection sync failed", "platform", platform.ID, "error", err)
			} else {
				outcome.ConnectionState = state
			}
		}

		result.Platforms = append(result.Platforms, outcome)
	}

	// The listing is what makes "removed a declaration" visible. A failure here
	// does not undo the upserts above, so it is reported alongside them.
	if undeclared, err := r.undeclared(ctx); err != nil {
		result.Error = joinErrors(result.Error, "could not list stored platforms: "+err.Error())
		r.log.Error("list stored platforms failed", "error", err)
	} else {
		result.Undeclared = undeclared
		for _, platform := range undeclared {
			r.log.Warn(
				"platform is stored but no longer declared; oss-center never deletes it",
				"platform", platform,
			)
		}
	}

	result.FinishedAt = time.Now()

	r.mutex.Lock()
	r.result = &result
	r.mutex.Unlock()

	return result
}

// Loop runs a pass immediately, then keeps converging until ctx is cancelled.
// Failures back off instead of restarting the process: an unreachable LobeHub is
// a transient condition, and a crash loop would only bury it in restart noise.
func (r *Reconciler) Loop(ctx context.Context) {
	wait := r.interval
	for {
		result := r.RunOnce(ctx)
		if result.Error != "" {
			wait = min(wait*2, maxInterval)
			r.log.Warn("reconcile failed, backing off", "retryIn", wait.String())
		} else {
			wait = r.interval
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// syncConnection brings one platform-level connection in line and returns its
// reported state.
func (r *Reconciler) syncConnection(ctx context.Context, platform config.Platform) (string, error) {
	if r.gateway == nil {
		return "", errNoGateway
	}

	connectionID := singletonConnectionID(platform.ID)

	if !platform.Enabled {
		// A disabled platform must not keep a live socket: nothing in LobeHub
		// would route what it receives.
		if err := r.gateway.Disconnect(ctx, connectionID); err != nil {
			return "", err
		}
		r.log.Info("disconnected", "platform", platform.ID, "connection", connectionID)
		return "disconnected", nil
	}

	if err := r.gateway.Ensure(ctx, messagegw.ConnectionConfig{
		ApplicationID: platform.ApplicationID,
		// A Messenger connection never consumes passive channel monitoring: the
		// shared bot answers DMs and explicit mentions only. Same value LobeHub
		// sends for the connections it registers itself.
		Capabilities: messagegw.Capabilities{
			MessageMonitoring: messagegw.MessageMonitoring{Enabled: false},
		},
		ConnectionID:   connectionID,
		ConnectionMode: "websocket",
		Credentials:    platform.Credentials,
		Platform:       platform.ID,
		UserID:         systemUserID,
		WebhookPath:    messengerWebhookPath(platform.ID),
	}); err != nil {
		return "", err
	}

	// Confirm rather than assume: `Ensure` is a no-op on an identical live
	// connection, so the status read is what proves the socket is actually up
	// after a gateway restart.
	status, err := r.gateway.Status(ctx, connectionID)
	if err != nil {
		return "", err
	}
	if status.State.Status != "connected" {
		r.log.Warn(
			"connection is not connected",
			"platform", platform.ID,
			"state", status.State.Status,
			"error", status.State.Error,
		)
	}
	return status.State.Status, nil
}

func (r *Reconciler) undeclared(ctx context.Context) ([]string, error) {
	stored, err := r.store.ListPlatforms(ctx)
	if err != nil {
		return nil, err
	}

	declared := make(map[string]struct{}, len(r.platforms))
	for _, platform := range r.platforms {
		declared[platform.ID] = struct{}{}
	}

	var undeclared []string
	for _, summary := range stored {
		if _, ok := declared[summary.Platform]; !ok {
			undeclared = append(undeclared, summary.Platform)
		}
	}
	sort.Strings(undeclared)
	return undeclared, nil
}

// singletonConnectionID mirrors LobeHub's `messengerConnectionId`: a
// platform-wide WebSocket lives at exactly this id, and a user's typing
// indicator is routed to it by name. Anywhere else and typing would target a
// connection that does not exist.
func singletonConnectionID(platform string) string {
	return "messenger:" + platform + ":singleton"
}

// messengerWebhookPath mirrors LobeHub's messenger webhook route — where the
// gateway forwards the inbound events it holds the socket for.
func messengerWebhookPath(platform string) string {
	return "/api/agent/messenger/webhooks/" + platform
}

func joinErrors(first, second string) string {
	if first == "" {
		return second
	}
	return first + "; " + second
}
