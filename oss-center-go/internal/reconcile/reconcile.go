// Package reconcile converges LobeHub's stored SystemBot providers onto the
// platforms declared in oss-center's environment.
//
// The direction is one-way and non-destructive: declarations are upserted,
// things LobeHub stores that nobody declares are reported and left alone. A
// typo in an environment variable must never be able to take a platform
// offline, so deleting is a deliberate operator action rather than a
// consequence of absence.
package reconcile

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/oss-center-go/internal/config"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/lobehub"
)

// maxInterval caps the retry backoff so a long outage still recovers promptly
// once the endpoint returns.
const maxInterval = 10 * time.Minute

// Store is the slice of LobeHub this package needs.
type Store interface {
	UpsertPlatform(ctx context.Context, platform lobehub.Platform) error
	ListPlatforms(ctx context.Context) ([]lobehub.Summary, error)
}

// PlatformResult is the outcome for one declared platform in one pass.
type PlatformResult struct {
	Enabled bool   `json:"enabled"`
	Error   string `json:"error,omitempty"`
	ID      string `json:"id"`
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
	interval time.Duration
	log      *slog.Logger
	mutex    sync.Mutex
	result   *Result
	store    Store
	// platforms is the declared set, held in the order the config listed them.
	platforms []config.Platform
}

func New(store Store, platforms []config.Platform, interval time.Duration, log *slog.Logger) *Reconciler {
	return &Reconciler{interval: interval, log: log, platforms: platforms, store: store}
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
		} else {
			r.log.Info("provisioned", "platform", platform.ID, "enabled", platform.Enabled)
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

func joinErrors(first, second string) string {
	if first == "" {
		return second
	}
	return first + "; " + second
}
