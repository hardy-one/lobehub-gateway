// Package osscenter provisions the deployment-level SystemBot providers behind
// LobeHub's Messenger, from this deployment's own environment.
//
// It replaces the parts of the closed-source admin console a self-hosted
// deployment needs: credentials, the enable switch, and the platform-level
// gateway connection. It has no UI and no database access — the declared state
// is the environment, and LobeHub does the storing and the encrypting.
package osscenter

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/lobehub/lobehub/apps/oss-center-go/internal/config"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/lobehub"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/messagegw"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/reconcile"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/server"
)

type Config = config.Config

func ConfigFromEnv() Config { return config.ConfigFromEnv() }

// Component wires the reconciler to its status surface for the unified binary.
type Component struct {
	cancel     context.CancelFunc
	done       chan struct{}
	log        *slog.Logger
	reconciler *reconcile.Reconciler
	server     *server.Server
}

func NewComponent(cfg Config, log *slog.Logger) *Component {
	if log == nil {
		log = slog.Default()
	}
	reconciler := reconcile.New(
		lobehub.New(cfg.LobeHubBaseURL, cfg.ServiceToken),
		platformConnections(cfg),
		cfg.Platforms,
		cfg.Interval,
		log,
	)
	return &Component{
		done:       make(chan struct{}),
		log:        log,
		reconciler: reconciler,
		server:     server.New(reconciler, cfg.DeclaredPlatforms()),
	}
}

// Start runs the convergence loop in the background. Oneshot performs exactly
// one pass — enough to apply a configuration change without leaving a loop
// running, and the shape a cron-driven deployment wraps.
func (c *Component) Start(oneshot bool) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	go func() {
		defer close(c.done)
		if oneshot {
			c.reconciler.RunOnce(ctx)
			return
		}
		c.reconciler.Loop(ctx)
	}()
}

func (c *Component) Routes() http.Handler { return c.server.Routes() }

// platformConnections builds the gateway client only when a declared platform
// actually needs one: a deployment running webhook platforms talks to LobeHub
// and the platform APIs alone, and never depends on the gateway.
func platformConnections(cfg Config) reconcile.Gateway {
	if cfg.MessageGatewayURL == "" {
		return nil
	}
	return messagegw.New(cfg.MessageGatewayURL, cfg.ServiceToken)
}

// Shutdown stops the loop and waits for the in-flight pass to finish, so a
// pass cannot be cut off halfway through writing a platform's state.
func (c *Component) Shutdown(ctx context.Context) {
	if c.cancel == nil {
		return
	}
	c.cancel()
	select {
	case <-c.done:
	case <-ctx.Done():
		c.log.Warn("oss center shutdown timed out waiting for the reconcile pass")
	}
}
