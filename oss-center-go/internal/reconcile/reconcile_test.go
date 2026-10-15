package reconcile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lobehub/lobehub/apps/oss-center-go/internal/config"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/lobehub"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/messagegw"
)

type fakeStore struct {
	listErr    error
	stored     []lobehub.Summary
	upsertErr  map[string]error
	upserted   []lobehub.Platform
	upsertLock sync.Mutex
}

func (f *fakeStore) UpsertPlatform(_ context.Context, platform lobehub.Platform) error {
	f.upsertLock.Lock()
	defer f.upsertLock.Unlock()
	f.upserted = append(f.upserted, platform)
	return f.upsertErr[platform.Platform]
}

func (f *fakeStore) ListPlatforms(context.Context) ([]lobehub.Summary, error) {
	return f.stored, f.listErr
}

type fakeGateway struct {
	disconnectErr error
	disconnected  []string
	ensureErr     error
	ensured       []messagegw.ConnectionConfig
	state         string
	statusErr     error
}

func (f *fakeGateway) Ensure(_ context.Context, cfg messagegw.ConnectionConfig) error {
	f.ensured = append(f.ensured, cfg)
	return f.ensureErr
}

func (f *fakeGateway) Disconnect(_ context.Context, connectionID string) error {
	f.disconnected = append(f.disconnected, connectionID)
	return f.disconnectErr
}

func (f *fakeGateway) Status(context.Context, string) (messagegw.ConnectionStatus, error) {
	var status messagegw.ConnectionStatus
	status.State.Status = f.state
	return status, f.statusErr
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func discord(platform config.Platform) config.Platform {
	platform.ID = "discord"
	platform.Singleton = true
	platform.Credentials = map[string]any{"botToken": "x"}
	return platform
}

func telegram(platform config.Platform) config.Platform {
	platform.ID = "telegram"
	platform.Credentials = map[string]any{"botToken": "y"}
	return platform
}

func TestRunOnceProvisionsEveryDeclaredPlatform(t *testing.T) {
	store := &fakeStore{}
	reconciler := New(store, nil, []config.Platform{
		{ApplicationID: "app", Credentials: map[string]any{"botToken": "x"}, Enabled: true, ID: "discord"},
		{Credentials: map[string]any{"botToken": "y"}, Enabled: false, ID: "telegram"},
	}, time.Minute, quietLogger())

	result := reconciler.RunOnce(context.Background())

	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	if len(store.upserted) != 2 {
		t.Fatalf("expected both platforms to be provisioned, got %d", len(store.upserted))
	}
	if store.upserted[1].Platform != "telegram" || store.upserted[1].Enabled {
		t.Fatalf("a disabled platform must still be written, as disabled: %+v", store.upserted[1])
	}
}

// Absence is not deletion: a stored platform nobody declares is reported and
// left exactly where it is.
func TestRunOnceReportsUndeclaredWithoutDeleting(t *testing.T) {
	store := &fakeStore{stored: []lobehub.Summary{
		{Platform: "slack"},
		{Platform: "telegram"},
	}}
	reconciler := New(store, nil, []config.Platform{{ID: "telegram"}}, time.Minute, quietLogger())

	result := reconciler.RunOnce(context.Background())

	if len(result.Undeclared) != 1 || result.Undeclared[0] != "slack" {
		t.Fatalf("expected slack to be reported as undeclared, got %v", result.Undeclared)
	}
}

// One failing platform must not hide the others.
func TestRunOnceKeepsGoingAfterAFailure(t *testing.T) {
	store := &fakeStore{upsertErr: map[string]error{"discord": errors.New("boom")}}
	reconciler := New(store, nil, []config.Platform{
		{ID: "discord"},
		{ID: "telegram"},
	}, time.Minute, quietLogger())

	result := reconciler.RunOnce(context.Background())

	if result.Error == "" {
		t.Fatal("expected the pass to report an error")
	}
	if len(result.Platforms) != 2 || result.Platforms[1].Error != "" {
		t.Fatalf("the second platform should still be provisioned: %+v", result.Platforms)
	}
	if result.Platforms[0].Error == "" {
		t.Fatalf("the failing platform should carry its error: %+v", result.Platforms[0])
	}
}

func TestRunOnceRecordsResultForStatus(t *testing.T) {
	reconciler := New(&fakeStore{}, nil, nil, time.Minute, quietLogger())

	if reconciler.Last() != nil {
		t.Fatal("a reconciler that never ran has no result to report")
	}

	reconciler.RunOnce(context.Background())

	if reconciler.Last() == nil {
		t.Fatal("expected the completed pass to be readable")
	}
}

func TestLoopStopsWithContext(t *testing.T) {
	reconciler := New(&fakeStore{}, nil, []config.Platform{{ID: "telegram"}}, time.Millisecond, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		reconciler.Loop(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop when its context was cancelled")
	}
	if reconciler.Last() == nil {
		t.Fatal("the loop should have completed at least one pass")
	}
}

// The singleton id and payload are a contract with LobeHub: registered anywhere
// else, a user's typing indicator targets a connection that does not exist.
func TestRunOnceRegistersThePlatformSingleton(t *testing.T) {
	gateway := &fakeGateway{state: "connected"}
	reconciler := New(
		&fakeStore{},
		gateway,
		[]config.Platform{discord(config.Platform{ApplicationID: "app-id", Enabled: true})},
		time.Minute,
		quietLogger(),
	)

	result := reconciler.RunOnce(context.Background())

	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	if len(gateway.ensured) != 1 {
		t.Fatalf("expected one connection to be ensured, got %d", len(gateway.ensured))
	}
	ensured := gateway.ensured[0]
	if ensured.ConnectionID != "messenger:discord:singleton" {
		t.Fatalf("unexpected connection id %q", ensured.ConnectionID)
	}
	if ensured.ConnectionMode != "websocket" {
		t.Fatalf("unexpected connection mode %q", ensured.ConnectionMode)
	}
	if ensured.WebhookPath != "/api/agent/messenger/webhooks/discord" {
		t.Fatalf("inbound events would go nowhere: %q", ensured.WebhookPath)
	}
	if ensured.Capabilities.MessageMonitoring.Enabled {
		t.Fatal("a Messenger connection never consumes passive monitoring")
	}
	if ensured.UserID != "system" {
		t.Fatalf("the singleton belongs to the deployment, not a user: %q", ensured.UserID)
	}
	if result.Platforms[0].ConnectionState != "connected" {
		t.Fatalf("the reported state should reach /status: %+v", result.Platforms[0])
	}
}

// A disabled platform must not keep a live socket nobody routes for.
func TestRunOnceDisconnectsADisabledPlatform(t *testing.T) {
	gateway := &fakeGateway{state: "connected"}
	reconciler := New(
		&fakeStore{},
		gateway,
		[]config.Platform{discord(config.Platform{Enabled: false})},
		time.Minute,
		quietLogger(),
	)

	result := reconciler.RunOnce(context.Background())

	if len(gateway.disconnected) != 1 || gateway.disconnected[0] != "messenger:discord:singleton" {
		t.Fatalf("expected the singleton to be dropped, got %v", gateway.disconnected)
	}
	if len(gateway.ensured) != 0 {
		t.Fatal("a disabled platform must not be connected")
	}
	if result.Platforms[0].ConnectionState != "disconnected" {
		t.Fatalf("unexpected state: %+v", result.Platforms[0])
	}
}

// Webhook platforms are LobeHub's to connect on demand.
func TestRunOnceLeavesWebhookPlatformsToLobeHub(t *testing.T) {
	gateway := &fakeGateway{}
	reconciler := New(
		&fakeStore{},
		gateway,
		[]config.Platform{telegram(config.Platform{Enabled: true})},
		time.Minute,
		quietLogger(),
	)

	result := reconciler.RunOnce(context.Background())

	if len(gateway.ensured) != 0 || len(gateway.disconnected) != 0 {
		t.Fatalf("telegram needs no gateway connection: %+v", gateway)
	}
	if result.Platforms[0].Connection != "" {
		t.Fatalf("no connection should be reported: %+v", result.Platforms[0])
	}
}

// A live socket whose platform LobeHub cannot route is worse than none, so the
// connection waits for the row to be right.
func TestRunOnceSkipsTheConnectionWhenTheRowFailed(t *testing.T) {
	store := &fakeStore{upsertErr: map[string]error{"discord": errors.New("boom")}}
	gateway := &fakeGateway{}
	reconciler := New(store, gateway, []config.Platform{discord(config.Platform{Enabled: true})}, time.Minute, quietLogger())

	reconciler.RunOnce(context.Background())

	if len(gateway.ensured) != 0 {
		t.Fatal("the connection must not be registered while the platform row is broken")
	}
}

func TestRunOnceReportsAGatewayFailure(t *testing.T) {
	gateway := &fakeGateway{ensureErr: errors.New("gateway unreachable")}
	reconciler := New(&fakeStore{}, gateway, []config.Platform{discord(config.Platform{Enabled: true})}, time.Minute, quietLogger())

	result := reconciler.RunOnce(context.Background())

	if result.Error == "" {
		t.Fatal("expected the pass to report the failure")
	}
	if result.Platforms[0].Error == "" {
		t.Fatalf("the platform should carry the gateway error: %+v", result.Platforms[0])
	}
}
