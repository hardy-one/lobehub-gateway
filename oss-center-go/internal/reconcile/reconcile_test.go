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

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunOnceProvisionsEveryDeclaredPlatform(t *testing.T) {
	store := &fakeStore{}
	reconciler := New(store, []config.Platform{
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
		t.Fatalf("disabled platform must still be written, as disabled: %+v", store.upserted[1])
	}
}

// Absence is not deletion: a stored platform nobody declares is reported and
// left exactly where it is.
func TestRunOnceReportsUndeclaredWithoutDeleting(t *testing.T) {
	store := &fakeStore{stored: []lobehub.Summary{
		{Platform: "slack"},
		{Platform: "telegram"},
	}}
	reconciler := New(store, []config.Platform{{ID: "telegram"}}, time.Minute, quietLogger())

	result := reconciler.RunOnce(context.Background())

	if len(result.Undeclared) != 1 || result.Undeclared[0] != "slack" {
		t.Fatalf("expected slack to be reported as undeclared, got %v", result.Undeclared)
	}
}

// One failing platform must not hide the others.
func TestRunOnceKeepsGoingAfterAFailure(t *testing.T) {
	store := &fakeStore{upsertErr: map[string]error{"discord": errors.New("boom")}}
	reconciler := New(store, []config.Platform{
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
	reconciler := New(&fakeStore{}, nil, time.Minute, quietLogger())

	if reconciler.Last() != nil {
		t.Fatal("a reconciler that never ran has no result to report")
	}

	reconciler.RunOnce(context.Background())

	if reconciler.Last() == nil {
		t.Fatal("expected the completed pass to be readable")
	}
}

func TestLoopStopsWithContext(t *testing.T) {
	reconciler := New(&fakeStore{}, []config.Platform{{ID: "telegram"}}, time.Millisecond, quietLogger())
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
