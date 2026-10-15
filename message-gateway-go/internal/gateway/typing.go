package gateway

import (
	"context"
	"sync"
	"time"
)

// typingScheduler keeps a platform typing indicator alive for as long as
// LobeHub is generating a reply. Platform indicators expire on their own
// (Discord after ~10s, Telegram after ~5s) while a generation can run for
// minutes, so the indicator is re-issued on an interval until it is stopped.
type typingScheduler struct {
	interval time.Duration

	mu    sync.Mutex
	loops map[string]*typingLoop
}

type typingLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func newTypingScheduler(interval time.Duration) *typingScheduler {
	if interval <= 0 {
		interval = defaultTypingInterval
	}
	return &typingScheduler{interval: interval, loops: map[string]*typingLoop{}}
}

// Start begins (or restarts) the indicator for one thread. The first tick is
// immediate, so the user sees the indicator without waiting an interval.
func (t *typingScheduler) Start(key string, send func(ctx context.Context) error) {
	t.Stop(key)

	ctx, cancel := context.WithCancel(context.Background())
	loop := &typingLoop{cancel: cancel, done: make(chan struct{})}

	t.mu.Lock()
	t.loops[key] = loop
	t.mu.Unlock()

	go func() {
		defer close(loop.done)
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()

		if err := send(ctx); err != nil {
			// A failed first tick usually means the indicator is unavailable
			// (missing thread context, revoked token). Keep trying on the
			// interval: the indicator is cosmetic and must never be fatal.
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := send(ctx); err != nil {
					return
				}
			}
		}
	}()
}

// Stop ends the indicator for one thread and waits for its loop to exit.
func (t *typingScheduler) Stop(key string) {
	t.mu.Lock()
	loop := t.loops[key]
	delete(t.loops, key)
	t.mu.Unlock()

	if loop == nil {
		return
	}
	loop.cancel()
	<-loop.done
}

// StopAll ends every indicator; used when a connection goes away.
func (t *typingScheduler) StopAll() {
	t.mu.Lock()
	loops := make([]*typingLoop, 0, len(t.loops))
	for key, loop := range t.loops {
		loops = append(loops, loop)
		delete(t.loops, key)
	}
	t.mu.Unlock()

	for _, loop := range loops {
		loop.cancel()
		<-loop.done
	}
}
