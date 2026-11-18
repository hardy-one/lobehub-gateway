package gateway

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultStateDBPath      = "/data/agent-gateway.db"
	stateStoreFlushInterval = 200 * time.Millisecond
	stateStoreFlushBatch    = 50
	stateStoreCloseTimeout  = 2 * time.Second
	stateOrphanTTL          = 24 * time.Hour
)

type stateRecord struct {
	Status       SessionStatus
	EventCounter int
	UserID       string
	CreatedAt    time.Time
}

type operationStateStore struct {
	db     *sql.DB
	logger *slog.Logger

	closeOnce   sync.Once
	closed      chan struct{}
	closing     chan struct{}
	cleanupDone chan struct{}
	flushMu     sync.Mutex
	pending     map[string]int
	flushTimer  *time.Timer
}

func newOperationStateStore(path string, logger *slog.Logger) *operationStateStore {
	return newOperationStateStoreWithPath(path, logger)
}

func newOperationStateStoreWithPath(path string, logger *slog.Logger) *operationStateStore {
	started := time.Now()
	db, err := openOperationStateDB(path)
	if err != nil {
		logger.Warn("state store degraded to in-memory mode",
			"path", path,
			"reason", err.Error(),
			"error", err,
		)
		return nil
	}
	logger.Info("state store enabled",
		"path", path,
		"openDurationMs", time.Since(started).Milliseconds(),
	)
	store := &operationStateStore{
		db:          db,
		logger:      logger,
		pending:     map[string]int{},
		closed:      make(chan struct{}),
		closing:     make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}
	go store.cleanupLoop()
	return store
}

// Close stops background work, flushes every pending counter and closes the
// database. It is safe to call more than once and returns within
// stateStoreCloseTimeout even when a write is stuck.
func (s *operationStateStore) Close() error {
	flushed := 0
	s.closeOnce.Do(func() {
		close(s.closing)
		<-s.cleanupDone
		if s.db != nil {
			s.flushMu.Lock()
			flushed = len(s.pending)
			s.flushMu.Unlock()
			done := make(chan struct{})
			go func() {
				s.flushCounters(nil)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(stateStoreCloseTimeout):
				s.logger.Warn("operation state counter flush timed out during close")
			}
			closeErr := s.db.Close()
			s.logger.Info("state store closed",
				"flushedCounters", flushed,
				"databaseClosed", closeErr == nil,
				"error", closeErr,
			)
		}
		close(s.closed)
	})
	<-s.closed
	return nil
}

func openOperationStateDB(path string) (*sql.DB, error) {
	if dir := filepath.Dir(path); dir != "." {
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			return nil, fmt.Errorf("state database directory unavailable: %s", dir)
		}
	}
	// A zero-length file is not a valid database: SQLite would silently treat
	// it as an empty database and re-create the schema in place, losing prior
	// terminal state without any warning. Treat truncation as corruption and
	// degrade to memory mode (LOBE-186 defect 1).
	if info, err := os.Stat(path); err == nil && info.Size() == 0 {
		return nil, fmt.Errorf("state database file is empty (possibly truncated): %s", path)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := initOperationStateDB(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func initOperationStateDB(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return err
	}
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS operation_state (
  id            TEXT PRIMARY KEY,
  user_id       TEXT NOT NULL,
  status        TEXT NOT NULL,
  event_counter INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
)`)
	return err
}

func (s *operationStateStore) load(operationID string) (stateRecord, bool) {
	var record stateRecord
	var status string
	var createdAt int64
	err := s.db.QueryRow(`SELECT user_id, status, event_counter, created_at FROM operation_state WHERE id = ?`, operationID).
		Scan(&record.UserID, &status, &record.EventCounter, &createdAt)
	if err != nil {
		return stateRecord{}, false
	}
	record.Status = SessionStatus(status)
	record.CreatedAt = time.UnixMilli(createdAt)
	return record, true
}

func (s *operationStateStore) save(record operationStateSnapshot) {
	_, err := s.db.Exec(`INSERT INTO operation_state (id, user_id, status, event_counter, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET user_id = excluded.user_id, status = excluded.status,
  event_counter = excluded.event_counter, updated_at = excluded.updated_at`,
		record.OperationID, record.UserID, string(record.Status), record.EventCounter,
		record.CreatedAt.UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		s.logger.Warn("failed to persist operation state", "operationId", record.OperationID, "error", err)
	}
}

func (s *operationStateStore) queueCounter(operationID string, counter int) {
	s.flushMu.Lock()
	previous := s.pending[operationID]
	if counter > previous {
		s.pending[operationID] = counter
	}
	batch := len(s.pending)
	flush := batch >= stateStoreFlushBatch
	if flush {
		s.takePendingLocked()
	}
	s.flushMu.Unlock()
	if flush {
		s.flushCounters(nil)
		return
	}
	s.flushMu.Lock()
	if s.flushTimer == nil {
		s.flushTimer = time.AfterFunc(stateStoreFlushInterval, func() { s.flushCounters(nil) })
	}
	s.flushMu.Unlock()
}

func (s *operationStateStore) flushCounter(operationID string, counter int) {
	s.flushMu.Lock()
	delete(s.pending, operationID)
	s.flushMu.Unlock()
	_, err := s.db.Exec(`UPDATE operation_state SET event_counter = ?, updated_at = ? WHERE id = ?`,
		counter, time.Now().UnixMilli(), operationID)
	if err != nil {
		s.logger.Warn("failed to persist event counter", "operationId", operationID, "error", err)
	}
}

func (s *operationStateStore) flushCounters(done chan<- struct{}) {
	s.flushMu.Lock()
	pending := s.takePendingLocked()
	if s.flushTimer != nil {
		s.flushTimer.Stop()
		s.flushTimer = nil
	}
	s.flushMu.Unlock()
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	for operationID, counter := range pending {
		_, err := s.db.Exec(`UPDATE operation_state SET event_counter = ?, updated_at = ? WHERE id = ?`,
			counter, time.Now().UnixMilli(), operationID)
		if err != nil {
			s.logger.Warn("failed to persist event counter", "operationId", operationID, "error", err)
		}
	}
}

func (s *operationStateStore) takePendingLocked() map[string]int {
	pending := s.pending
	s.pending = map[string]int{}
	return pending
}

func (s *operationStateStore) delete(operationID string) {
	if _, err := s.db.Exec(`DELETE FROM operation_state WHERE id = ?`, operationID); err != nil {
		s.logger.Warn("failed to delete operation state", "operationId", operationID, "error", err)
	}
}

// remove deletes a persisted operation row and logs the cleanup at debug
// level. It is called only when the operation has already been removed from
// memory by the caller.
func (s *operationStateStore) remove(operationID string) {
	s.logger.Debug("operation state removed", "operationId", operationID)
	s.delete(operationID)
}

func (s *operationStateStore) cleanupLoop() {
	defer close(s.cleanupDone)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	s.cleanupOrphans()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.cleanupOrphans()
		}
	}
}

func (s *operationStateStore) cleanupOrphans() {
	cutoff := time.Now().Add(-stateOrphanTTL).UnixMilli()
	if _, err := s.db.Exec(`DELETE FROM operation_state WHERE updated_at < ?`, cutoff); err != nil {
		s.logger.Warn("failed to clean up orphaned operation state", "error", err)
	}
}

func (s *operationStateStore) flush() {
	s.flushCounters(nil)
}
