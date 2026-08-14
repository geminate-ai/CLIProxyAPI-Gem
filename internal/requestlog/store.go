package requestlog

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	log "github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
)

const (
	defaultQueueSize = 1024
	maxListLimit     = 200
	defaultListLimit = 50
	cleanupBatchSize = 1000
)

var (
	bearerPattern = regexp.MustCompile(`(?i)bearer\s+[a-z0-9._~+/=-]+`)
	jwtPattern    = regexp.MustCompile(`\beyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\b`)
	queryPattern  = regexp.MustCompile(`(?i)([?&](?:api[_-]?key|token|key|secret|authorization)=)[^&\s]+`)
)

// Store is a SQLite-backed, fail-open event writer and query repository.
type Store struct {
	db       *sql.DB
	path     string
	opts     Options
	queue    chan Event
	done     chan struct{}
	closeOne sync.Once
	writerWG sync.WaitGroup
	dropped  atomic.Uint64

	lastErrorMu sync.RWMutex
	lastError   string
}

var (
	_ Repository               = (*Store)(nil)
	_ logging.RequestEventSink = (*Store)(nil)
)

// Open creates a writable SQLite request-log store and starts its asynchronous writer.
func Open(ctx context.Context, opts Options) (*Store, error) {
	if strings.TrimSpace(opts.Path) == "" || !filepath.IsAbs(opts.Path) {
		return nil, fmt.Errorf("request-log path must be absolute")
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = defaultQueueSize
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Hour
	}
	if opts.BusyTimeout < 0 {
		return nil, fmt.Errorf("request-log busy timeout must be zero or greater")
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o700); err != nil {
		return nil, fmt.Errorf("create request-log directory: %w", err)
	}
	db, err := sql.Open("sqlite", opts.Path)
	if err != nil {
		return nil, fmt.Errorf("open request-log database: %w", err)
	}
	closeDB := true
	defer func() {
		if closeDB {
			if errClose := db.Close(); errClose != nil {
				log.WithError(errClose).Warn("close failed request-log database")
			}
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err = db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("enable request-log WAL: %w", err)
	}
	if _, err = db.ExecContext(ctx, `PRAGMA foreign_keys=ON`); err != nil {
		return nil, fmt.Errorf("enable request-log foreign keys: %w", err)
	}
	if _, err = db.ExecContext(ctx, fmt.Sprintf(`PRAGMA busy_timeout=%d`, opts.BusyTimeout.Milliseconds())); err != nil {
		return nil, fmt.Errorf("set request-log busy timeout: %w", err)
	}
	if err = migrate(ctx, db); err != nil {
		return nil, err
	}
	if err = os.Chmod(opts.Path, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("secure request-log database permissions: %w", err)
	}
	store := &Store{db: db, path: opts.Path, opts: opts, queue: make(chan Event, opts.QueueSize), done: make(chan struct{})}
	store.writerWG.Add(1)
	go store.runWriter()
	closeDB = false
	return store, nil
}

// Capture queues a sanitized event without blocking the proxy request path.
func (s *Store) Capture(event Event) {
	if s == nil {
		return
	}
	event = s.sanitize(event)
	select {
	case <-s.done:
		s.dropped.Add(1)
	case s.queue <- event:
	default:
		s.dropped.Add(1)
	}
}

// CaptureRequestEvent adapts the shared request lifecycle contract to durable storage.
// It is non-blocking and therefore safe for middleware to call on the request path.
func (s *Store) CaptureRequestEvent(event logging.RequestEvent) {
	s.Capture(Event{
		ID:               event.ID,
		RequestID:        event.RequestID,
		StartedAt:        event.StartedAt,
		CompletedAt:      event.CompletedAt,
		DurationMS:       event.DurationMS,
		Method:           event.Method,
		Route:            event.Route,
		Provider:         event.Provider,
		ModelRequested:   event.ModelRequested,
		ModelResolved:    event.ModelResolved,
		AccountAlias:     event.AccountAlias,
		APIKeyAlias:      event.APIKeyAlias,
		StatusCode:       event.StatusCode,
		Outcome:          event.Outcome,
		InputTokens:      event.InputTokens,
		OutputTokens:     event.OutputTokens,
		CachedTokens:     event.CachedTokens,
		EstimatedCostUSD: event.EstimatedCostUSD,
		RetryCount:       event.RetryCount,
		ErrorClass:       event.ErrorClass,
		ErrorMessage:     event.ErrorMessage,
		Metadata:         event.Metadata,
	})
}

// Close flushes queued events and closes the database.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOne.Do(func() {
		close(s.done)
		s.writerWG.Wait()
		closeErr = s.db.Close()
	})
	return closeErr
}

func (s *Store) runWriter() {
	defer s.writerWG.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	cleanupTicker := time.NewTicker(s.opts.CleanupInterval)
	defer ticker.Stop()
	defer cleanupTicker.Stop()
	batch := make([]Event, 0, 100)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.insertBatch(context.Background(), batch); err != nil {
			s.recordError(err)
			s.dropped.Add(uint64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case event := <-s.queue:
			batch = append(batch, event)
			if len(batch) >= cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-cleanupTicker.C:
			flush()
			if err := s.cleanup(context.Background()); err != nil {
				s.recordError(err)
			}
		case <-s.done:
			for {
				select {
				case event := <-s.queue:
					batch = append(batch, event)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (s *Store) insertBatch(ctx context.Context, events []Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin request-log insert: %w", err)
	}
	defer func() {
		if errRollback := tx.Rollback(); errRollback != nil && !errors.Is(errRollback, sql.ErrTxDone) {
			log.WithError(errRollback).Warn("rollback failed for request-log insert")
		}
	}()
	statement, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO request_events (
id, request_id, started_at, completed_at, duration_ms, method, route, provider,
model_requested, model_resolved, account_alias, api_key_alias, status_code, outcome,
input_tokens, output_tokens, cached_tokens, estimated_cost_usd, retry_count, error_class,
error_message, metadata_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare request-log insert: %w", err)
	}
	defer func() {
		if errClose := statement.Close(); errClose != nil {
			log.WithError(errClose).Warn("close failed request-log statement")
		}
	}()
	for _, event := range events {
		metadata, errMarshal := json.Marshal(event.Metadata)
		if errMarshal != nil {
			return fmt.Errorf("marshal request-log metadata: %w", errMarshal)
		}
		if _, errExec := statement.ExecContext(ctx,
			event.ID, event.RequestID, unixMilliseconds(event.StartedAt), unixMilliseconds(event.CompletedAt), event.DurationMS,
			event.Method, event.Route, event.Provider, event.ModelRequested, event.ModelResolved, event.AccountAlias,
			event.APIKeyAlias, event.StatusCode, event.Outcome, event.InputTokens, event.OutputTokens, event.CachedTokens,
			event.EstimatedCostUSD, event.RetryCount, event.ErrorClass, event.ErrorMessage, string(metadata),
		); errExec != nil {
			return fmt.Errorf("insert request-log event: %w", errExec)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit request-log insert: %w", err)
	}
	return nil
}

func (s *Store) sanitize(event Event) Event {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.StartedAt.IsZero() {
		event.StartedAt = time.Now().UTC()
	}
	if event.CompletedAt.IsZero() {
		event.CompletedAt = event.StartedAt
	}
	if event.DurationMS < 0 {
		event.DurationMS = 0
	}
	event.Method = limit(strings.ToUpper(strings.TrimSpace(event.Method)), 16)
	event.Route = limit(stripQuery(strings.TrimSpace(event.Route)), 512)
	event.Provider = limit(strings.TrimSpace(event.Provider), 128)
	event.ModelRequested = limit(strings.TrimSpace(event.ModelRequested), 256)
	event.ModelResolved = limit(strings.TrimSpace(event.ModelResolved), 256)
	event.AccountAlias = limit(strings.TrimSpace(event.AccountAlias), 256)
	event.APIKeyAlias = limit(strings.TrimSpace(event.APIKeyAlias), 256)
	event.Outcome = limit(strings.TrimSpace(event.Outcome), 64)
	event.ErrorClass = limit(strings.TrimSpace(event.ErrorClass), 128)
	event.ErrorMessage = limit(strings.TrimSpace(event.ErrorMessage), 1024)
	if s.opts.RedactErrors {
		event.ErrorMessage = redact(event.ErrorMessage)
	}
	event.Metadata = sanitizeMetadata(event.Metadata)
	return event
}

func (s *Store) cleanup(ctx context.Context) error {
	if s.opts.RetentionDays > 0 {
		cutoff := time.Now().AddDate(0, 0, -s.opts.RetentionDays).UnixMilli()
		for {
			result, err := s.db.ExecContext(ctx, `DELETE FROM request_events WHERE id IN (SELECT id FROM request_events WHERE started_at < ? ORDER BY started_at ASC LIMIT ?)`, cutoff, cleanupBatchSize)
			if err != nil {
				return fmt.Errorf("apply request-log retention: %w", err)
			}
			deleted, errRows := result.RowsAffected()
			if errRows != nil || deleted < cleanupBatchSize {
				break
			}
		}
	}
	if s.opts.MaxDatabaseMB > 0 {
		maxBytes := int64(s.opts.MaxDatabaseMB) * 1024 * 1024
		for size := s.databaseSize(); size > maxBytes; size = s.databaseSize() {
			result, err := s.db.ExecContext(ctx, `DELETE FROM request_events WHERE id IN (SELECT id FROM request_events ORDER BY started_at ASC LIMIT ?)`, cleanupBatchSize)
			if err != nil {
				return fmt.Errorf("enforce request-log database size: %w", err)
			}
			deleted, errRows := result.RowsAffected()
			if errRows != nil || deleted == 0 {
				break
			}
		}
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		return fmt.Errorf("checkpoint request-log WAL: %w", err)
	}
	return nil
}

func (s *Store) recordError(err error) {
	if err == nil {
		return
	}
	s.lastErrorMu.Lock()
	s.lastError = limit(redact(err.Error()), 512)
	s.lastErrorMu.Unlock()
	log.WithError(err).Warn("request-log storage operation failed; proxy request continues")
}

func (s *Store) databaseSize() int64 {
	info, err := os.Stat(s.path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func unixMilliseconds(value time.Time) int64 { return value.UTC().UnixMilli() }

func limit(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func stripQuery(value string) string {
	if idx := strings.IndexByte(value, '?'); idx >= 0 {
		return value[:idx]
	}
	return value
}

func redact(value string) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	value = jwtPattern.ReplaceAllString(value, "[REDACTED_JWT]")
	return queryPattern.ReplaceAllString(value, "$1[REDACTED]")
}

func sanitizeMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	allowed := map[string]struct{}{"stream": {}, "cache_status": {}, "client_name": {}, "reason": {}}
	sanitized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if _, ok := allowed[key]; ok {
			sanitized[key] = limit(redact(strings.TrimSpace(value)), 256)
		}
	}
	if len(sanitized) == 0 {
		return nil
	}
	return sanitized
}

type cursor struct {
	StartedAt int64  `json:"startedAt"`
	ID        string `json:"id"`
}

func encodeCursor(startedAt int64, id string) string {
	data, _ := json.Marshal(cursor{StartedAt: startedAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(value string) (cursor, error) {
	if value == "" {
		return cursor{}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor{}, fmt.Errorf("invalid request-log cursor")
	}
	var result cursor
	if err = json.Unmarshal(data, &result); err != nil || result.ID == "" {
		return cursor{}, fmt.Errorf("invalid request-log cursor")
	}
	return result, nil
}

func percentile(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)-1) * percentile)
	return values[index]
}
