package requestlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const eventColumns = `id, request_id, started_at, completed_at, duration_ms, method, route, provider,
model_requested, model_resolved, account_alias, api_key_alias, status_code, outcome,
input_tokens, output_tokens, cached_tokens, estimated_cost_usd, retry_count, error_class,
error_message, metadata_json`

// List returns newest-first events using deterministic cursor pagination.
func (s *Store) List(ctx context.Context, filter ListFilter) (ListResult, error) {
	if s == nil {
		return ListResult{}, ErrDisabled
	}
	cursorValue, err := decodeCursor(filter.Cursor)
	if err != nil {
		return ListResult{}, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	where, args := listWhere(filter.From, filter.To, filter.Provider, filter.Model, filter.Account, filter.APIKey, filter.Status, filter.Outcome, filter.RequestID)
	if filter.Cursor != "" {
		where = append(where, `(started_at < ? OR (started_at = ? AND id < ?))`)
		args = append(args, cursorValue.StartedAt, cursorValue.StartedAt, cursorValue.ID)
	}
	args = append(args, limit+1)
	query := `SELECT ` + eventColumns + ` FROM request_events` + whereSQL(where) + ` ORDER BY started_at DESC, id DESC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list request-log events: %w", err)
	}
	defer func() {
		if errClose := rows.Close(); errClose != nil {
			log.WithError(errClose).Warn("close failed request-log list rows")
		}
	}()
	result := ListResult{Events: make([]Event, 0, limit)}
	for rows.Next() {
		event, errScan := scanEvent(rows)
		if errScan != nil {
			return ListResult{}, errScan
		}
		if len(result.Events) == limit {
			last := result.Events[len(result.Events)-1]
			result.NextCursor = encodeCursor(last.StartedAt.UnixMilli(), last.ID)
			break
		}
		result.Events = append(result.Events, event)
	}
	if err = rows.Err(); err != nil {
		return ListResult{}, fmt.Errorf("iterate request-log events: %w", err)
	}
	return result, nil
}

// Get returns one event by its internal identifier.
func (s *Store) Get(ctx context.Context, id string) (Event, error) {
	if s == nil {
		return Event{}, ErrDisabled
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+eventColumns+` FROM request_events WHERE id = ?`, id)
	event, err := scanEvent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

// Summary returns aggregate counters and duration percentiles for the requested interval.
func (s *Store) Summary(ctx context.Context, filter SummaryFilter) (Summary, error) {
	if s == nil {
		return Summary{}, ErrDisabled
	}
	where, args := listWhere(filter.From, filter.To, filter.Provider, filter.Model, filter.Account, filter.APIKey, nil, "", "")
	query := `SELECT COUNT(*),
COALESCE(SUM(CASE WHEN outcome != 'success' THEN 1 ELSE 0 END), 0),
COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached_tokens), 0),
COALESCE(SUM(estimated_cost_usd), 0)
FROM request_events` + whereSQL(where)
	var summary Summary
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&summary.Requests, &summary.Failures, &summary.InputTokens, &summary.OutputTokens, &summary.CachedTokens, &summary.EstimatedCostUSD,
	); err != nil {
		return Summary{}, fmt.Errorf("summarize request-log events: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT duration_ms FROM request_events`+whereSQL(where)+` ORDER BY duration_ms ASC LIMIT 10000`, args...)
	if err != nil {
		return Summary{}, fmt.Errorf("read request-log durations: %w", err)
	}
	defer func() {
		if errClose := rows.Close(); errClose != nil {
			log.WithError(errClose).Warn("close failed request-log summary rows")
		}
	}()
	durations := make([]int64, 0)
	for rows.Next() {
		var duration int64
		if errScan := rows.Scan(&duration); errScan != nil {
			return Summary{}, fmt.Errorf("scan request-log duration: %w", errScan)
		}
		durations = append(durations, duration)
	}
	if err = rows.Err(); err != nil {
		return Summary{}, fmt.Errorf("iterate request-log durations: %w", err)
	}
	summary.P50DurationMS = percentile(durations, 0.50)
	summary.P95DurationMS = percentile(durations, 0.95)
	return summary, nil
}

// Export invokes callback once for every matching event, from oldest to newest.
func (s *Store) Export(ctx context.Context, filter ExportFilter, callback func(Event) error) error {
	if s == nil {
		return ErrDisabled
	}
	if callback == nil {
		return fmt.Errorf("request-log export callback is required")
	}
	where, args := listWhere(filter.From, filter.To, filter.Provider, filter.Model, filter.Account, filter.APIKey, filter.Status, filter.Outcome, filter.RequestID)
	rows, err := s.db.QueryContext(ctx, `SELECT `+eventColumns+` FROM request_events`+whereSQL(where)+` ORDER BY started_at ASC, id ASC`, args...)
	if err != nil {
		return fmt.Errorf("export request-log events: %w", err)
	}
	defer func() {
		if errClose := rows.Close(); errClose != nil {
			log.WithError(errClose).Warn("close failed request-log export rows")
		}
	}()
	for rows.Next() {
		event, errScan := scanEvent(rows)
		if errScan != nil {
			return errScan
		}
		if errCallback := callback(event); errCallback != nil {
			return errCallback
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterate exported request-log events: %w", err)
	}
	return nil
}

// Purge removes rows before the supplied timestamp in bounded batches.
func (s *Store) Purge(ctx context.Context, before time.Time) (int, error) {
	if s == nil {
		return 0, ErrDisabled
	}
	if before.IsZero() {
		return 0, fmt.Errorf("request-log purge timestamp is required")
	}
	deletedTotal := 0
	for {
		result, err := s.db.ExecContext(ctx, `DELETE FROM request_events WHERE id IN (SELECT id FROM request_events WHERE started_at < ? ORDER BY started_at ASC LIMIT ?)`, before.UnixMilli(), cleanupBatchSize)
		if err != nil {
			return deletedTotal, fmt.Errorf("purge request-log events: %w", err)
		}
		deleted, errRows := result.RowsAffected()
		if errRows != nil {
			return deletedTotal, fmt.Errorf("read purged request-log rows: %w", errRows)
		}
		deletedTotal += int(deleted)
		if deleted < cleanupBatchSize {
			return deletedTotal, nil
		}
	}
}

// Health returns safe operational state for the request-log store.
func (s *Store) Health(ctx context.Context) (Health, error) {
	if s == nil {
		return Health{Enabled: false}, nil
	}
	health := Health{Enabled: true, QueueDepth: len(s.queue), DroppedEvents: s.dropped.Load(), DatabaseBytes: s.databaseSize()}
	if err := s.db.PingContext(ctx); err == nil {
		health.Writable = true
	} else {
		s.recordError(err)
	}
	var oldest, newest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(started_at), MAX(started_at) FROM request_events`).Scan(&oldest, &newest); err != nil {
		return health, fmt.Errorf("read request-log event range: %w", err)
	}
	if oldest.Valid {
		value := time.UnixMilli(oldest.Int64).UTC()
		health.OldestEvent = &value
	}
	if newest.Valid {
		value := time.UnixMilli(newest.Int64).UTC()
		health.NewestEvent = &value
	}
	s.lastErrorMu.RLock()
	health.LastError = s.lastError
	s.lastErrorMu.RUnlock()
	return health, nil
}

func listWhere(from, to time.Time, provider, model, account, apiKey string, status *int, outcome, requestID string) ([]string, []any) {
	where := make([]string, 0, 9)
	args := make([]any, 0, 10)
	if !from.IsZero() {
		where, args = append(where, `started_at >= ?`), append(args, from.UnixMilli())
	}
	if !to.IsZero() {
		where, args = append(where, `started_at <= ?`), append(args, to.UnixMilli())
	}
	for _, filter := range []struct {
		column string
		value  string
	}{{"provider", provider}, {"model_resolved", model}, {"account_alias", account}, {"api_key_alias", apiKey}, {"outcome", outcome}, {"request_id", requestID}} {
		if value := strings.TrimSpace(filter.value); value != "" {
			where, args = append(where, filter.column+` = ?`), append(args, value)
		}
	}
	if status != nil {
		where, args = append(where, `status_code = ?`), append(args, *status)
	}
	return where, args
}

func whereSQL(where []string) string {
	if len(where) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(where, " AND ")
}

type scanner interface{ Scan(...any) error }

func scanEvent(source scanner) (Event, error) {
	var event Event
	var startedAt, completedAt int64
	var inputTokens, outputTokens, cachedTokens sql.NullInt64
	var estimatedCost sql.NullFloat64
	var metadata string
	if err := source.Scan(
		&event.ID, &event.RequestID, &startedAt, &completedAt, &event.DurationMS, &event.Method, &event.Route,
		&event.Provider, &event.ModelRequested, &event.ModelResolved, &event.AccountAlias, &event.APIKeyAlias,
		&event.StatusCode, &event.Outcome, &inputTokens, &outputTokens, &cachedTokens, &estimatedCost, &event.RetryCount,
		&event.ErrorClass, &event.ErrorMessage, &metadata,
	); err != nil {
		return Event{}, err
	}
	event.StartedAt = time.UnixMilli(startedAt).UTC()
	event.CompletedAt = time.UnixMilli(completedAt).UTC()
	event.InputTokens = nullableInt64(inputTokens)
	event.OutputTokens = nullableInt64(outputTokens)
	event.CachedTokens = nullableInt64(cachedTokens)
	if estimatedCost.Valid {
		value := estimatedCost.Float64
		event.EstimatedCostUSD = &value
	}
	if metadata != "" && json.Unmarshal([]byte(metadata), &event.Metadata) != nil {
		event.Metadata = nil
	}
	return event, nil
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}
