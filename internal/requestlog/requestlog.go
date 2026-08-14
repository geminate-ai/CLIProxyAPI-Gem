// Package requestlog provides durable, sanitized request event storage.
package requestlog

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrDisabled is returned by a disabled request-log repository.
	ErrDisabled = errors.New("request logging is disabled")
	// ErrNotFound is returned when a requested event does not exist.
	ErrNotFound = errors.New("request log event not found")
)

// Event contains sanitized request metadata. It never contains request bodies,
// response bodies, headers, or credentials.
type Event struct {
	ID               string            `json:"id"`
	RequestID        string            `json:"requestId"`
	StartedAt        time.Time         `json:"startedAt"`
	CompletedAt      time.Time         `json:"completedAt"`
	DurationMS       int64             `json:"durationMs"`
	Method           string            `json:"method"`
	Route            string            `json:"route"`
	Provider         string            `json:"provider"`
	ModelRequested   string            `json:"modelRequested"`
	ModelResolved    string            `json:"modelResolved"`
	AccountAlias     string            `json:"accountAlias"`
	APIKeyAlias      string            `json:"apiKeyAlias"`
	StatusCode       int               `json:"statusCode"`
	Outcome          string            `json:"outcome"`
	InputTokens      *int64            `json:"inputTokens,omitempty"`
	OutputTokens     *int64            `json:"outputTokens,omitempty"`
	CachedTokens     *int64            `json:"cachedTokens,omitempty"`
	EstimatedCostUSD *float64          `json:"estimatedCostUsd,omitempty"`
	RetryCount       int               `json:"retryCount"`
	ErrorClass       string            `json:"errorClass,omitempty"`
	ErrorMessage     string            `json:"errorMessage,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// ListFilter narrows request event queries. A zero value returns the newest events.
type ListFilter struct {
	From      time.Time
	To        time.Time
	Cursor    string
	Limit     int
	Provider  string
	Model     string
	Account   string
	APIKey    string
	Status    *int
	Outcome   string
	RequestID string
}

// ListResult is one newest-first page of events.
type ListResult struct {
	Events     []Event `json:"events"`
	NextCursor string  `json:"nextCursor,omitempty"`
}

// SummaryFilter narrows aggregate statistics.
type SummaryFilter struct {
	From     time.Time
	To       time.Time
	Provider string
	Model    string
	Account  string
	APIKey   string
}

// Summary contains aggregate values for a bounded request event interval.
type Summary struct {
	Requests         int64   `json:"requests"`
	Failures         int64   `json:"failures"`
	InputTokens      int64   `json:"inputTokens"`
	OutputTokens     int64   `json:"outputTokens"`
	CachedTokens     int64   `json:"cachedTokens"`
	EstimatedCostUSD float64 `json:"estimatedCostUsd"`
	P50DurationMS    int64   `json:"p50DurationMs"`
	P95DurationMS    int64   `json:"p95DurationMs"`
}

// ExportFilter narrows an export. The same sanitized Event is returned as list/detail queries.
type ExportFilter struct {
	From      time.Time
	To        time.Time
	Provider  string
	Model     string
	Account   string
	APIKey    string
	Status    *int
	Outcome   string
	RequestID string
}

// Health reports storage state without exposing filesystem paths or errors containing secrets.
type Health struct {
	Enabled       bool       `json:"enabled"`
	Writable      bool       `json:"writable"`
	QueueDepth    int        `json:"queueDepth"`
	DroppedEvents uint64     `json:"droppedEvents"`
	DatabaseBytes int64      `json:"databaseBytes"`
	OldestEvent   *time.Time `json:"oldestEvent,omitempty"`
	NewestEvent   *time.Time `json:"newestEvent,omitempty"`
	LastError     string     `json:"lastError,omitempty"`
}

// Repository is the query surface consumed by management handlers.
type Repository interface {
	List(context.Context, ListFilter) (ListResult, error)
	Get(context.Context, string) (Event, error)
	Summary(context.Context, SummaryFilter) (Summary, error)
	Export(context.Context, ExportFilter, func(Event) error) error
	Purge(context.Context, time.Time) (int, error)
	Health(context.Context) (Health, error)
}

// Options configures a SQLite request-log Store.
type Options struct {
	Path            string
	RetentionDays   int
	MaxDatabaseMB   int
	CleanupInterval time.Duration
	BusyTimeout     time.Duration
	RedactErrors    bool
	QueueSize       int
}
