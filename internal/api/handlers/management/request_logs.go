package management

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/requestlog"
	log "github.com/sirupsen/logrus"
)

const (
	defaultRequestLogLimit       = 50
	maxRequestLogLimit           = 200
	maxRequestLogSummaryRange    = 31 * 24 * time.Hour
	maxRequestLogExportRange     = 7 * 24 * time.Hour
	defaultRequestLogExportRange = 24 * time.Hour
	maxRequestLogExportRows      = 10000
)

var (
	// ErrRequestLogsDisabled is returned when persistent request-log capture is disabled.
	ErrRequestLogsDisabled = errors.New("request log storage disabled")
	// ErrRequestLogNotFound is returned when an event ID does not exist.
	ErrRequestLogNotFound    = errors.New("request log event not found")
	errRequestLogExportLimit = errors.New("request log export limit reached")
)

var (
	requestLogBearerPattern = regexp.MustCompile(`(?i)bearer[[:space:]]+[^[:space:],;]+`)
	requestLogJWTPattern    = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*\b`)
	requestLogQueryPattern  = regexp.MustCompile(`(?i)(api[_-]?key|token|password|secret)=([^&#[:space:]]+)`)
)

// RequestLogEvent is the allow-listed event shape exposed by the management API.
// It intentionally contains no headers, bodies, credentials, or raw provider errors.
type RequestLogEvent struct {
	ID               string    `json:"id"`
	RequestID        string    `json:"requestId,omitempty"`
	StartedAt        time.Time `json:"startedAt"`
	CompletedAt      time.Time `json:"completedAt,omitempty"`
	DurationMS       int64     `json:"durationMs"`
	Method           string    `json:"method,omitempty"`
	Route            string    `json:"route,omitempty"`
	Provider         string    `json:"provider,omitempty"`
	ModelRequested   string    `json:"modelRequested,omitempty"`
	ModelResolved    string    `json:"modelResolved,omitempty"`
	AccountAlias     string    `json:"accountAlias,omitempty"`
	APIKeyAlias      string    `json:"apiKeyAlias,omitempty"`
	StatusCode       int       `json:"statusCode"`
	Outcome          string    `json:"outcome,omitempty"`
	InputTokens      *int64    `json:"inputTokens,omitempty"`
	OutputTokens     *int64    `json:"outputTokens,omitempty"`
	CachedTokens     *int64    `json:"cachedTokens,omitempty"`
	EstimatedCostUSD *float64  `json:"estimatedCostUsd,omitempty"`
	RetryCount       int       `json:"retryCount"`
	ErrorClass       string    `json:"errorClass,omitempty"`
	ErrorMessage     string    `json:"errorMessage,omitempty"`
}

// RequestLogQuery describes the bounded filters accepted by list and export operations.
type RequestLogQuery struct {
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

// RequestLogPage is a cursor-paginated collection of request events.
type RequestLogPage struct {
	Events     []RequestLogEvent `json:"events"`
	NextCursor string            `json:"nextCursor,omitempty"`
}

// RequestLogSummary contains aggregate request-log metrics for a bounded interval.
type RequestLogSummary struct {
	RequestCount     int64   `json:"requestCount"`
	FailureCount     int64   `json:"failureCount"`
	ErrorRate        float64 `json:"errorRate"`
	InputTokens      int64   `json:"inputTokens"`
	OutputTokens     int64   `json:"outputTokens"`
	CachedTokens     int64   `json:"cachedTokens"`
	EstimatedCostUSD float64 `json:"estimatedCostUsd"`
	P50LatencyMS     int64   `json:"p50LatencyMs"`
	P95LatencyMS     int64   `json:"p95LatencyMs"`
}

// RequestLogRepository is the narrow persistence interface required by the
// management API. Storage implementations must return only sanitized events.
type RequestLogRepository interface {
	List(context.Context, RequestLogQuery) (RequestLogPage, error)
	Get(context.Context, string) (RequestLogEvent, error)
	Summary(context.Context, RequestLogQuery) (RequestLogSummary, error)
	Export(context.Context, RequestLogQuery, func(RequestLogEvent) error) error
	DeleteBefore(context.Context, time.Time) (int64, error)
}

type requestLogStoreAdapter struct {
	repository requestlog.Repository
}

func (a requestLogStoreAdapter) List(ctx context.Context, query RequestLogQuery) (RequestLogPage, error) {
	result, errList := a.repository.List(ctx, requestlog.ListFilter{
		From: query.From, To: query.To, Cursor: query.Cursor, Limit: query.Limit,
		Provider: query.Provider, Model: query.Model, Account: query.Account, APIKey: query.APIKey,
		Status: query.Status, Outcome: query.Outcome, RequestID: query.RequestID,
	})
	if errList != nil {
		return RequestLogPage{}, mapRequestLogStoreError(errList)
	}
	page := RequestLogPage{Events: make([]RequestLogEvent, 0, len(result.Events)), NextCursor: result.NextCursor}
	for _, event := range result.Events {
		page.Events = append(page.Events, requestLogEventFromStore(event))
	}
	return page, nil
}

func (a requestLogStoreAdapter) Get(ctx context.Context, id string) (RequestLogEvent, error) {
	event, errGet := a.repository.Get(ctx, id)
	if errGet != nil {
		return RequestLogEvent{}, mapRequestLogStoreError(errGet)
	}
	return requestLogEventFromStore(event), nil
}

func (a requestLogStoreAdapter) Summary(ctx context.Context, query RequestLogQuery) (RequestLogSummary, error) {
	summary, errSummary := a.repository.Summary(ctx, requestlog.SummaryFilter{
		From: query.From, To: query.To, Provider: query.Provider, Model: query.Model, Account: query.Account, APIKey: query.APIKey,
	})
	if errSummary != nil {
		return RequestLogSummary{}, mapRequestLogStoreError(errSummary)
	}
	result := RequestLogSummary{
		RequestCount: summary.Requests, FailureCount: summary.Failures, InputTokens: summary.InputTokens,
		OutputTokens: summary.OutputTokens, CachedTokens: summary.CachedTokens, EstimatedCostUSD: summary.EstimatedCostUSD,
		P50LatencyMS: summary.P50DurationMS, P95LatencyMS: summary.P95DurationMS,
	}
	if result.RequestCount > 0 {
		result.ErrorRate = float64(result.FailureCount) / float64(result.RequestCount)
	}
	return result, nil
}

func (a requestLogStoreAdapter) Export(ctx context.Context, query RequestLogQuery, visit func(RequestLogEvent) error) error {
	errExport := a.repository.Export(ctx, requestlog.ExportFilter{
		From: query.From, To: query.To, Provider: query.Provider, Model: query.Model, Account: query.Account, APIKey: query.APIKey,
		Status: query.Status, Outcome: query.Outcome, RequestID: query.RequestID,
	}, func(event requestlog.Event) error {
		return visit(requestLogEventFromStore(event))
	})
	return mapRequestLogStoreError(errExport)
}

func (a requestLogStoreAdapter) DeleteBefore(ctx context.Context, before time.Time) (int64, error) {
	deleted, errPurge := a.repository.Purge(ctx, before)
	return int64(deleted), mapRequestLogStoreError(errPurge)
}

func mapRequestLogStoreError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, requestlog.ErrDisabled):
		return ErrRequestLogsDisabled
	case errors.Is(err, requestlog.ErrNotFound):
		return ErrRequestLogNotFound
	default:
		return err
	}
}

func requestLogEventFromStore(event requestlog.Event) RequestLogEvent {
	return RequestLogEvent{
		ID: event.ID, RequestID: event.RequestID, StartedAt: event.StartedAt, CompletedAt: event.CompletedAt,
		DurationMS: event.DurationMS, Method: event.Method, Route: event.Route, Provider: event.Provider,
		ModelRequested: event.ModelRequested, ModelResolved: event.ModelResolved, AccountAlias: event.AccountAlias,
		APIKeyAlias: event.APIKeyAlias, StatusCode: event.StatusCode, Outcome: event.Outcome, InputTokens: event.InputTokens,
		OutputTokens: event.OutputTokens, CachedTokens: event.CachedTokens, EstimatedCostUSD: event.EstimatedCostUSD,
		RetryCount: event.RetryCount, ErrorClass: event.ErrorClass, ErrorMessage: event.ErrorMessage,
	}
}

// ListRequestLogs returns request events with cursor pagination and allow-listed filters.
func (h *Handler) ListRequestLogs(c *gin.Context) {
	query, ok := parseRequestLogQuery(c, defaultRequestLogLimit, maxRequestLogLimit, false)
	if !ok {
		return
	}
	repository := h.getRequestLogRepository()
	if repository == nil {
		writeRequestLogError(c, http.StatusServiceUnavailable, "request_logs_unavailable", "request log storage is unavailable")
		return
	}
	page, errList := repository.List(c.Request.Context(), query)
	if !handleRequestLogRepositoryError(c, errList) {
		return
	}
	for index := range page.Events {
		page.Events[index] = sanitizeRequestLogEvent(page.Events[index])
	}
	c.JSON(http.StatusOK, page)
}

// GetRequestLogEvent returns a single sanitized request-log event by ID.
func (h *Handler) GetRequestLogEvent(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		writeRequestLogError(c, http.StatusBadRequest, "invalid_request_log_id", "request log ID is required")
		return
	}
	repository := h.getRequestLogRepository()
	if repository == nil {
		writeRequestLogError(c, http.StatusServiceUnavailable, "request_logs_unavailable", "request log storage is unavailable")
		return
	}
	event, errGet := repository.Get(c.Request.Context(), id)
	if !handleRequestLogRepositoryError(c, errGet) {
		return
	}
	c.JSON(http.StatusOK, sanitizeRequestLogEvent(event))
}

// GetRequestLogSummary returns aggregate metrics for at most 31 days.
func (h *Handler) GetRequestLogSummary(c *gin.Context) {
	query, ok := parseRequestLogQuery(c, 0, 0, true)
	if !ok {
		return
	}
	if !ensureRequestLogRange(c, &query, maxRequestLogSummaryRange, maxRequestLogSummaryRange) {
		return
	}
	repository := h.getRequestLogRepository()
	if repository == nil {
		writeRequestLogError(c, http.StatusServiceUnavailable, "request_logs_unavailable", "request log storage is unavailable")
		return
	}
	summary, errSummary := repository.Summary(c.Request.Context(), query)
	if !handleRequestLogRepositoryError(c, errSummary) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"from": query.From, "to": query.To, "summary": summary})
}

// ExportRequestLogs streams a bounded CSV or JSON Lines export of sanitized events.
func (h *Handler) ExportRequestLogs(c *gin.Context) {
	format := strings.ToLower(strings.TrimSpace(c.DefaultQuery("format", "csv")))
	if format != "csv" && format != "jsonl" {
		writeRequestLogError(c, http.StatusBadRequest, "invalid_export_format", "format must be csv or jsonl")
		return
	}
	query, ok := parseRequestLogQuery(c, maxRequestLogExportRows, maxRequestLogExportRows, true)
	if !ok {
		return
	}
	if !ensureRequestLogRange(c, &query, defaultRequestLogExportRange, maxRequestLogExportRange) {
		return
	}
	repository := h.getRequestLogRepository()
	if repository == nil {
		writeRequestLogError(c, http.StatusServiceUnavailable, "request_logs_unavailable", "request log storage is unavailable")
		return
	}

	filename := "request-logs-" + time.Now().UTC().Format("20060102T150405Z") + "." + format
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	if format == "csv" {
		c.Header("Content-Type", "text/csv; charset=utf-8")
	} else {
		c.Header("Content-Type", "application/x-ndjson; charset=utf-8")
	}

	var writeEvent func(RequestLogEvent) error
	if format == "csv" {
		writer := csv.NewWriter(c.Writer)
		if errHeader := writer.Write(requestLogCSVHeader); errHeader != nil {
			writeRequestLogError(c, http.StatusInternalServerError, "request_log_export_failed", "failed to write request log export")
			return
		}
		writer.Flush()
		if errHeader := writer.Error(); errHeader != nil {
			writeRequestLogError(c, http.StatusInternalServerError, "request_log_export_failed", "failed to write request log export")
			return
		}
		writeEvent = func(event RequestLogEvent) error {
			if errRow := writer.Write(requestLogCSVRow(sanitizeRequestLogEvent(event))); errRow != nil {
				return errRow
			}
			writer.Flush()
			return writer.Error()
		}
	} else {
		encoder := json.NewEncoder(c.Writer)
		writeEvent = func(event RequestLogEvent) error {
			return encoder.Encode(sanitizeRequestLogEvent(event))
		}
	}

	exported := 0
	errExport := repository.Export(c.Request.Context(), query, func(event RequestLogEvent) error {
		if exported >= maxRequestLogExportRows {
			return errRequestLogExportLimit
		}
		exported++
		return writeEvent(event)
	})
	if errExport != nil && !errors.Is(errExport, errRequestLogExportLimit) {
		log.WithError(errExport).Warn("management request-log export failed")
		return
	}
}

// DeleteRequestLogs purges events older than the required before timestamp.
func (h *Handler) DeleteRequestLogs(c *gin.Context) {
	before, errBefore := parseRequestLogTimestamp(strings.TrimSpace(c.Query("before")))
	if errBefore != nil || before.IsZero() {
		writeRequestLogError(c, http.StatusBadRequest, "invalid_purge_before", "before must be an RFC3339 or Unix millisecond timestamp")
		return
	}
	repository := h.getRequestLogRepository()
	if repository == nil {
		writeRequestLogError(c, http.StatusServiceUnavailable, "request_logs_unavailable", "request log storage is unavailable")
		return
	}
	deleted, errDelete := repository.DeleteBefore(c.Request.Context(), before)
	if !handleRequestLogRepositoryError(c, errDelete) {
		return
	}
	log.WithFields(log.Fields{"before": before.UTC().Format(time.RFC3339), "deleted": deleted}).Info("management request-log purge completed")
	c.JSON(http.StatusOK, gin.H{"deleted": deleted, "before": before})
}

func parseRequestLogQuery(c *gin.Context, defaultLimit, maxLimit int, permitZeroLimit bool) (RequestLogQuery, bool) {
	query := RequestLogQuery{
		Cursor:    strings.TrimSpace(c.Query("cursor")),
		Provider:  strings.TrimSpace(c.Query("provider")),
		Model:     strings.TrimSpace(c.Query("model")),
		Account:   strings.TrimSpace(c.Query("account")),
		APIKey:    strings.TrimSpace(c.Query("apiKey")),
		Outcome:   strings.TrimSpace(c.Query("outcome")),
		RequestID: strings.TrimSpace(c.Query("requestId")),
	}
	var errTime error
	if query.From, errTime = parseRequestLogTimestamp(strings.TrimSpace(c.Query("from"))); errTime != nil {
		writeRequestLogError(c, http.StatusBadRequest, "invalid_from", "from must be an RFC3339 or Unix millisecond timestamp")
		return RequestLogQuery{}, false
	}
	if query.To, errTime = parseRequestLogTimestamp(strings.TrimSpace(c.Query("to"))); errTime != nil {
		writeRequestLogError(c, http.StatusBadRequest, "invalid_to", "to must be an RFC3339 or Unix millisecond timestamp")
		return RequestLogQuery{}, false
	}
	if !query.From.IsZero() && !query.To.IsZero() && query.To.Before(query.From) {
		writeRequestLogError(c, http.StatusBadRequest, "invalid_time_range", "to must not be before from")
		return RequestLogQuery{}, false
	}
	if rawStatus := strings.TrimSpace(c.Query("status")); rawStatus != "" {
		status, errStatus := strconv.Atoi(rawStatus)
		if errStatus != nil || status < 100 || status > 599 {
			writeRequestLogError(c, http.StatusBadRequest, "invalid_status", "status must be a valid HTTP status code")
			return RequestLogQuery{}, false
		}
		query.Status = &status
	}
	if maxLimit == 0 {
		return query, true
	}
	limit := defaultLimit
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsedLimit, errLimit := strconv.Atoi(rawLimit)
		if errLimit != nil || parsedLimit < 1 || parsedLimit > maxLimit {
			writeRequestLogError(c, http.StatusBadRequest, "invalid_limit", fmt.Sprintf("limit must be between 1 and %d", maxLimit))
			return RequestLogQuery{}, false
		}
		limit = parsedLimit
	}
	if !permitZeroLimit || limit != 0 {
		query.Limit = limit
	}
	return query, true
}

func ensureRequestLogRange(c *gin.Context, query *RequestLogQuery, defaultRange, maxRange time.Duration) bool {
	if query == nil {
		writeRequestLogError(c, http.StatusInternalServerError, "request_logs_unavailable", "request log storage is unavailable")
		return false
	}
	if query.To.IsZero() {
		query.To = time.Now().UTC()
	}
	if query.From.IsZero() {
		query.From = query.To.Add(-defaultRange)
	}
	if query.To.Before(query.From) || query.To.Sub(query.From) > maxRange {
		writeRequestLogError(c, http.StatusBadRequest, "time_range_too_large", fmt.Sprintf("time range must not exceed %s", maxRange))
		return false
	}
	return true
}

func parseRequestLogTimestamp(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	if milliseconds, errMilliseconds := strconv.ParseInt(raw, 10, 64); errMilliseconds == nil {
		return time.UnixMilli(milliseconds).UTC(), nil
	}
	parsed, errParsed := time.Parse(time.RFC3339, raw)
	if errParsed != nil {
		return time.Time{}, errParsed
	}
	return parsed.UTC(), nil
}

func handleRequestLogRepositoryError(c *gin.Context, err error) bool {
	if err == nil {
		return true
	}
	switch {
	case errors.Is(err, ErrRequestLogsDisabled):
		writeRequestLogError(c, http.StatusServiceUnavailable, "request_logs_disabled", "request log storage is disabled")
	case errors.Is(err, ErrRequestLogNotFound):
		writeRequestLogError(c, http.StatusNotFound, "request_log_not_found", "request log event was not found")
	default:
		log.WithError(err).Warn("management request-log operation failed")
		writeRequestLogError(c, http.StatusInternalServerError, "request_log_operation_failed", "request log operation failed")
	}
	return false
}

func writeRequestLogError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}

func sanitizeRequestLogEvent(event RequestLogEvent) RequestLogEvent {
	event.ID = sanitizeRequestLogText(event.ID, 256)
	event.RequestID = sanitizeRequestLogText(event.RequestID, 256)
	event.Method = sanitizeRequestLogText(event.Method, 32)
	event.Route = strings.SplitN(sanitizeRequestLogText(event.Route, 512), "?", 2)[0]
	event.Provider = sanitizeRequestLogText(event.Provider, 128)
	event.ModelRequested = sanitizeRequestLogText(event.ModelRequested, 256)
	event.ModelResolved = sanitizeRequestLogText(event.ModelResolved, 256)
	event.AccountAlias = sanitizeRequestLogText(event.AccountAlias, 256)
	event.APIKeyAlias = sanitizeRequestLogText(event.APIKeyAlias, 256)
	event.Outcome = sanitizeRequestLogText(event.Outcome, 64)
	event.ErrorClass = sanitizeRequestLogText(event.ErrorClass, 128)
	event.ErrorMessage = sanitizeRequestLogText(event.ErrorMessage, 1024)
	return event
}

func sanitizeRequestLogText(value string, maximum int) string {
	value = requestLogBearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	value = requestLogJWTPattern.ReplaceAllString(value, "[REDACTED_JWT]")
	value = requestLogQueryPattern.ReplaceAllString(value, "$1=[REDACTED]")
	if maximum > 0 && len(value) > maximum {
		return value[:maximum]
	}
	return value
}

var requestLogCSVHeader = []string{
	"id", "request_id", "started_at", "completed_at", "duration_ms", "method", "route", "provider", "model_requested", "model_resolved", "account_alias", "api_key_alias", "status_code", "outcome", "input_tokens", "output_tokens", "cached_tokens", "estimated_cost_usd", "retry_count", "error_class", "error_message",
}

func requestLogCSVRow(event RequestLogEvent) []string {
	return []string{
		event.ID,
		event.RequestID,
		formatRequestLogTime(event.StartedAt),
		formatRequestLogTime(event.CompletedAt),
		strconv.FormatInt(event.DurationMS, 10),
		event.Method,
		event.Route,
		event.Provider,
		event.ModelRequested,
		event.ModelResolved,
		event.AccountAlias,
		event.APIKeyAlias,
		strconv.Itoa(event.StatusCode),
		event.Outcome,
		formatRequestLogInt(event.InputTokens),
		formatRequestLogInt(event.OutputTokens),
		formatRequestLogInt(event.CachedTokens),
		formatRequestLogFloat(event.EstimatedCostUSD),
		strconv.Itoa(event.RetryCount),
		event.ErrorClass,
		event.ErrorMessage,
	}
}

func formatRequestLogTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func formatRequestLogInt(value *int64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(*value, 10)
}

func formatRequestLogFloat(value *float64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatFloat(*value, 'f', -1, 64)
}
