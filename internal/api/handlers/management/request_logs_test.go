package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

type requestLogRepositoryStub struct {
	listPage     RequestLogPage
	listErr      error
	getEvent     RequestLogEvent
	getErr       error
	summary      RequestLogSummary
	summaryErr   error
	exportEvents []RequestLogEvent
	exportErr    error
	deleted      int64
	deleteErr    error
	listQuery    RequestLogQuery
	summaryQuery RequestLogQuery
	exportQuery  RequestLogQuery
	deleteBefore time.Time
}

func (s *requestLogRepositoryStub) List(_ context.Context, query RequestLogQuery) (RequestLogPage, error) {
	s.listQuery = query
	return s.listPage, s.listErr
}

func (s *requestLogRepositoryStub) Get(_ context.Context, _ string) (RequestLogEvent, error) {
	return s.getEvent, s.getErr
}

func (s *requestLogRepositoryStub) Summary(_ context.Context, query RequestLogQuery) (RequestLogSummary, error) {
	s.summaryQuery = query
	return s.summary, s.summaryErr
}

func (s *requestLogRepositoryStub) Export(_ context.Context, query RequestLogQuery, visit func(RequestLogEvent) error) error {
	s.exportQuery = query
	if s.exportErr != nil {
		return s.exportErr
	}
	for _, event := range s.exportEvents {
		if errVisit := visit(event); errVisit != nil {
			return errVisit
		}
	}
	return nil
}

func (s *requestLogRepositoryStub) DeleteBefore(_ context.Context, before time.Time) (int64, error) {
	s.deleteBefore = before
	return s.deleted, s.deleteErr
}

func TestListRequestLogsPassesBoundedFiltersAndRedactsLegacyData(t *testing.T) {
	stub := &requestLogRepositoryStub{listPage: RequestLogPage{Events: []RequestLogEvent{{
		ID:           "event-1",
		Route:        "/v1/chat?api_key=not-safe",
		ErrorMessage: "upstream rejected Bearer very-secret-token eyJhbGciOiJIUzI1NiJ9.abc.def",
	}}}}
	handler := newRequestLogTestHandler(stub)
	response := performRequestLogRequest(t, handler.ListRequestLogs, http.MethodGet, "/v0/management/request-logs?limit=12&provider=claude&model=sonnet&account=team-a&apiKey=key-a&status=429&outcome=upstream_error&requestId=req-1")
	if response.Code != http.StatusOK {
		t.Fatalf("ListRequestLogs status = %d body=%s", response.Code, response.Body.String())
	}
	if stub.listQuery.Limit != 12 || stub.listQuery.Provider != "claude" || stub.listQuery.Model != "sonnet" || stub.listQuery.Account != "team-a" || stub.listQuery.APIKey != "key-a" || stub.listQuery.Outcome != "upstream_error" || stub.listQuery.RequestID != "req-1" {
		t.Fatalf("unexpected list query: %#v", stub.listQuery)
	}
	if stub.listQuery.Status == nil || *stub.listQuery.Status != 429 {
		t.Fatalf("status filter = %#v, want 429", stub.listQuery.Status)
	}
	if body := response.Body.String(); strings.Contains(body, "very-secret-token") || strings.Contains(body, "eyJhbGciOiJIUzI1NiJ9") || strings.Contains(body, "not-safe") || strings.Contains(body, "?") {
		t.Fatalf("response leaked sensitive legacy data: %s", body)
	}
}

func TestListRequestLogsRejectsInvalidLimitBeforeRepository(t *testing.T) {
	stub := &requestLogRepositoryStub{}
	response := performRequestLogRequest(t, newRequestLogTestHandler(stub).ListRequestLogs, http.MethodGet, "/v0/management/request-logs?limit=201")
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "invalid_limit") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if stub.listQuery.Limit != 0 {
		t.Fatalf("repository should not be called: %#v", stub.listQuery)
	}
}

func TestGetRequestLogEventMapsNotFound(t *testing.T) {
	stub := &requestLogRepositoryStub{getErr: ErrRequestLogNotFound}
	gin.SetMode(gin.TestMode)
	response := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(response)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/request-logs/missing", nil)
	ctx.AddParam("id", "missing")
	newRequestLogTestHandler(stub).GetRequestLogEvent(ctx)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), "request_log_not_found") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGetRequestLogSummaryDefaultsAndBoundsRange(t *testing.T) {
	stub := &requestLogRepositoryStub{summary: RequestLogSummary{RequestCount: 3}}
	response := performRequestLogRequest(t, newRequestLogTestHandler(stub).GetRequestLogSummary, http.MethodGet, "/v0/management/request-logs/summary?from=2026-01-01T00:00:00Z&to=2026-02-02T00:00:01Z")
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "time_range_too_large") {
		t.Fatalf("oversized range status=%d body=%s", response.Code, response.Body.String())
	}
	response = performRequestLogRequest(t, newRequestLogTestHandler(stub).GetRequestLogSummary, http.MethodGet, "/v0/management/request-logs/summary")
	if response.Code != http.StatusOK {
		t.Fatalf("default summary status=%d body=%s", response.Code, response.Body.String())
	}
	if stub.summaryQuery.From.IsZero() || stub.summaryQuery.To.IsZero() || stub.summaryQuery.To.Sub(stub.summaryQuery.From) != maxRequestLogSummaryRange {
		t.Fatalf("unexpected default interval: %#v", stub.summaryQuery)
	}
}

func TestExportRequestLogsStreamsOnlySanitizedEvents(t *testing.T) {
	stub := &requestLogRepositoryStub{exportEvents: []RequestLogEvent{{
		ID:           "event-1",
		Provider:     "codex",
		ErrorMessage: "authorization failed: token=super-secret",
	}}}
	response := performRequestLogRequest(t, newRequestLogTestHandler(stub).ExportRequestLogs, http.MethodGet, "/v0/management/request-logs/export?format=jsonl")
	if response.Code != http.StatusOK {
		t.Fatalf("export status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Header().Get("Content-Type"), "application/x-ndjson") || strings.Contains(response.Body.String(), "super-secret") || !strings.Contains(response.Body.String(), "[REDACTED]") {
		t.Fatalf("unexpected export headers/body: headers=%v body=%s", response.Header(), response.Body.String())
	}
	if stub.exportQuery.Limit != maxRequestLogExportRows || stub.exportQuery.To.Sub(stub.exportQuery.From) != defaultRequestLogExportRange {
		t.Fatalf("unexpected export query: %#v", stub.exportQuery)
	}
}

func TestDeleteRequestLogsUsesRequiredTimestampAndMapsDisabled(t *testing.T) {
	stub := &requestLogRepositoryStub{deleted: 4}
	handler := newRequestLogTestHandler(stub)
	response := performRequestLogRequest(t, handler.DeleteRequestLogs, http.MethodDelete, "/v0/management/request-logs")
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "invalid_purge_before") {
		t.Fatalf("missing before status=%d body=%s", response.Code, response.Body.String())
	}
	response = performRequestLogRequest(t, handler.DeleteRequestLogs, http.MethodDelete, "/v0/management/request-logs?before=1767225600000")
	if response.Code != http.StatusOK || stub.deleted != 4 || stub.deleteBefore.IsZero() {
		t.Fatalf("purge status=%d body=%s before=%s", response.Code, response.Body.String(), stub.deleteBefore)
	}
	stub.deleteErr = errors.New("unexpected storage failure")
	response = performRequestLogRequest(t, handler.DeleteRequestLogs, http.MethodDelete, "/v0/management/request-logs?before=1767225600000")
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "unexpected storage failure") {
		t.Fatalf("storage failure status=%d body=%s", response.Code, response.Body.String())
	}
}

func newRequestLogTestHandler(repository RequestLogRepository) *Handler {
	handler := NewHandler(&config.Config{}, "", nil)
	handler.SetRequestLogRepository(repository)
	return handler
}

func performRequestLogRequest(t *testing.T, handler gin.HandlerFunc, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	response := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(response)
	ctx.Request = httptest.NewRequest(method, target, nil)
	handler(ctx)
	return response
}
