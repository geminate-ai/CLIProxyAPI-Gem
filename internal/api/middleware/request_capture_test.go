package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
)

type captureSink struct {
	mu     sync.Mutex
	events []logging.RequestEvent
}

func (s *captureSink) CaptureRequestEvent(event logging.RequestEvent) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *captureSink) event(t *testing.T) logging.RequestEvent {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) != 1 {
		t.Fatalf("captured event count = %d, want 1", len(s.events))
	}
	return s.events[0]
}

func TestRequestCaptureMiddlewareCapturesSanitizedLifecycleEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := &captureSink{}
	router := gin.New()
	router.Use(RequestCaptureMiddleware(sink))
	router.POST("/v1/chat/completions", func(c *gin.Context) {
		c.Set("userApiKey", "sk-this-must-not-be-persisted")
		inputTokens := int64(12)
		SetRequestCaptureDetails(c, RequestCaptureDetails{
			Provider:       "codex",
			ModelRequested: "gpt-5.4",
			ModelResolved:  "gpt-5.4-codex",
			AccountAlias:   "work-account",
			InputTokens:    &inputTokens,
			Metadata: map[string]string{
				"region":        "us-east-1",
				"authorization": "Bearer must-not-persist",
			},
		})
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?api_key=must-not-persist", strings.NewReader(`{"model":"gpt-5.4"}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	event := sink.event(t)
	if event.ID == "" || event.RequestID == "" {
		t.Fatalf("event IDs must be populated: %#v", event)
	}
	if event.Method != http.MethodPost || event.Route != "/v1/chat/completions" {
		t.Fatalf("event route = %s %s", event.Method, event.Route)
	}
	if event.StatusCode != http.StatusOK || event.Outcome != "success" {
		t.Fatalf("event outcome = %d %q", event.StatusCode, event.Outcome)
	}
	if event.Provider != "codex" || event.ModelRequested != "gpt-5.4" || event.ModelResolved != "gpt-5.4-codex" {
		t.Fatalf("event routing metadata = %#v", event)
	}
	if event.AccountAlias != "work-account" || event.InputTokens == nil || *event.InputTokens != 12 {
		t.Fatalf("event account/usage = %#v", event)
	}
	if event.APIKeyAlias == "" || strings.Contains(event.APIKeyAlias, "sk-this") {
		t.Fatalf("event API key alias = %q", event.APIKeyAlias)
	}
	if event.Metadata["region"] != "us-east-1" {
		t.Fatalf("event metadata = %#v", event.Metadata)
	}
	if _, exists := event.Metadata["authorization"]; exists {
		t.Fatalf("sensitive metadata was retained: %#v", event.Metadata)
	}
}

func TestRequestCaptureMiddlewareSkipsManagementAndSanitizesErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sink := &captureSink{}
	router := gin.New()
	router.Use(RequestCaptureMiddleware(sink))
	router.GET("/v0/management/config", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.GET("/v1/models", func(c *gin.Context) {
		_ = c.Error(http.ErrAbortHandler)
		c.Status(http.StatusInternalServerError)
	})

	managementResponse := httptest.NewRecorder()
	router.ServeHTTP(managementResponse, httptest.NewRequest(http.MethodGet, "/v0/management/config", nil))

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	event := sink.event(t)
	if event.StatusCode != http.StatusInternalServerError || event.Outcome != "upstream_error" {
		t.Fatalf("event outcome = %d %q", event.StatusCode, event.Outcome)
	}
	if event.ErrorClass == "" {
		t.Fatal("expected error class")
	}
}

func TestSanitizeCaptureErrorRedactsCredentials(t *testing.T) {
	message := sanitizeCaptureError(`request failed Authorization: Bearer token-value eyJhbGciOiJIUzI1NiJ9.payload.signature {"api_key":"secret-value"}`)
	for _, secret := range []string{"token-value", "eyJhbGciOiJIUzI1NiJ9.payload.signature", "secret-value"} {
		if strings.Contains(message, secret) {
			t.Fatalf("error message leaked %q: %q", secret, message)
		}
	}
}
