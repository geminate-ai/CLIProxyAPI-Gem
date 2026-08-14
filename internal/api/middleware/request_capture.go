package middleware

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
)

const (
	maxCapturedErrorMessageRunes  = 512
	maxCapturedMetadataValueRunes = 256
)

var (
	bearerCredentialPattern = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;]+`)
	jwtPattern              = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
	secretJSONPattern       = regexp.MustCompile(`(?i)("(?:api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|cookie|password|secret)"\s*:\s*")[^"]*`)
)

// RequestCaptureDetails is retained as a convenience alias for the logging
// package's request event details type.
type RequestCaptureDetails = logging.RequestEventDetails

// SetRequestCaptureDetails attaches safe routing and usage metadata to the
// current request. Values are sanitized again immediately before capture.
func SetRequestCaptureDetails(c *gin.Context, details RequestCaptureDetails) {
	if c == nil {
		return
	}
	logging.SetGinRequestEventDetails(c, details)
}

// RequestCaptureMiddleware creates a sanitized lifecycle event for every
// non-management request. It does not read request bodies or headers. The sink
// is expected to enqueue work without blocking or returning an error.
func RequestCaptureMiddleware(sink logging.RequestEventSink) gin.HandlerFunc {
	return func(c *gin.Context) {
		if sink == nil || c == nil || c.Request == nil || !shouldCaptureRequest(c.Request) {
			c.Next()
			return
		}

		startedAt := time.Now().UTC()
		ensureCapturedRequestID(c)
		c.Next()

		completedAt := time.Now().UTC()
		sink.CaptureRequestEvent(buildRequestEvent(c, startedAt, completedAt))
	}
}

func shouldCaptureRequest(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	return !strings.HasPrefix(req.URL.Path, "/v0/management") && !strings.HasPrefix(req.URL.Path, "/management")
}

func ensureCapturedRequestID(c *gin.Context) {
	if logging.GetGinRequestID(c) != "" {
		return
	}
	requestID := generateRequestEventID()
	logging.SetGinRequestID(c, requestID)
	c.Request = c.Request.WithContext(logging.WithRequestID(c.Request.Context(), requestID))
}

func buildRequestEvent(c *gin.Context, startedAt, completedAt time.Time) logging.RequestEvent {
	statusCode := http.StatusOK
	if c != nil && c.Writer != nil && c.Writer.Status() > 0 {
		statusCode = c.Writer.Status()
	}
	details := requestCaptureDetails(c)
	errorClass, errorMessage := requestCaptureError(c, statusCode)
	event := logging.RequestEvent{
		ID:               generateRequestEventID(),
		RequestID:        logging.GetGinRequestID(c),
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
		DurationMS:       completedAt.Sub(startedAt).Milliseconds(),
		Method:           safeCaptureValue(c.Request.Method, maxCapturedMetadataValueRunes),
		Route:            safeCaptureRoute(c.Request.URL.Path),
		Provider:         safeCaptureValue(details.Provider, maxCapturedMetadataValueRunes),
		ModelRequested:   safeCaptureValue(details.ModelRequested, maxCapturedMetadataValueRunes),
		ModelResolved:    safeCaptureValue(details.ModelResolved, maxCapturedMetadataValueRunes),
		AccountAlias:     safeCaptureValue(details.AccountAlias, maxCapturedMetadataValueRunes),
		APIKeyAlias:      requestCaptureAPIKeyAlias(c),
		StatusCode:       statusCode,
		Outcome:          requestCaptureOutcome(statusCode, c != nil && c.Request != nil && c.Request.Context().Err() != nil),
		InputTokens:      details.InputTokens,
		OutputTokens:     details.OutputTokens,
		CachedTokens:     details.CachedTokens,
		EstimatedCostUSD: details.EstimatedCostUSD,
		RetryCount:       details.RetryCount,
		ErrorClass:       errorClass,
		ErrorMessage:     errorMessage,
		Metadata:         safeCaptureMetadata(details.Metadata),
	}
	return event
}

func requestCaptureDetails(c *gin.Context) RequestCaptureDetails {
	return logging.GetGinRequestEventDetails(c)
}

func requestCaptureAPIKeyAlias(c *gin.Context) string {
	if c == nil {
		return ""
	}
	value, ok := c.Get("userApiKey")
	if !ok {
		return ""
	}
	principal, ok := value.(string)
	principal = strings.TrimSpace(principal)
	if !ok || principal == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(principal))
	return "sha256:" + hex.EncodeToString(digest[:6])
}

func requestCaptureOutcome(statusCode int, cancelled bool) string {
	if cancelled {
		return "cancelled"
	}
	switch {
	case statusCode >= http.StatusInternalServerError:
		return "upstream_error"
	case statusCode >= http.StatusBadRequest:
		return "client_error"
	default:
		return "success"
	}
}

func requestCaptureError(c *gin.Context, statusCode int) (string, string) {
	if c != nil && c.Errors != nil && len(c.Errors) > 0 {
		err := c.Errors.Last()
		if err != nil {
			return "handler_error", sanitizeCaptureError(err.Error())
		}
	}
	if statusCode >= http.StatusInternalServerError {
		return "server_error", ""
	}
	if statusCode >= http.StatusBadRequest {
		return "client_error", ""
	}
	return "", ""
}

func safeCaptureRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "/"
	}
	if index := strings.IndexByte(route, '?'); index >= 0 {
		route = route[:index]
	}
	return safeCaptureValue(route, maxCapturedMetadataValueRunes)
}

func safeCaptureMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		key = safeCaptureValue(key, 64)
		if key == "" || looksSensitiveCaptureKey(key) {
			continue
		}
		result[key] = safeCaptureValue(value, maxCapturedMetadataValueRunes)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func safeCaptureValue(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	if utf8RuneCount(value) <= maxRunes {
		return value
	}
	return string([]rune(value)[:maxRunes])
}

func sanitizeCaptureError(message string) string {
	message = safeCaptureValue(message, maxCapturedErrorMessageRunes)
	message = bearerCredentialPattern.ReplaceAllString(message, "Bearer [REDACTED]")
	message = jwtPattern.ReplaceAllString(message, "[REDACTED_JWT]")
	return secretJSONPattern.ReplaceAllString(message, "$1[REDACTED]")
}

func looksSensitiveCaptureKey(key string) bool {
	key = strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	return strings.Contains(key, "token") || strings.Contains(key, "secret") || strings.Contains(key, "password") || strings.Contains(key, "authorization") || strings.Contains(key, "cookie") || strings.Contains(key, "api_key")
}

func generateRequestEventID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	return hex.EncodeToString([]byte(time.Now().UTC().Format("20060102150405.000000000")))
}

func utf8RuneCount(value string) int {
	return len([]rune(value))
}
