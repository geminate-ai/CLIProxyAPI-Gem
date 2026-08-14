package logging

import (
	"time"

	"github.com/gin-gonic/gin"
)

const ginRequestEventDetailsKey = "__request_event_details__"

// RequestEvent is the allow-listed request lifecycle data that may be persisted
// by an optional request event sink. It deliberately excludes request and
// response bodies, headers, credentials, and arbitrary context values.
type RequestEvent struct {
	ID               string
	RequestID        string
	StartedAt        time.Time
	CompletedAt      time.Time
	DurationMS       int64
	Method           string
	Route            string
	Provider         string
	ModelRequested   string
	ModelResolved    string
	AccountAlias     string
	APIKeyAlias      string
	StatusCode       int
	Outcome          string
	InputTokens      *int64
	OutputTokens     *int64
	CachedTokens     *int64
	EstimatedCostUSD *float64
	RetryCount       int
	ErrorClass       string
	ErrorMessage     string
	Metadata         map[string]string
}

// RequestEventSink accepts sanitized request lifecycle events. Implementations
// must be fail-open and return promptly because they are called on the request
// path. A bounded asynchronous queue is the expected implementation strategy.
type RequestEventSink interface {
	CaptureRequestEvent(RequestEvent)
}

// RequestEventDetails is allow-listed routing and usage metadata that request
// handling code may attach to the event being captured. Identity fields must be
// aliases, never credential values or credential filenames.
type RequestEventDetails struct {
	Provider         string
	ModelRequested   string
	ModelResolved    string
	AccountAlias     string
	InputTokens      *int64
	OutputTokens     *int64
	CachedTokens     *int64
	EstimatedCostUSD *float64
	RetryCount       int
	Metadata         map[string]string
}

// SetGinRequestEventDetails replaces the current request event details.
func SetGinRequestEventDetails(c *gin.Context, details RequestEventDetails) {
	if c != nil {
		c.Set(ginRequestEventDetailsKey, details)
	}
}

// MergeGinRequestEventDetails merges non-empty details into the current event.
// It is safe to call for every upstream retry; later routing details take
// precedence and RetryCount keeps the highest observed value.
func MergeGinRequestEventDetails(c *gin.Context, details RequestEventDetails) {
	if c == nil {
		return
	}
	current := GetGinRequestEventDetails(c)
	if details.Provider != "" {
		current.Provider = details.Provider
	}
	if details.ModelRequested != "" {
		current.ModelRequested = details.ModelRequested
	}
	if details.ModelResolved != "" {
		current.ModelResolved = details.ModelResolved
	}
	if details.AccountAlias != "" {
		current.AccountAlias = details.AccountAlias
	}
	if details.InputTokens != nil {
		current.InputTokens = details.InputTokens
	}
	if details.OutputTokens != nil {
		current.OutputTokens = details.OutputTokens
	}
	if details.CachedTokens != nil {
		current.CachedTokens = details.CachedTokens
	}
	if details.EstimatedCostUSD != nil {
		current.EstimatedCostUSD = details.EstimatedCostUSD
	}
	if details.RetryCount > current.RetryCount {
		current.RetryCount = details.RetryCount
	}
	if len(details.Metadata) > 0 {
		if current.Metadata == nil {
			current.Metadata = make(map[string]string, len(details.Metadata))
		}
		for key, value := range details.Metadata {
			current.Metadata[key] = value
		}
	}
	SetGinRequestEventDetails(c, current)
}

// GetGinRequestEventDetails returns the current request event details.
func GetGinRequestEventDetails(c *gin.Context) RequestEventDetails {
	if c == nil {
		return RequestEventDetails{}
	}
	value, exists := c.Get(ginRequestEventDetailsKey)
	if !exists {
		return RequestEventDetails{}
	}
	details, _ := value.(RequestEventDetails)
	return details
}
