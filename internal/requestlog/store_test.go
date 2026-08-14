package requestlog

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestStoreCapturesSanitizesAndQueriesEvents(t *testing.T) {
	store := newTestStore(t, Options{RedactErrors: true})
	input, output := int64(12), int64(7)
	cost := 0.125
	started := time.Now().UTC().Add(-time.Minute)
	store.Capture(Event{
		ID: "event-1", RequestID: "request-1", StartedAt: started, CompletedAt: started.Add(42 * time.Millisecond), DurationMS: 42,
		Method: "post", Route: "/v1/chat/completions?api_key=must-not-persist", Provider: "claude", ModelRequested: "claude-test",
		ModelResolved: "claude-test-upstream", AccountAlias: "claude-account", APIKeyAlias: "key-1", StatusCode: 200, Outcome: "success",
		InputTokens: &input, OutputTokens: &output, EstimatedCostUSD: &cost, Metadata: map[string]string{"stream": "true", "authorization": "Bearer bad"},
	})
	store.Capture(Event{
		ID: "event-2", RequestID: "request-2", StartedAt: started.Add(time.Millisecond), CompletedAt: started.Add(101 * time.Millisecond), DurationMS: 100,
		Method: "GET", Route: "/v1/models", Provider: "codex", ModelResolved: "gpt-test", StatusCode: 502, Outcome: "upstream_error",
		ErrorMessage: "upstream rejected Bearer abc.def and eyJabc.def.ghi",
	})

	page := waitForEvents(t, store, 2)
	if page.Events[0].ID != "event-2" || page.Events[1].ID != "event-1" {
		t.Fatalf("event order = %#v", page.Events)
	}
	if page.Events[1].Route != "/v1/chat/completions" {
		t.Fatalf("route = %q, want query stripped", page.Events[1].Route)
	}
	if page.Events[1].Metadata["stream"] != "true" || len(page.Events[1].Metadata) != 1 {
		t.Fatalf("metadata = %#v, want allow-listed metadata only", page.Events[1].Metadata)
	}
	if strings.Contains(page.Events[0].ErrorMessage, "abc.def") || strings.Contains(page.Events[0].ErrorMessage, "eyJ") {
		t.Fatalf("error message was not redacted: %q", page.Events[0].ErrorMessage)
	}

	filtered, err := store.List(context.Background(), ListFilter{Provider: "claude", Limit: 5})
	if err != nil || len(filtered.Events) != 1 || filtered.Events[0].ID != "event-1" {
		t.Fatalf("filtered list = %#v, %v", filtered, err)
	}
	got, err := store.Get(context.Background(), "event-1")
	if err != nil || got.RequestID != "request-1" || got.InputTokens == nil || *got.InputTokens != input {
		t.Fatalf("get event = %#v, %v", got, err)
	}
	summary, err := store.Summary(context.Background(), SummaryFilter{From: started.Add(-time.Second), To: time.Now().Add(time.Second)})
	if err != nil || summary.Requests != 2 || summary.Failures != 1 || summary.InputTokens != input || summary.OutputTokens != output {
		t.Fatalf("summary = %#v, %v", summary, err)
	}
	var exported []string
	err = store.Export(context.Background(), ExportFilter{}, func(event Event) error {
		exported = append(exported, event.ID)
		return nil
	})
	if err != nil || len(exported) != 2 || exported[0] != "event-1" {
		t.Fatalf("exported = %#v, %v", exported, err)
	}
}

func TestStoreCursorPurgeHealthAndPersistence(t *testing.T) {
	path := t.TempDir() + "/requests.db"
	store, err := Open(context.Background(), Options{Path: path, CleanupInterval: time.Hour, BusyTimeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if errClose := store.Close(); errClose != nil {
			t.Errorf("close store: %v", errClose)
		}
	}()
	base := time.Now().UTC().Add(-time.Hour)
	for index := 0; index < 3; index++ {
		store.Capture(Event{ID: string(rune('a' + index)), StartedAt: base.Add(time.Duration(index) * time.Second), CompletedAt: base, Outcome: "success"})
	}
	waitForEvents(t, store, 3)
	first, err := store.List(context.Background(), ListFilter{Limit: 2})
	if err != nil || len(first.Events) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := store.List(context.Background(), ListFilter{Limit: 2, Cursor: first.NextCursor})
	if err != nil || len(second.Events) != 1 || second.Events[0].ID != "a" {
		t.Fatalf("second page = %#v, %v", second, err)
	}
	health, err := store.Health(context.Background())
	if err != nil || !health.Enabled || !health.Writable || health.DatabaseBytes == 0 || health.OldestEvent == nil || health.NewestEvent == nil {
		t.Fatalf("health = %#v, %v", health, err)
	}
	deleted, err := store.Purge(context.Background(), base.Add(1500*time.Millisecond))
	if err != nil || deleted != 2 {
		t.Fatalf("purge = %d, %v", deleted, err)
	}
	remaining, err := store.List(context.Background(), ListFilter{})
	if err != nil || len(remaining.Events) != 1 || remaining.Events[0].ID != "c" {
		t.Fatalf("remaining = %#v, %v", remaining, err)
	}
}

func TestStoreCleanupAppliesRetention(t *testing.T) {
	store := newTestStore(t, Options{RetentionDays: 1})
	store.Capture(Event{ID: "old", StartedAt: time.Now().AddDate(0, 0, -2), CompletedAt: time.Now().AddDate(0, 0, -2)})
	store.Capture(Event{ID: "new", StartedAt: time.Now(), CompletedAt: time.Now()})
	waitForEvents(t, store, 2)
	if err := store.cleanup(context.Background()); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	page, err := store.List(context.Background(), ListFilter{})
	if err != nil || len(page.Events) != 1 || page.Events[0].ID != "new" {
		t.Fatalf("post-cleanup events = %#v, %v", page.Events, err)
	}
}

func newTestStore(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.Path == "" {
		opts.Path = t.TempDir() + "/requests.db"
	}
	if opts.CleanupInterval == 0 {
		opts.CleanupInterval = time.Hour
	}
	store, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if errClose := store.Close(); errClose != nil {
			t.Errorf("close store: %v", errClose)
		}
	})
	return store
}

func waitForEvents(t *testing.T, store *Store, count int) ListResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		page, err := store.List(context.Background(), ListFilter{Limit: maxListLimit})
		if err == nil && len(page.Events) == count {
			return page
		}
		time.Sleep(10 * time.Millisecond)
	}
	page, err := store.List(context.Background(), ListFilter{Limit: maxListLimit})
	t.Fatalf("timed out waiting for %d events; got %#v, err %v", count, page, err)
	return ListResult{}
}
