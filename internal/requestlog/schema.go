package requestlog

import (
	"context"
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"
)

const schemaVersion = 1

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("request-log database schema version %d is newer than supported version %d", version, schemaVersion)
	}
	if version == schemaVersion {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema migration: %w", err)
	}
	defer func() {
		if errRollback := tx.Rollback(); errRollback != nil && errRollback != sql.ErrTxDone {
			log.WithError(errRollback).Warn("rollback failed for request-log schema migration")
		}
	}()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE request_events (
id TEXT PRIMARY KEY,
request_id TEXT NOT NULL,
started_at INTEGER NOT NULL,
completed_at INTEGER NOT NULL,
duration_ms INTEGER NOT NULL,
method TEXT NOT NULL,
route TEXT NOT NULL,
provider TEXT NOT NULL,
model_requested TEXT NOT NULL,
model_resolved TEXT NOT NULL,
account_alias TEXT NOT NULL,
api_key_alias TEXT NOT NULL,
status_code INTEGER NOT NULL,
outcome TEXT NOT NULL,
input_tokens INTEGER,
output_tokens INTEGER,
cached_tokens INTEGER,
estimated_cost_usd REAL,
retry_count INTEGER NOT NULL,
error_class TEXT NOT NULL,
error_message TEXT NOT NULL,
metadata_json TEXT NOT NULL
)`); err != nil {
		return fmt.Errorf("create request_events: %w", err)
	}
	for _, statement := range []string{
		`CREATE INDEX request_events_started_at_idx ON request_events(started_at DESC, id DESC)`,
		`CREATE INDEX request_events_request_id_idx ON request_events(request_id)`,
		`CREATE INDEX request_events_provider_started_at_idx ON request_events(provider, started_at DESC)`,
		`CREATE INDEX request_events_model_resolved_started_at_idx ON request_events(model_resolved, started_at DESC)`,
		`CREATE INDEX request_events_status_started_at_idx ON request_events(status_code, started_at DESC)`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create request event index: %w", err)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (?)`, schemaVersion); err != nil {
		return fmt.Errorf("record schema migration: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migration: %w", err)
	}
	return nil
}
