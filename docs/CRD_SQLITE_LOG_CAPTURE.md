# Change Requirements Document: SQLite Request Log Capture and Viewer

Status: Proposed  
Target branch: `dev-gem`  
Last updated: 2026-08-14

## 1. Summary

Add optional, native SQLite persistence for CLIProxyAPI request metadata and provide an authenticated management view for searching and inspecting those logs. The feature must remain disabled by default, must not store secrets or request/response bodies by default, and must not interfere with proxy traffic if the database is unavailable.

## 2. Problem

CLIProxyAPI can write process logs to files and expose in-memory usage statistics, but operators need durable, queryable request history across container restarts. File logs are difficult to filter by provider, model, account, status, latency, or request ID, and they do not provide a structured management UI.

The Docker deployment already persists `/CLIProxyAPI/logs`. SQLite storage should use that persistent directory by default so a normal restart, image update, or Dokploy recreation retains history.

## 3. Goals

- Persist sanitized request-level events in SQLite.
- Show recent requests and request details in the management panel.
- Filter by time, provider, model, account alias, API-key alias, status, and request ID.
- Record latency, token usage, estimated cost, retry count, and sanitized errors when available.
- Support retention, size limits, WAL mode, backup, and export.
- Keep proxy requests operational when log persistence fails.
- Preserve logs across Docker container recreation.

## 4. Non-goals

- Storing OAuth tokens, API keys, cookies, authorization headers, or management secrets.
- Capturing prompts or model responses by default.
- Replacing application/debug file logs.
- Providing billing-grade accounting in the first release.
- Synchronizing one SQLite database across multiple replicas or hosts.

## 5. Configuration

```yaml
request-log:
  enabled: false
  driver: "sqlite"
  sqlite-path: "/CLIProxyAPI/logs/requests.db"
  retention-days: 30
  max-database-mb: 2048
  cleanup-interval: "1h"
  capture-bodies: false
  capture-headers: false
  redact-errors: true
  busy-timeout: "5s"
```

Requirements:

- Relative paths are rejected; the path must be explicit.
- Startup creates the parent directory and database when writable.
- Enabling body or header capture requires a separate warning and explicit opt-in.
- Docker documentation must retain this mount:

```yaml
- ${CLI_PROXY_LOG_PATH:-./logs}:/CLIProxyAPI/logs
```

## 6. Data model

Initial table: `request_events`.

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | UUID/ULID primary key |
| `request_id` | TEXT | Correlation ID exposed to clients |
| `started_at` | INTEGER | UTC Unix milliseconds |
| `completed_at` | INTEGER | UTC Unix milliseconds |
| `duration_ms` | INTEGER | End-to-end latency |
| `method` | TEXT | HTTP method |
| `route` | TEXT | Normalized route, no query secrets |
| `provider` | TEXT | Provider type |
| `model_requested` | TEXT | Client-requested model |
| `model_resolved` | TEXT | Upstream model |
| `account_alias` | TEXT | Sanitized alias, never account tokens |
| `api_key_alias` | TEXT | Stable masked alias or key ID |
| `status_code` | INTEGER | Final HTTP status |
| `outcome` | TEXT | `success`, `client_error`, `upstream_error`, `cancelled` |
| `input_tokens` | INTEGER | Nullable |
| `output_tokens` | INTEGER | Nullable |
| `cached_tokens` | INTEGER | Nullable |
| `estimated_cost_usd` | REAL | Nullable and explicitly estimated |
| `retry_count` | INTEGER | Number of upstream retries |
| `error_class` | TEXT | Sanitized stable category |
| `error_message` | TEXT | Redacted and length-limited |
| `metadata_json` | TEXT | Versioned allow-listed metadata only |

Indexes are required on `started_at`, `request_id`, `(provider, started_at)`, `(model_resolved, started_at)`, and `(status_code, started_at)`.

Use `PRAGMA journal_mode=WAL`, `foreign_keys=ON`, and a configured `busy_timeout`. Schema changes use numbered migrations and run transactionally.

## 7. Capture pipeline

1. Middleware creates or accepts a safe request ID and records the start time.
2. Provider routing adds resolved provider, model, and sanitized account identifiers.
3. Completion adds status, token usage, retry count, latency, and redacted error data.
4. The event is sent to a bounded asynchronous writer queue.
5. The writer batches inserts into short transactions.

The logging path must be fail-open. Queue saturation, SQLite locks, disk-full errors, or migration failures must produce rate-limited operational warnings and metrics, never fail an otherwise valid proxy request.

## 8. Management API

All endpoints require the existing management authentication and remote-management policy.

- `GET /v0/management/request-logs`
  - Cursor pagination; default 50, maximum 200.
  - Filters: `from`, `to`, `provider`, `model`, `account`, `apiKey`, `status`, `outcome`, `requestId`.
- `GET /v0/management/request-logs/{id}`
  - Returns one sanitized event.
- `GET /v0/management/request-logs/summary`
  - Counts, error rate, token totals, and latency percentiles for a bounded interval.
- `GET /v0/management/request-logs/export?format=csv|jsonl`
  - Streams a bounded, sanitized export.
- `DELETE /v0/management/request-logs?before=<timestamp>`
  - Explicit administrative purge with an audit entry.

Queries must use parameters, enforce time/range limits, and return stable error objects. No endpoint may return raw stored secrets, even if legacy rows contain them.

## 9. Management UI

Add a **Request Logs** page containing:

- Summary cards for requests, failures, tokens, estimated cost, and p50/p95 latency.
- A searchable, paginated table with timestamp, provider, model, account alias, status, tokens, cost, and latency.
- Filters matching the management API.
- A detail drawer showing routing, retries, token breakdown, request ID, and sanitized failure information.
- Auto-refresh with pause control; default refresh interval of 10 seconds.
- CSV/JSONL export and retention-status indicators.
- Clear empty, disabled, database-error, and permission-denied states.

Prompts and generated content must never appear unless body capture is explicitly enabled. When enabled, the UI displays a persistent privacy warning and redacts known secret formats.

## 10. Security and privacy

- Apply an allow-list before persistence; do not rely only on redaction after capture.
- Never persist `Authorization`, cookies, OAuth tokens, API keys, management keys, or full credential filenames.
- Hash or map client API keys to administrator-defined aliases.
- Limit error text and remove bearer-like strings, JWTs, email addresses where configured, and query credentials.
- Database and backup files should be owner-readable only (`0600`).
- Management exports follow the same sanitization policy as UI responses.
- Record administrative export and purge actions without logging their credentials.

## 11. Retention and operations

- Delete expired rows in bounded batches.
- Run WAL checkpoints and space reclamation without blocking request handling.
- When the size cap is reached, remove oldest eligible rows before dropping new events.
- Expose health fields: enabled, writable, queue depth, dropped events, database size, oldest event, newest event, and last writer error.
- Document safe backup: copy with SQLite backup API or checkpoint before copying the database and WAL files.
- A multi-replica deployment requires one database per replica or an external database; shared network-filesystem SQLite is unsupported.

## 12. Compatibility and rollout

- Default remains disabled, preserving current behavior.
- Existing file logging and in-memory usage statistics continue independently.
- Phase 1: schema, writer, configuration, and health metrics.
- Phase 2: list/detail/summary APIs.
- Phase 3: management UI, export, and retention controls.
- Phase 4: optional body capture only after a dedicated security review.

## 13. Acceptance criteria

- With the feature disabled, there is no database file and no measurable request-path regression.
- With it enabled, successful and failed Claude and Codex requests appear in the UI within five seconds.
- Logs remain after container restart and recreation when the documented Docker mount is used.
- Searching by request ID returns the matching event.
- Filters and cursor pagination produce deterministic, bounded results.
- No test fixture containing bearer tokens, JWTs, cookies, or API keys is persisted or returned.
- A locked, read-only, full, or corrupt database does not interrupt proxy responses.
- Retention removes expired rows and enforces the configured size limit.
- Concurrent-load testing shows bounded memory usage and no unbounded writer queue.
- Migration, repository, API, redaction, retention, and management authorization tests pass.

## 14. Open decisions

- Whether cost tables are bundled, remotely refreshed, or configured by operators.
- Whether account aliases come from credential metadata or a separate mapping.
- Whether the UI ships in the existing management panel repository or this fork.
- Whether exports need asynchronous jobs for large date ranges.
- Whether a future PostgreSQL driver should share the repository interface introduced for SQLite.
