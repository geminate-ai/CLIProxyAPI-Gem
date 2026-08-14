package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultRequestLogDriver          = "sqlite"
	DefaultRequestLogSQLitePath      = "/CLIProxyAPI/logs/requests.db"
	DefaultRequestLogRetentionDays   = 30
	DefaultRequestLogMaxDatabaseMB   = 2048
	DefaultRequestLogCleanupInterval = "1h"
	DefaultRequestLogBusyTimeout     = "5s"
)

// RequestLogConfig configures durable, sanitized request-event persistence.
// Capture of request and response bodies is deliberately disabled by default.
type RequestLogConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	Driver          string `yaml:"driver" json:"driver"`
	SQLitePath      string `yaml:"sqlite-path" json:"sqlite-path"`
	RetentionDays   int    `yaml:"retention-days" json:"retention-days"`
	MaxDatabaseMB   int    `yaml:"max-database-mb" json:"max-database-mb"`
	CleanupInterval string `yaml:"cleanup-interval" json:"cleanup-interval"`
	CaptureBodies   bool   `yaml:"capture-bodies" json:"capture-bodies"`
	CaptureHeaders  bool   `yaml:"capture-headers" json:"capture-headers"`
	RedactErrors    bool   `yaml:"redact-errors" json:"redact-errors"`
	BusyTimeout     string `yaml:"busy-timeout" json:"busy-timeout"`
}

// DefaultRequestLogConfig returns the request-log defaults used by config loading.
func DefaultRequestLogConfig() RequestLogConfig {
	return RequestLogConfig{
		Driver:          DefaultRequestLogDriver,
		SQLitePath:      DefaultRequestLogSQLitePath,
		RetentionDays:   DefaultRequestLogRetentionDays,
		MaxDatabaseMB:   DefaultRequestLogMaxDatabaseMB,
		CleanupInterval: DefaultRequestLogCleanupInterval,
		RedactErrors:    true,
		BusyTimeout:     DefaultRequestLogBusyTimeout,
	}
}

// Validate verifies request-log configuration intrinsic to this feature.
func (c RequestLogConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(c.Driver) != DefaultRequestLogDriver {
		return fmt.Errorf("request-log.driver must be %q", DefaultRequestLogDriver)
	}
	path := strings.TrimSpace(c.SQLitePath)
	if path == "" || !filepath.IsAbs(path) {
		return fmt.Errorf("request-log.sqlite-path must be an absolute path")
	}
	if c.RetentionDays < 0 {
		return fmt.Errorf("request-log.retention-days must be zero or greater")
	}
	if c.MaxDatabaseMB < 0 {
		return fmt.Errorf("request-log.max-database-mb must be zero or greater")
	}
	if c.CaptureBodies || c.CaptureHeaders {
		return fmt.Errorf("request-log body and header capture are not supported")
	}
	if _, err := time.ParseDuration(c.CleanupInterval); err != nil {
		return fmt.Errorf("request-log.cleanup-interval: %w", err)
	}
	busyTimeout, err := time.ParseDuration(c.BusyTimeout)
	if err != nil {
		return fmt.Errorf("request-log.busy-timeout: %w", err)
	}
	if busyTimeout < 0 {
		return fmt.Errorf("request-log.busy-timeout must be zero or greater")
	}
	return nil
}
