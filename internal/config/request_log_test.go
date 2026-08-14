package config

import "testing"

func TestRequestLogStoreConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte("request-log-store:\n  enabled: true\n"))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if !cfg.RequestLogStore.Enabled || cfg.RequestLogStore.SQLitePath != DefaultRequestLogSQLitePath || !cfg.RequestLogStore.RedactErrors {
		t.Fatalf("request-log-store defaults = %#v", cfg.RequestLogStore)
	}
	if cfg.SDKConfig.RequestLog {
		t.Fatal("request-log-store must not change existing request-log boolean")
	}
}

func TestRequestLogStoreRejectsInvalidEnabledConfiguration(t *testing.T) {
	for _, value := range []string{
		"request-log-store:\n  enabled: true\n  driver: postgres\n",
		"request-log-store:\n  enabled: true\n  sqlite-path: relative.db\n",
		"request-log-store:\n  enabled: true\n  busy-timeout: nope\n",
		"request-log-store:\n  enabled: true\n  capture-bodies: true\n",
		"request-log-store:\n  enabled: true\n  capture-headers: true\n",
	} {
		if _, err := ParseConfigBytes([]byte(value)); err == nil {
			t.Fatalf("ParseConfigBytes(%q) accepted invalid request-log-store config", value)
		}
	}
}
