package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_validConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
mongo:
  uri: "mongodb://localhost:27017"
  database: "testdb"
clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
  user: "default"
  password: ""
sync:
  mode: "oplog"
  batch_size: 500
  flush_interval_ms: 250
  resume_token_path: "/tmp/tokens"
  max_pending_rows: 50000
api:
  port: 9090
  bind: "0.0.0.0"
routing:
  clickhouse_param: "clickhouse"
logging:
  level: "debug"
`
	os.WriteFile(path, []byte(content), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Mongo.URI != "mongodb://localhost:27017" {
		t.Errorf("got URI %s", cfg.Mongo.URI)
	}
	if cfg.Mongo.Database != "testdb" {
		t.Errorf("got database %s", cfg.Mongo.Database)
	}
	if cfg.Sync.BatchSize != 500 {
		t.Errorf("got batch_size %d", cfg.Sync.BatchSize)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("got level %s", cfg.Logging.Level)
	}
}

func TestLoad_missingRequired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
mongo:
  uri: ""
  database: "testdb"
clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
sync:
  mode: "oplog"
  batch_size: 1000
  flush_interval_ms: 500
  max_pending_rows: 100000
`
	os.WriteFile(path, []byte(content), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "mongo.uri is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoad_invalidSyncMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
mongo:
  uri: "mongodb://localhost:27017"
  database: "testdb"
clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
sync:
  mode: "invalid"
  batch_size: 1000
  flush_interval_ms: 500
  max_pending_rows: 100000
`
	os.WriteFile(path, []byte(content), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "sync.mode") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoad_invalidPort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
mongo:
  uri: "mongodb://localhost:27017"
  database: "testdb"
clickhouse:
  host: "localhost"
  port: 99999
  database: "analytics"
sync:
  mode: "oplog"
  batch_size: 1000
  flush_interval_ms: 500
  max_pending_rows: 100000
`
	os.WriteFile(path, []byte(content), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "clickhouse.port") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoad_fileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_envOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
mongo:
  uri: "mongodb://localhost:27017"
  database: "testdb"
clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
sync:
  mode: "oplog"
  batch_size: 1000
  flush_interval_ms: 500
  resume_token_path: "/tmp/tokens"
  max_pending_rows: 100000
`
	os.WriteFile(path, []byte(content), 0644)

	t.Setenv("MG_CH_HOST", "override-host")
	t.Setenv("MG_CH_PORT", "9000")
	t.Setenv("MG_CH_PASSWORD", "secret123")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ClickHouse.Host != "override-host" {
		t.Errorf("expected host override, got %s", cfg.ClickHouse.Host)
	}
	if cfg.ClickHouse.Port != 9000 {
		t.Errorf("expected port override, got %d", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.Password != "secret123" {
		t.Errorf("expected password override, got %s", cfg.ClickHouse.Password)
	}
}

func TestLoad_multipleValidationErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
mongo:
  uri: ""
  database: ""
clickhouse:
  host: ""
  port: 8123
  database: "analytics"
sync:
  mode: "oplog"
  batch_size: 1000
  flush_interval_ms: 500
  resume_token_path: "/tmp/tokens"
  max_pending_rows: 100000
`
	os.WriteFile(path, []byte(content), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	// Should report all errors at once
	if !strings.Contains(err.Error(), "mongo.uri") {
		t.Errorf("expected mongo.uri error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "mongo.database") {
		t.Errorf("expected mongo.database error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "clickhouse.host") {
		t.Errorf("expected clickhouse.host error, got: %v", err)
	}
}
