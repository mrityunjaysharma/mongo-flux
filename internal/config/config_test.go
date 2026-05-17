package config

import (
	"strings"
	"testing"
)

func TestLoad_defaults(t *testing.T) {
	// Clear any env vars that might interfere
	t.Setenv("MG_MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
	t.Setenv("MG_MONGO_DB", "testdb")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Mongo.Database != "testdb" {
		t.Errorf("got database %s", cfg.Mongo.Database)
	}
	if cfg.ClickHouse.Port != 8123 {
		t.Errorf("got CH port %d", cfg.ClickHouse.Port)
	}
	if cfg.Sync.BatchSize != 1000 {
		t.Errorf("got batch_size %d", cfg.Sync.BatchSize)
	}
	if cfg.API.Port != 9090 {
		t.Errorf("got api port %d", cfg.API.Port)
	}
}

func TestLoad_envOverrides(t *testing.T) {
	t.Setenv("MG_MONGO_URI", "mongodb://custom:27017")
	t.Setenv("MG_MONGO_DB", "mydb")
	t.Setenv("MG_CH_HOST", "ch-server")
	t.Setenv("MG_CH_PORT", "9000")
	t.Setenv("MG_CH_PASSWORD", "secret123")
	t.Setenv("MG_SYNC_BATCH_SIZE", "5000")
	t.Setenv("MG_API_PORT", "8080")
	t.Setenv("MG_LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Mongo.URI != "mongodb://custom:27017" {
		t.Errorf("got URI %s", cfg.Mongo.URI)
	}
	if cfg.ClickHouse.Host != "ch-server" {
		t.Errorf("got host %s", cfg.ClickHouse.Host)
	}
	if cfg.ClickHouse.Port != 9000 {
		t.Errorf("got port %d", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.Password != "secret123" {
		t.Errorf("got password %s", cfg.ClickHouse.Password)
	}
	if cfg.Sync.BatchSize != 5000 {
		t.Errorf("got batch_size %d", cfg.Sync.BatchSize)
	}
	if cfg.API.Port != 8080 {
		t.Errorf("got api port %d", cfg.API.Port)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("got level %s", cfg.Logging.Level)
	}
}

func TestLoad_invalidSyncMode(t *testing.T) {
	t.Setenv("MG_MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MG_MONGO_DB", "test")
	t.Setenv("MG_SYNC_MODE", "invalid")

	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "MG_SYNC_MODE") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoad_invalidPort(t *testing.T) {
	t.Setenv("MG_MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MG_MONGO_DB", "test")
	t.Setenv("MG_CH_PORT", "99999")

	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "MG_CH_PORT") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoad_missingRequired(t *testing.T) {
	t.Setenv("MG_MONGO_URI", "")
	t.Setenv("MG_MONGO_DB", "")

	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error")
	}
	// Should report multiple errors
	if !strings.Contains(err.Error(), "MG_MONGO_URI") {
		t.Errorf("expected MG_MONGO_URI error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "MG_MONGO_DB") {
		t.Errorf("expected MG_MONGO_DB error, got: %v", err)
	}
}

func TestLoad_boolParsing(t *testing.T) {
	t.Setenv("MG_MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("MG_MONGO_DB", "test")
	t.Setenv("MG_SYNC_PROPAGATE_DELETES", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Sync.PropagateDeletes {
		t.Error("expected PropagateDeletes to be true")
	}
}
