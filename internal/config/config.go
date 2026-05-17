// Package config loads application configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type MongoConfig struct {
	URI      string `json:"uri"`
	Database string `json:"database"`
}

type ClickHouseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	Cluster  string `json:"cluster"`
}

type SyncConfig struct {
	Mode             string `json:"mode"`
	BatchSize        int    `json:"batch_size"`
	FlushIntervalMs  int    `json:"flush_interval_ms"`
	ResumeTokenPath  string `json:"resume_token_path"`
	MaxPendingRows   int    `json:"max_pending_rows"`
	PropagateDeletes bool   `json:"propagate_deletes"`
	DeleteColumn     string `json:"delete_column"`
}

type APIConfig struct {
	Port int    `json:"port"`
	Bind string `json:"bind"`
}

type RoutingConfig struct {
	ClickHouseParam string `json:"clickhouse_param"`
}

type LoggingConfig struct {
	Level string `json:"level"`
}

type Config struct {
	Mongo      MongoConfig      `json:"mongo"`
	ClickHouse ClickHouseConfig `json:"clickhouse"`
	Sync       SyncConfig       `json:"sync"`
	API        APIConfig        `json:"api"`
	Routing    RoutingConfig    `json:"routing"`
	Logging    LoggingConfig    `json:"logging"`
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		Mongo: MongoConfig{
			URI:      envStr("MG_MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0"),
			Database: envStr("MG_MONGO_DB", "myapp"),
		},
		ClickHouse: ClickHouseConfig{
			Host:     envStr("MG_CH_HOST", "localhost"),
			Port:     envInt("MG_CH_PORT", 8123),
			Database: envStr("MG_CH_DB", "analytics"),
			User:     envStr("MG_CH_USER", "default"),
			Password: envStr("MG_CH_PASSWORD", ""),
			Cluster:  envStr("MG_CH_CLUSTER", ""),
		},
		Sync: SyncConfig{
			Mode:             envStr("MG_SYNC_MODE", "oplog"),
			BatchSize:        envInt("MG_SYNC_BATCH_SIZE", 1000),
			FlushIntervalMs:  envInt("MG_SYNC_FLUSH_INTERVAL_MS", 500),
			ResumeTokenPath:  envStr("MG_SYNC_RESUME_TOKEN_PATH", "/var/lib/mongoflux/resume_tokens"),
			MaxPendingRows:   envInt("MG_SYNC_MAX_PENDING_ROWS", 100000),
			PropagateDeletes: envBool("MG_SYNC_PROPAGATE_DELETES", false),
			DeleteColumn:     envStr("MG_SYNC_DELETE_COLUMN", "_deleted"),
		},
		API: APIConfig{
			Port: envInt("MG_API_PORT", 9090),
			Bind: envStr("MG_API_BIND", "0.0.0.0"),
		},
		Routing: RoutingConfig{
			ClickHouseParam: envStr("MG_ROUTING_CLICKHOUSE_PARAM", "clickhouse"),
		},
		Logging: LoggingConfig{
			Level: envStr("MG_LOG_LEVEL", "info"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks all configuration invariants, reporting all errors at once.
func (c *Config) Validate() error {
	var errs []string

	if c.Mongo.URI == "" {
		errs = append(errs, "MG_MONGO_URI is required")
	}
	if c.Mongo.Database == "" {
		errs = append(errs, "MG_MONGO_DB is required")
	}
	if c.ClickHouse.Host == "" {
		errs = append(errs, "MG_CH_HOST is required")
	}
	if c.ClickHouse.Database == "" {
		errs = append(errs, "MG_CH_DB is required")
	}
	if c.ClickHouse.Port < 1 || c.ClickHouse.Port > 65535 {
		errs = append(errs, "MG_CH_PORT must be 1-65535")
	}
	if c.API.Port < 1 || c.API.Port > 65535 {
		errs = append(errs, "MG_API_PORT must be 1-65535")
	}
	if c.Sync.BatchSize < 1 || c.Sync.BatchSize > 1_000_000 {
		errs = append(errs, "MG_SYNC_BATCH_SIZE must be 1-1000000")
	}
	if c.Sync.FlushIntervalMs < 1 || c.Sync.FlushIntervalMs > 60_000 {
		errs = append(errs, "MG_SYNC_FLUSH_INTERVAL_MS must be 1-60000")
	}
	if c.Sync.MaxPendingRows < 1 || c.Sync.MaxPendingRows > 10_000_000 {
		errs = append(errs, "MG_SYNC_MAX_PENDING_ROWS must be 1-10000000")
	}
	if c.Sync.Mode != "oplog" && c.Sync.Mode != "changestream" {
		errs = append(errs, "MG_SYNC_MODE must be 'oplog' or 'changestream'")
	}
	if c.Sync.ResumeTokenPath == "" {
		errs = append(errs, "MG_SYNC_RESUME_TOKEN_PATH is required")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

func envStr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		v = strings.ToLower(v)
		return v == "true" || v == "1" || v == "yes"
	}
	return fallback
}
