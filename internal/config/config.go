// Package config handles YAML configuration loading, validation, and environment variable overrides.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type MongoConfig struct {
	URI      string `yaml:"uri" json:"uri"`
	Database string `yaml:"database" json:"database"`
}

type ClickHouseConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Database string `yaml:"database" json:"database"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	Cluster  string `yaml:"cluster" json:"cluster"`
}

type SyncConfig struct {
	Mode             string `yaml:"mode" json:"mode"`
	BatchSize        int    `yaml:"batch_size" json:"batch_size"`
	FlushIntervalMs  int    `yaml:"flush_interval_ms" json:"flush_interval_ms"`
	ResumeTokenPath  string `yaml:"resume_token_path" json:"resume_token_path"`
	MaxPendingRows   int    `yaml:"max_pending_rows" json:"max_pending_rows"`
	PropagateDeletes bool   `yaml:"propagate_deletes" json:"propagate_deletes"`
	DeleteColumn     string `yaml:"delete_column" json:"delete_column"`
}

type APIConfig struct {
	Port int    `yaml:"port" json:"port"`
	Bind string `yaml:"bind" json:"bind"`
}

type RoutingConfig struct {
	ClickHouseParam string `yaml:"clickhouse_param" json:"clickhouse_param"`
}

type LoggingConfig struct {
	Level string `yaml:"level" json:"level"`
	File  string `yaml:"file" json:"file"`
}

type Config struct {
	Mongo      MongoConfig      `yaml:"mongo" json:"mongo"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse" json:"clickhouse"`
	Sync       SyncConfig       `yaml:"sync" json:"sync"`
	API        APIConfig        `yaml:"api" json:"api"`
	Routing    RoutingConfig    `yaml:"routing" json:"routing"`
	Logging    LoggingConfig    `yaml:"logging" json:"logging"`
}

// Load reads config from YAML, applies environment variable overrides, and validates.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", path, err)
	}

	cfg := &Config{
		Mongo:      MongoConfig{URI: "mongodb://localhost:27017", Database: "myapp"},
		ClickHouse: ClickHouseConfig{Host: "localhost", Port: 8123, Database: "analytics", User: "default"},
		Sync:       SyncConfig{Mode: "oplog", BatchSize: 1000, FlushIntervalMs: 500, ResumeTokenPath: "/var/lib/mongoflux/resume_tokens", MaxPendingRows: 100000, DeleteColumn: "_deleted"},
		API:        APIConfig{Port: 9090, Bind: "0.0.0.0"},
		Routing:    RoutingConfig{ClickHouseParam: "clickhouse"},
		Logging:    LoggingConfig{Level: "info"},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file '%s': %w", path, err)
	}

	applyEnvOverrides(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks all configuration invariants, reporting all errors at once.
func (c *Config) Validate() error {
	var errs []string

	if c.Mongo.URI == "" {
		errs = append(errs, "mongo.uri is required")
	}
	if c.Mongo.Database == "" {
		errs = append(errs, "mongo.database is required")
	}
	if c.ClickHouse.Host == "" {
		errs = append(errs, "clickhouse.host is required")
	}
	if c.ClickHouse.Database == "" {
		errs = append(errs, "clickhouse.database is required")
	}
	if c.ClickHouse.Port <= 0 || c.ClickHouse.Port > 65535 {
		errs = append(errs, "clickhouse.port must be 1-65535")
	}
	if c.API.Port <= 0 || c.API.Port > 65535 {
		errs = append(errs, "api.port must be 1-65535")
	}
	if c.Sync.BatchSize <= 0 || c.Sync.BatchSize > 1000000 {
		errs = append(errs, "sync.batch_size must be 1-1000000")
	}
	if c.Sync.FlushIntervalMs <= 0 || c.Sync.FlushIntervalMs > 60000 {
		errs = append(errs, "sync.flush_interval_ms must be 1-60000")
	}
	if c.Sync.MaxPendingRows <= 0 || c.Sync.MaxPendingRows > 10000000 {
		errs = append(errs, "sync.max_pending_rows must be 1-10000000")
	}
	if c.Sync.Mode != "oplog" && c.Sync.Mode != "changestream" {
		errs = append(errs, "sync.mode must be 'oplog' or 'changestream'")
	}
	if c.Sync.ResumeTokenPath == "" {
		errs = append(errs, "sync.resume_token_path is required")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// applyEnvOverrides applies MG_ prefixed environment variable overrides.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("MG_MONGO_URI"); v != "" {
		cfg.Mongo.URI = v
	}
	if v := os.Getenv("MG_MONGO_DB"); v != "" {
		cfg.Mongo.Database = v
	}
	if v := os.Getenv("MG_CH_HOST"); v != "" {
		cfg.ClickHouse.Host = v
	}
	if v := os.Getenv("MG_CH_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.ClickHouse.Port = p
		}
	}
	if v := os.Getenv("MG_CH_DB"); v != "" {
		cfg.ClickHouse.Database = v
	}
	if v := os.Getenv("MG_CH_USER"); v != "" {
		cfg.ClickHouse.User = v
	}
	if v := os.Getenv("MG_CH_PASSWORD"); v != "" {
		cfg.ClickHouse.Password = v
	}
}
