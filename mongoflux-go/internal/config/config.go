package config

import (
	"fmt"
	"os"

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

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Mongo.URI == "" {
		return fmt.Errorf("config validation: mongo.uri is required")
	}
	if c.Mongo.Database == "" {
		return fmt.Errorf("config validation: mongo.database is required")
	}
	if c.ClickHouse.Host == "" {
		return fmt.Errorf("config validation: clickhouse.host is required")
	}
	if c.ClickHouse.Database == "" {
		return fmt.Errorf("config validation: clickhouse.database is required")
	}
	if c.ClickHouse.Port <= 0 || c.ClickHouse.Port > 65535 {
		return fmt.Errorf("config validation: clickhouse.port must be 1-65535")
	}
	if c.API.Port <= 0 || c.API.Port > 65535 {
		return fmt.Errorf("config validation: api.port must be 1-65535")
	}
	if c.Sync.BatchSize <= 0 || c.Sync.BatchSize > 1000000 {
		return fmt.Errorf("config validation: sync.batch_size must be 1-1000000")
	}
	if c.Sync.FlushIntervalMs <= 0 || c.Sync.FlushIntervalMs > 60000 {
		return fmt.Errorf("config validation: sync.flush_interval_ms must be 1-60000")
	}
	if c.Sync.MaxPendingRows <= 0 || c.Sync.MaxPendingRows > 10000000 {
		return fmt.Errorf("config validation: sync.max_pending_rows must be 1-10000000")
	}
	if c.Sync.Mode != "oplog" && c.Sync.Mode != "changestream" {
		return fmt.Errorf("config validation: sync.mode must be 'oplog' or 'changestream'")
	}
	return nil
}
