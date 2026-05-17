package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mongoflux/mongoflux/internal/api"
	"github.com/mongoflux/mongoflux/internal/clickhouse"
	"github.com/mongoflux/mongoflux/internal/config"
	"github.com/mongoflux/mongoflux/internal/schema"
	mfsync "github.com/mongoflux/mongoflux/internal/sync"
)

func main() {
	configPath := "/etc/mongoflux/mongoflux.yaml"
	if len(os.Args) > 1 {
		arg := os.Args[1]
		if arg == "-h" || arg == "--help" {
			fmt.Fprintf(os.Stderr, "Usage: %s [config_path]\n  config_path: Path to mongoflux.yaml (default: /etc/mongoflux/mongoflux.yaml)\n", os.Args[0])
			os.Exit(0)
		}
		configPath = arg
	}

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	// Initialize schema registry
	registry := schema.NewRegistry()
	mappingsFile := filepath.Join(cfg.Sync.ResumeTokenPath, "mappings.json")
	if err := registry.LoadFromFile(mappingsFile); err != nil {
		slog.Warn("Could not load mappings", "error", err)
	} else {
		slog.Info("Loaded schema mappings", "count", len(registry.GetAll()))
	}

	// ClickHouse client
	chClient := clickhouse.NewClient(cfg.ClickHouse)
	if err := chClient.Ping(); err != nil {
		slog.Warn("ClickHouse not reachable", "error", err)
	} else {
		slog.Info("Connected to ClickHouse", "host", cfg.ClickHouse.Host, "port", cfg.ClickHouse.Port)
	}

	// Sync engines
	csSync := mfsync.NewChangeStreamSync(cfg, registry, chClient)
	oplogSync := mfsync.NewOplogSync(cfg, registry, chClient)

	// Management API
	mgmtAPI := api.NewManagementAPI(cfg.API, registry, chClient, csSync, oplogSync)

	// Start sync based on configured mode
	if cfg.Sync.Mode == "oplog" {
		slog.Info("Starting oplog sync")
		oplogSync.Start()
	} else {
		slog.Info("Starting change stream sync")
		csSync.Start()
	}

	slog.Info("Starting management API", "port", cfg.API.Port)
	mgmtAPI.Start()

	slog.Info("MongoFlux is running. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	slog.Info("Shutting down...")
	mgmtAPI.Stop()
	oplogSync.Stop()
	csSync.Stop()

	// Persist mappings
	if err := registry.SaveToFile(mappingsFile); err != nil {
		slog.Warn("Failed to save mappings", "error", err)
	}

	slog.Info("Shutdown complete.")
}
