package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mongoflux/mongoflux/internal/api"
	"github.com/mongoflux/mongoflux/internal/clickhouse"
	"github.com/mongoflux/mongoflux/internal/config"
	"github.com/mongoflux/mongoflux/internal/schema"
	mfsync "github.com/mongoflux/mongoflux/internal/sync"
	"github.com/mongoflux/mongoflux/internal/translator"
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
		log.Fatalf("[mongoflux] Failed to load config: %v", err)
	}

	// Initialize schema registry
	registry := schema.NewRegistry()
	mappingsFile := filepath.Join(cfg.Sync.ResumeTokenPath, "mappings.json")
	if err := registry.LoadFromFile(mappingsFile); err != nil {
		log.Printf("[mongoflux] Warning: %v", err)
	} else {
		log.Printf("[mongoflux] Loaded %d schema mappings", len(registry.GetAll()))
	}

	// ClickHouse client
	chClient := clickhouse.NewClient(cfg.ClickHouse)
	if err := chClient.Ping(); err != nil {
		log.Printf("[mongoflux] Warning: ClickHouse not reachable: %v", err)
	} else {
		log.Printf("[mongoflux] Connected to ClickHouse at %s:%d", cfg.ClickHouse.Host, cfg.ClickHouse.Port)
	}

	// Sync engines
	csSync := mfsync.NewChangeStreamSync(cfg, registry, chClient)
	oplogSync := mfsync.NewOplogSync(cfg, registry, chClient)

	// Query translator (used by wire proxy)
	_ = translator.NewTranslator(registry)

	// Management API
	mgmtAPI := api.NewManagementAPI(cfg.API, registry, chClient, csSync, oplogSync)

	// Start sync based on configured mode
	if cfg.Sync.Mode == "oplog" {
		log.Println("[mongoflux] Starting oplog sync (direct tailing, like a secondary node)...")
		oplogSync.Start()
	} else {
		log.Println("[mongoflux] Starting change stream sync...")
		csSync.Start()
	}

	log.Printf("[mongoflux] Starting management API on port %d", cfg.API.Port)
	mgmtAPI.Start()

	log.Println("[mongoflux] MongoFlux is running. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	log.Println("\n[mongoflux] Shutting down...")
	mgmtAPI.Stop()
	oplogSync.Stop()
	csSync.Stop()

	// Persist mappings
	if err := registry.SaveToFile(mappingsFile); err != nil {
		log.Printf("[mongoflux] Warning: Failed to save mappings: %v", err)
	}

	log.Println("[mongoflux] Shutdown complete.")
}
