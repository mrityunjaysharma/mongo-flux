package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mongoflux/mongoflux/internal/clickhouse"
	"github.com/mongoflux/mongoflux/internal/config"
	"github.com/mongoflux/mongoflux/internal/metrics"
	"github.com/mongoflux/mongoflux/internal/schema"
	mfsync "github.com/mongoflux/mongoflux/internal/sync"
)

// ManagementAPI is the HTTP management API for schema mappings and system status.
type ManagementAPI struct {
	config     config.APIConfig
	registry   *schema.Registry
	chClient   *clickhouse.Client
	csSync     *mfsync.ChangeStreamSync
	oplogSync  *mfsync.OplogSync
	httpServer *http.Server
}

// NewManagementAPI creates a new management API.
func NewManagementAPI(
	cfg config.APIConfig,
	registry *schema.Registry,
	chClient *clickhouse.Client,
	csSync *mfsync.ChangeStreamSync,
	oplogSync *mfsync.OplogSync,
) *ManagementAPI {
	return &ManagementAPI{
		config:    cfg,
		registry:  registry,
		chClient:  chClient,
		csSync:    csSync,
		oplogSync: oplogSync,
	}
}

// Start launches the HTTP server. Non-blocking.
func (a *ManagementAPI) Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Mappings CRUD
	r.GET("/api/v1/mappings", a.listMappings)
	r.GET("/api/v1/mappings/:name", a.getMapping)
	r.POST("/api/v1/mappings", a.createMapping)
	r.DELETE("/api/v1/mappings/:name", a.deleteMapping)
	r.POST("/api/v1/mappings/:name/sync", a.syncMapping)

	// Status & control
	r.GET("/api/v1/status", a.getStatus)
	r.POST("/api/v1/sync/restart", a.restartSync)

	// Probes
	r.GET("/health", a.health)
	r.GET("/ready", a.ready)
	r.GET("/metrics", a.metricsEndpoint)

	addr := fmt.Sprintf("%s:%d", a.config.Bind, a.config.Port)
	a.httpServer = &http.Server{Addr: addr, Handler: r}

	go func() {
		log.Printf("[mongoflux] Management API listening on %s", addr)
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[mongoflux] API server error: %v", err)
		}
	}()
}

// Stop gracefully shuts down the HTTP server.
func (a *ManagementAPI) Stop() {
	if a.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.httpServer.Shutdown(ctx)
	}
}

func (a *ManagementAPI) listMappings(c *gin.Context) {
	mappings := a.registry.GetAll()
	if mappings == nil {
		mappings = []schema.CollectionMapping{}
	}
	c.JSON(http.StatusOK, mappings)
}

func (a *ManagementAPI) getMapping(c *gin.Context) {
	collection := c.Param("name")
	mapping := a.registry.Get(collection)
	if mapping == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Mapping not found for collection: " + collection})
		return
	}
	c.JSON(http.StatusOK, mapping)
}

func (a *ManagementAPI) createMapping(c *gin.Context) {
	var mapping schema.CollectionMapping
	if err := c.ShouldBindJSON(&mapping); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON: " + err.Error()})
		return
	}

	if mapping.Collection == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "collection field is required"})
		return
	}
	if len(mapping.Fields) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fields array must not be empty"})
		return
	}
	if mapping.ClickHouseTable == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "clickhouse_table is required"})
		return
	}

	for _, f := range mapping.Fields {
		if f.MongoField == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Each field must have a non-empty mongo_field"})
			return
		}
		if f.CHColumn == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Each field must have a non-empty ch_column"})
			return
		}
		if f.CHType == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Each field must have a non-empty ch_type"})
			return
		}
	}

	if mapping.Engine == "" {
		mapping.Engine = "ReplacingMergeTree"
	}
	// Default to enabled for new mappings; respect explicit false from client
	if mapping.Collection != "" && !mapping.Enabled {
		// Only default to true if this is a brand new mapping (not an update)
		existing := a.registry.Get(mapping.Collection)
		if existing == nil {
			mapping.Enabled = true
		}
	}

	created := a.registry.Upsert(mapping)

	status := "updated"
	httpStatus := http.StatusOK
	if created {
		status = "created"
		httpStatus = http.StatusCreated
	}

	c.JSON(httpStatus, gin.H{
		"status":           status,
		"collection":       mapping.Collection,
		"clickhouse_table": mapping.ClickHouseTable,
	})
}

func (a *ManagementAPI) deleteMapping(c *gin.Context) {
	collection := c.Param("name")
	if !a.registry.Remove(collection) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Mapping not found for collection: " + collection})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted", "collection": collection})
}

func (a *ManagementAPI) syncMapping(c *gin.Context) {
	collection := c.Param("name")
	mapping := a.registry.Get(collection)
	if mapping == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Mapping not found for collection: " + collection})
		return
	}

	ddl := a.registry.GenerateCreateTableSQL(*mapping)
	if err := a.chClient.CreateTable(ddl); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if a.csSync != nil {
		a.csSync.RestartCollection(collection)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "synced",
		"collection": collection,
		"clustered":  mapping.Cluster != "",
		"ddl":        ddl,
	})
}

func (a *ManagementAPI) getStatus(c *gin.Context) {
	status := gin.H{
		"oplog_sync_running":        a.oplogSync != nil && a.oplogSync.IsRunning(),
		"changestream_sync_running": a.csSync != nil && a.csSync.IsRunning(),
		"mappings_count":            len(a.registry.GetAll()),
	}

	if err := a.chClient.Ping(); err != nil {
		status["clickhouse"] = "disconnected"
	} else {
		status["clickhouse"] = "connected"
	}

	c.JSON(http.StatusOK, status)
}

func (a *ManagementAPI) restartSync(c *gin.Context) {
	if a.oplogSync != nil {
		a.oplogSync.Stop()
		a.oplogSync.Start()
	}
	if a.csSync != nil {
		a.csSync.Stop()
		a.csSync.Start()
	}
	c.JSON(http.StatusOK, gin.H{"status": "restarted"})
}

func (a *ManagementAPI) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (a *ManagementAPI) ready(c *gin.Context) {
	if err := a.chClient.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "reason": "clickhouse_unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (a *ManagementAPI) metricsEndpoint(c *gin.Context) {
	c.Data(http.StatusOK, "text/plain; version=0.0.4", []byte(metrics.Instance().Render()))
}
