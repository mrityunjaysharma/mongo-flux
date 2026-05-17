package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mongoflux/mongoflux/internal/clickhouse"
	"github.com/mongoflux/mongoflux/internal/config"
	"github.com/mongoflux/mongoflux/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeStreamSync uses MongoDB change streams for CDC replication.
// Alternative to OplogSync for Atlas/sharded clusters.
type ChangeStreamSync struct {
	config   *config.Config
	registry *schema.Registry
	chClient *clickhouse.Client
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	running  bool
	mu       sync.Mutex
}

// NewChangeStreamSync creates a new change stream sync engine.
func NewChangeStreamSync(cfg *config.Config, registry *schema.Registry, chClient *clickhouse.Client) *ChangeStreamSync {
	return &ChangeStreamSync{
		config:   cfg,
		registry: registry,
		chClient: chClient,
	}
}

// Start begins syncing all mapped collections. Non-blocking.
func (cs *ChangeStreamSync) Start() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.running {
		return
	}
	cs.running = true

	mappings := cs.registry.GetAll()
	for _, m := range mappings {
		if !m.Enabled {
			continue
		}
		ddl, err := cs.registry.GenerateCreateTableSQL(m)
		if err != nil {
			slog.Warn("Invalid mapping", "collection", m.Collection, "error", err)
			continue
		}
		if err := cs.chClient.CreateTable(ddl); err != nil {
			slog.Error("Failed to create table", "collection", m.Collection, "error", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cs.cancel = cancel

	for _, m := range mappings {
		if !m.Enabled {
			continue
		}
		cs.wg.Add(1)
		go cs.syncCollection(ctx, m.Collection)
	}
}

// Stop halts all sync threads.
func (cs *ChangeStreamSync) Stop() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if !cs.running {
		return
	}
	cs.running = false
	if cs.cancel != nil {
		cs.cancel()
	}
	cs.wg.Wait()
}

// IsRunning returns whether sync is active.
func (cs *ChangeStreamSync) IsRunning() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.running
}

// RestartCollection restarts sync for a specific collection without affecting others.
func (cs *ChangeStreamSync) RestartCollection(collection string) {
	// For simplicity, restart all. A production version could track per-collection
	// cancellation contexts, but stop/start is safe and fast for typical mapping counts.
	cs.Stop()
	cs.Start()
}

func (cs *ChangeStreamSync) syncCollection(ctx context.Context, collection string) {
	defer cs.wg.Done()

	// Retry loop — reconnects on stream failure (network blip, cursor death)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := cs.syncCollectionInner(ctx, collection)
		if err != nil {
			if ctx.Err() != nil {
				return // Shutdown requested
			}
			slog.Warn("Change stream failed, retrying", "collection", collection, "error", err, "retry_in", "3s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (cs *ChangeStreamSync) syncCollectionInner(ctx context.Context, collection string) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cs.config.Mongo.URI))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect(context.Background())

	coll := client.Database(cs.config.Mongo.Database).Collection(collection)
	mapping := cs.registry.Get(collection)
	if mapping == nil {
		return fmt.Errorf("no mapping found for collection: %s", collection)
	}

	// Set up change stream options
	csOpts := options.ChangeStream()
	csOpts.SetFullDocument(options.UpdateLookup)

	// Resume from saved token
	if token := cs.loadResumeToken(collection); token != nil {
		csOpts.SetResumeAfter(token)
	}

	stream, err := coll.Watch(ctx, mongo.Pipeline{}, csOpts)
	if err != nil {
		return fmt.Errorf("open change stream for %s: %w", collection, err)
	}
	defer stream.Close(ctx)

	var batch []map[string]interface{}
	lastFlush := time.Now()

	for stream.Next(ctx) {
		var event bson.M
		if err := stream.Decode(&event); err != nil {
			continue
		}

		opType, _ := event["operationType"].(string)
		if opType != "insert" && opType != "update" && opType != "replace" {
			continue
		}

		fullDoc, ok := event["fullDocument"].(bson.M)
		if !ok || len(fullDoc) == 0 {
			continue
		}

		batch = append(batch, extractMappedFields(fullDoc, mapping))

		// Save resume token
		if id, ok := event["_id"]; ok {
			cs.saveResumeToken(collection, id)
		}

		// Flush if batch is full
		if len(batch) >= cs.config.Sync.BatchSize {
			cs.flushBatch(collection, mapping, &batch)
			lastFlush = time.Now()
		}

		// Flush on interval
		if len(batch) > 0 && time.Since(lastFlush).Milliseconds() >= int64(cs.config.Sync.FlushIntervalMs) {
			cs.flushBatch(collection, mapping, &batch)
			lastFlush = time.Now()
		}
	}

	// Final flush
	if len(batch) > 0 {
		cs.flushBatch(collection, mapping, &batch)
	}

	return nil
}

func (cs *ChangeStreamSync) flushBatch(collection string, mapping *schema.CollectionMapping, batch *[]map[string]interface{}) {
	if len(*batch) == 0 {
		return
	}

	columns, rows := prepareBatchForInsert(*batch, mapping)
	if err := cs.chClient.InsertBatch(mapping.ClickHouseDatabase, mapping.ClickHouseTable, columns, rows); err != nil {
		slog.Error("Failed to flush batch", "collection", collection, "rows", len(*batch), "error", err)
	}
	*batch = (*batch)[:0]
}

func (cs *ChangeStreamSync) saveResumeToken(collection string, token interface{}) {
	path := filepath.Join(cs.config.Sync.ResumeTokenPath, collection+".json")
	if err := os.MkdirAll(cs.config.Sync.ResumeTokenPath, 0755); err != nil {
		slog.Error("Failed to create resume token directory", "error", err)
		return
	}
	data, err := json.Marshal(token)
	if err != nil {
		slog.Error("Failed to marshal resume token", "collection", collection, "error", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		slog.Error("Failed to persist resume token", "collection", collection, "error", err)
	}
}

func (cs *ChangeStreamSync) loadResumeToken(collection string) bson.Raw {
	path := filepath.Join(cs.config.Sync.ResumeTokenPath, collection+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var token bson.Raw
	if err := json.Unmarshal(data, &token); err != nil {
		slog.Warn("Corrupted resume token, starting fresh", "collection", collection)
		return nil
	}
	return token
}
