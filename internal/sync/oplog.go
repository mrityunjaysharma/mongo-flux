package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mongoflux/mongoflux/internal/clickhouse"
	"github.com/mongoflux/mongoflux/internal/config"
	"github.com/mongoflux/mongoflux/internal/metrics"
	"github.com/mongoflux/mongoflux/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OplogSync replicates data to ClickHouse by tailing MongoDB's oplog,
// using the same mechanism MongoDB secondaries use.
type OplogSync struct {
	config   *config.Config
	registry *schema.Registry
	chClient *clickhouse.Client
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	running  bool
	mu       sync.Mutex
}

// NewOplogSync creates a new oplog sync engine.
func NewOplogSync(cfg *config.Config, registry *schema.Registry, chClient *clickhouse.Client) *OplogSync {
	return &OplogSync{
		config:   cfg,
		registry: registry,
		chClient: chClient,
	}
}

// Start begins tailing the oplog. Non-blocking.
func (o *OplogSync) Start() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.running {
		return
	}
	o.running = true

	// Ensure ClickHouse tables exist
	mappings := o.registry.GetAll()
	for _, m := range mappings {
		if !m.Enabled {
			continue
		}
		ddl := o.registry.GenerateCreateTableSQL(m)
		if err := o.chClient.CreateTable(ddl); err != nil {
			log.Printf("[mongoflux/oplog] Failed to create table for %s: %v", m.Collection, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	o.cancel = cancel
	o.wg.Add(1)
	go o.tailOplog(ctx)
}

// Stop halts the oplog tailer.
func (o *OplogSync) Stop() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.running {
		return
	}
	o.running = false
	if o.cancel != nil {
		o.cancel()
	}
	o.wg.Wait()
}

// IsRunning returns whether the sync is active.
func (o *OplogSync) IsRunning() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.running
}

func (o *OplogSync) tailOplog(ctx context.Context) {
	defer o.wg.Done()

	for {
		select {
		case <-ctx.Done():
			metrics.Instance().SetSyncRunning(false)
			return
		default:
		}

		metrics.Instance().SetSyncRunning(true)
		err := o.tailOplogInner(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[mongoflux/oplog] Connection failed: %v. Retrying in 3s...", err)
			metrics.Instance().IncOplogReconnects()
			metrics.Instance().SetSyncRunning(false)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (o *OplogSync) tailOplogInner(ctx context.Context) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(o.config.Mongo.URI))
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	defer client.Disconnect(ctx)

	oplog := client.Database("local").Collection("oplog.rs")

	// Load saved position
	startTS := o.loadOplogTimestamp()

	// If no saved timestamp, start from current oplog tail
	if startTS.T == 0 {
		opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}})
		var lastEntry bson.M
		err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&lastEntry)
		if err != nil {
			return fmt.Errorf("cannot determine oplog position (is this a replica set?): %w", err)
		}
		if ts, ok := lastEntry["ts"].(primitive.Timestamp); ok {
			startTS = ts
		}
		log.Printf("[mongoflux/oplog] Starting from oplog ts=%d:%d", startTS.T, startTS.I)
	} else {
		log.Printf("[mongoflux/oplog] Resuming from saved oplog ts=%d:%d", startTS.T, startTS.I)
	}

	// Build tailable-await cursor
	nsPrefix := o.config.Mongo.Database + "."
	filter := bson.D{
		{Key: "ts", Value: bson.D{{Key: "$gt", Value: startTS}}},
		{Key: "ns", Value: bson.D{{Key: "$regex", Value: primitive.Regex{Pattern: nsPrefix}}}},
	}

	opts := options.Find().
		SetCursorType(options.TailableAwait).
		SetNoCursorTimeout(true).
		SetBatchSize(int32(o.config.Sync.BatchSize))

	cursor, err := oplog.Find(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("oplog cursor: %w", err)
	}
	defer cursor.Close(ctx)

	// Pending batches per collection
	type pendingBatch struct {
		rows      []map[string]interface{}
		lastFlush time.Time
	}
	pending := make(map[string]*pendingBatch)
	var lastProcessedTS primitive.Timestamp

	flushAll := func() {
		allFlushed := true
		flushStart := time.Now()

		for collection, batch := range pending {
			if len(batch.rows) == 0 {
				continue
			}
			mapping := o.registry.Get(collection)
			if mapping == nil {
				continue
			}

			columns, chRows := prepareBatchForInsert(batch.rows, mapping)
			err := o.chClient.InsertBatch(mapping.ClickHouseDatabase, mapping.ClickHouseTable, columns, chRows)
			if err != nil {
				log.Printf("[mongoflux/oplog] Flush failed for %s (%d rows): %v",
					collection, len(batch.rows), err)
				metrics.Instance().IncFlushFailure()
				allFlushed = false
			} else {
				metrics.Instance().IncRowsSynced(collection, int64(len(batch.rows)))
				metrics.Instance().IncFlushSuccess()
				batch.rows = batch.rows[:0]
				batch.lastFlush = time.Now()
			}
		}

		flushDuration := time.Since(flushStart).Milliseconds()
		metrics.Instance().SetLastFlushMs(flushDuration)

		if allFlushed && lastProcessedTS.T > 0 {
			o.saveOplogTimestamp(lastProcessedTS)
		}

		var totalPending int64
		for _, b := range pending {
			totalPending += int64(len(b.rows))
		}
		metrics.Instance().SetPendingRows(totalPending)
	}

	// Main tailing loop
	for cursor.Next(ctx) {
		var entry bson.M
		if err := cursor.Decode(&entry); err != nil {
			continue
		}

		opType, _ := entry["op"].(string)
		ns, _ := entry["ns"].(string)

		collection := extractCollection(ns)
		if collection == "" {
			continue
		}
		if !o.registry.HasMapping(collection) {
			continue
		}
		mapping := o.registry.Get(collection)
		if mapping == nil || !mapping.Enabled {
			continue
		}

		// Track timestamp
		if ts, ok := entry["ts"].(primitive.Timestamp); ok {
			lastProcessedTS = ts
		}

		if pending[collection] == nil {
			pending[collection] = &pendingBatch{lastFlush: time.Now()}
		}

		switch opType {
		case "i": // INSERT
			oDoc, ok := entry["o"].(bson.M)
			if !ok {
				continue
			}
			pending[collection].rows = append(pending[collection].rows, extractMappedFields(oDoc, mapping))

		case "u": // UPDATE
			oDoc, ok := entry["o"].(bson.M)
			if !ok {
				continue
			}
			// Check if full replacement (no $ operators)
			isReplacement := true
			for k := range oDoc {
				if strings.HasPrefix(k, "$") || k == "diff" {
					isReplacement = false
					break
				}
			}
			if isReplacement {
				pending[collection].rows = append(pending[collection].rows, extractMappedFields(oDoc, mapping))
			}

		case "d": // DELETE
			if o.config.Sync.PropagateDeletes {
				oDoc, ok := entry["o"].(bson.M)
				if !ok {
					continue
				}
				row := make(map[string]interface{})
				for _, fm := range mapping.Fields {
					if fm.MongoField == "_id" {
						if id, ok := oDoc["_id"]; ok {
							row[fm.CHColumn] = formatValue(id)
						}
					} else {
						row[fm.CHColumn] = nil
					}
				}
				row[o.config.Sync.DeleteColumn] = 1
				pending[collection].rows = append(pending[collection].rows, row)
			}
		}

		metrics.Instance().IncOplogEntries()

		// Backpressure
		var totalPending int64
		for _, b := range pending {
			totalPending += int64(len(b.rows))
		}
		if totalPending >= int64(o.config.Sync.MaxPendingRows) {
			flushAll()
		}

		// Flush if batch is full
		for _, batch := range pending {
			if len(batch.rows) >= o.config.Sync.BatchSize {
				flushAll()
				break
			}
		}

		// Flush on time interval
		now := time.Now()
		for _, batch := range pending {
			if len(batch.rows) > 0 && now.Sub(batch.lastFlush).Milliseconds() >= int64(o.config.Sync.FlushIntervalMs) {
				flushAll()
				break
			}
		}
	}

	// Final flush
	flushAll()
	return cursor.Err()
}

func (o *OplogSync) saveOplogTimestamp(ts primitive.Timestamp) {
	path := filepath.Join(o.config.Sync.ResumeTokenPath, "oplog_position.json")
	os.MkdirAll(o.config.Sync.ResumeTokenPath, 0755)

	data, _ := json.Marshal(map[string]uint32{
		"timestamp": ts.T,
		"increment": ts.I,
	})
	os.WriteFile(path, data, 0644)
}

func (o *OplogSync) loadOplogTimestamp() primitive.Timestamp {
	path := filepath.Join(o.config.Sync.ResumeTokenPath, "oplog_position.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return primitive.Timestamp{}
	}

	var saved struct {
		Timestamp uint32 `json:"timestamp"`
		Increment uint32 `json:"increment"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		return primitive.Timestamp{}
	}
	return primitive.Timestamp{T: saved.Timestamp, I: saved.Increment}
}

func extractCollection(ns string) string {
	dot := strings.IndexByte(ns, '.')
	if dot < 0 {
		return ""
	}
	return ns[dot+1:]
}

func extractMappedFields(doc bson.M, mapping *schema.CollectionMapping) map[string]interface{} {
	row := make(map[string]interface{}, len(mapping.Fields))
	for _, fm := range mapping.Fields {
		val, ok := doc[fm.MongoField]
		if ok {
			row[fm.CHColumn] = formatValue(val)
		} else {
			row[fm.CHColumn] = nil
		}
	}
	return row
}

func formatValue(val interface{}) interface{} {
	switch v := val.(type) {
	case primitive.ObjectID:
		return v.Hex()
	case primitive.DateTime:
		return v.Time().UnixMilli()
	default:
		return v
	}
}

func prepareBatchForInsert(batch []map[string]interface{}, mapping *schema.CollectionMapping) ([]string, [][]string) {
	columns := make([]string, len(mapping.Fields))
	for i, f := range mapping.Fields {
		columns[i] = f.CHColumn
	}

	rows := make([][]string, 0, len(batch))
	for _, row := range batch {
		values := make([]string, len(mapping.Fields))
		for i, f := range mapping.Fields {
			val, ok := row[f.CHColumn]
			if !ok || val == nil {
				values[i] = "NULL"
			} else {
				switch v := val.(type) {
				case string:
					values[i] = escapeCHString(v)
				case int32:
					values[i] = fmt.Sprintf("%d", v)
				case int64:
					values[i] = fmt.Sprintf("%d", v)
				case float64:
					values[i] = fmt.Sprintf("%v", v)
				case bool:
					if v {
						values[i] = "1"
					} else {
						values[i] = "0"
					}
				case int:
					values[i] = fmt.Sprintf("%d", v)
				default:
					values[i] = fmt.Sprintf("'%v'", v)
				}
			}
		}
		rows = append(rows, values)
	}
	return columns, rows
}

func escapeCHString(val string) string {
	escaped := strings.ReplaceAll(val, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return "'" + escaped + "'"
}
