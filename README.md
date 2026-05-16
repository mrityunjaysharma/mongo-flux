# MongoFlux

Real-time MongoDB to ClickHouse replication with transparent analytics query routing. Makes ClickHouse behave as a "virtual secondary" — same write stream as replica set members, zero write overhead, 12-84x faster analytical reads.

## Why: OLTP + OLAP in One Stack

MongoDB excels at OLTP (transactional reads/writes — point lookups, single-document operations, low-latency CRUD). But it struggles with OLAP (analytical queries — aggregations over millions of rows, GROUP BY, time-range scans, percentiles). These workloads have fundamentally different storage and execution requirements:

| | OLTP (MongoDB) | OLAP (ClickHouse) |
|:--|:---------------|:-------------------|
| Storage | Row-oriented (full documents) | Column-oriented (only needed columns scanned) |
| Strength | Point lookups, writes, transactions | Aggregations, scans, GROUP BY |
| Compression | ~2x | ~10-20x (columnar + codecs) |
| Scan 1M rows | 500-1500 ms | 3-40 ms |
| Typical query | `findById`, `insertOne` | `SELECT avg(amount) GROUP BY region` |

The traditional solution is ETL pipelines (batch jobs that copy data to a warehouse). MongoFlux eliminates ETL entirely — it replicates in real-time via the oplog (same stream MongoDB secondaries consume) and routes queries transparently. Your application keeps using MongoDB for OLTP, and analytical reads automatically go to ClickHouse with a single URI parameter.

**The result**: OLTP + OLAP from one MongoDB connection string. No ETL, no data staleness, no application changes.

## Architecture

```mermaid
flowchart TD
    APP[Your Application] -->|writes + OLTP reads| MGP[(MongoDB Primary)]
    MGP -->|replication| MGS1[(Secondary 1)]
    MGP -->|replication| MGS2[(Secondary 2)]
    APP -->|"all reads (find/aggregate)"| MGC[MongoFlux]
    MGC -->|"?clickhouse=true → SQL"| CH[(ClickHouse)]
    MGC -->|"no flag → forward to MongoDB"| MGP
    MGP -->|"oplog (async)"| MGC
    MGC -->|batch INSERT| CH
```

MongoDB runs as a 3-node replica set (`rs0`): 1 primary + 2 secondaries. For reads, the application connects to MongoFlux which acts as a routing proxy. If the connection URI contains `?clickhouse=true`, MongoFlux translates the query to SQL and sends it to ClickHouse. If the parameter is absent or false, MongoFlux forwards the query to MongoDB Primary unchanged. Writes always go directly to the primary. Separately, MongoFlux tails the primary's oplog to replicate data into ClickHouse in real-time.

## Benchmark Results

### Break-Even Analysis: MongoDB vs MongoDB+Index vs MongoFlux

Tested 5 complex aggregation queries (2D/3D GROUP BY, conditional counts, date bucketing, Top-N) at increasing data sizes to find where MongoFlux becomes faster.

```mermaid
xychart-beta
    title "Avg Query Latency by Data Size (log₁₀ ms)"
    x-axis ["100", "500", "1K", "5K", "10K", "50K", "100K", "500K", "1M", "2M"]
    y-axis "log₁₀(Latency ms)" 0 --> 3.5
    line [0.04, 0.30, 0.41, 0.87, 1.12, 1.74, 2.01, 2.75, 3.06, 3.46]
    line [0.04, 0.26, 0.38, 0.96, 1.12, 1.85, 2.05, 2.71, 3.04, 3.44]
    line [0.73, 0.81, 0.68, 1.01, 0.73, 0.94, 0.92, 1.05, 1.12, 1.33]
```

> Lines: MongoDB (blue), MongoDB+Index (green), MongoFlux (red). Values are log₁₀(ms) — e.g., 1.0 = 10ms, 2.0 = 100ms, 3.0 = 1000ms.

| Size | MongoDB | Mongo+Index | MongoFlux | Speedup | Winner |
|:-----|:--------|:------------|:----------|:--------|:-------|
| 100 | 1.1 ms | 1.1 ms | 5.4 ms | — | MongoDB |
| 500 | 2.0 ms | 1.8 ms | 6.4 ms | — | Mongo+Index |
| 1,000 | 2.6 ms | 2.4 ms | 4.8 ms | — | Mongo+Index |
| 5,000 | 7.4 ms | 9.2 ms | 10.3 ms | — | MongoDB |
| 10,000 | 13.3 ms | 13.2 ms | 5.4 ms | 2.5x | MongoFlux ⚡ |
| 50,000 | 55.4 ms | 70.6 ms | 8.7 ms | 6.4x | MongoFlux ⚡ |
| 100,000 | 102.1 ms | 111.6 ms | 8.4 ms | 12.1x | MongoFlux ⚡ |
| 500,000 | 562.6 ms | 515.2 ms | 11.2 ms | 50.1x | MongoFlux ⚡ |
| 1,000,000 | 1,139 ms | 1,085 ms | 13.1 ms | 87.3x | MongoFlux ⚡ |
| 2,000,000 | 2,854 ms | 2,745 ms | 21.5 ms | 132.5x | MongoFlux ⚡ |

**Break-even point: ~10,000 documents.** Below this, MongoDB is faster due to HTTP round-trip overhead. Above this, MongoFlux wins decisively — and the gap grows linearly with data size. Indexes do not help aggregations (they're designed for point lookups, not full-collection scans).

Key observations:
- MongoFlux latency stays flat (5-21ms) regardless of data size — columnar scans are O(columns), not O(rows)
- MongoDB latency grows linearly: ~1ms per 1,000 documents
- At 2M documents: 132.5x faster with MongoFlux (21ms vs 2,854ms)

### Read Performance — Standalone vs Distributed (500K records)

| Query | MongoDB (ms) | MongoFlux (ms) | Distributed 3-shard (ms) | Speedup | D speedup |
|:------|:-------------|:---------------|:----------------------------|:----------|:----------|
| [Count by status (GROUP BY)](benchmark/distributed_results.json#L5) | 822.8 | 16.5 | 61.1 | 50.0x | 13.5x |
| [Avg amount by region](benchmark/distributed_results.json#L10) | 886.1 | 21.5 | 44.4 | 41.2x | 20.0x |
| [Top 10 customers by spend](benchmark/distributed_results.json#L15) | 1,096.8 | 88.6 | 177.6 | 12.4x | 6.2x |
| [Single-region filter (shard-local)](benchmark/distributed_results.json#L20) | 205.0 | 40.6 | 66.6 | 5.0x | 3.1x |
| [Single-region aggregation (shard-local)](benchmark/distributed_results.json#L25) | 313.2 | 30.6 | 56.1 | 10.2x | 5.6x |
| [Date range scan (all shards, parallel)](benchmark/distributed_results.json#L30) | 1,492.2 | 77.6 | 63.1 | 19.2x | 23.7x |
| [Full table count](benchmark/distributed_results.json#L35) | 447.7 | 11.8 | 36.5 | 37.8x | 12.3x |
| [Percentile + multi-agg](benchmark/distributed_results.json#L40) | 1,206.1 | 22.6 | 57.8 | 53.3x | 20.9x |
| [Heavy aggregation (uniqExact)](benchmark/distributed_results.json#L45) | 1,336.5 | 140.5 | 250.9 | 9.5x | 5.3x |

**Standalone avg: 26.5x** | **Distributed avg: 12.3x** | Distributed wins on large parallel scans (date range: 23.7x vs 19.2x). Standalone wins when data fits in single-node RAM.

### Write Overhead (200K records)

| Metric | Standalone MongoDB | With MongoFlux | Overhead |
|:-------|:-------------------|:---------------|:---------|
| [Batch throughput](benchmark/write_results.json#L7) | 28,639 docs/s | 31,858 docs/s | ~0% |
| [Single insert avg latency](benchmark/write_results.json#L30) | 2.67 ms | 2.60 ms | ~0% |
| [Single insert P99 latency](benchmark/write_results.json#L35) | 8.25 ms | 8.08 ms | ~0% |

**Zero write overhead.** MongoFlux tails the oplog asynchronously — MongoDB acknowledges writes before the sync layer sees them.

Full results: [`benchmark/results.json`](benchmark/results.json), [`benchmark/write_results.json`](benchmark/write_results.json), [`benchmark/distributed_results.json`](benchmark/distributed_results.json)

## How It Works

**Replication**: Opens a tailable-await cursor on `local.oplog.rs` (same mechanism MongoDB secondaries use), extracts mapped fields, batches them, and flushes to ClickHouse via HTTP INSERT. Persists oplog timestamps for crash recovery. Supports backpressure (configurable `max_pending_rows`) and optional soft-delete propagation via tombstone rows.

**Write path**: All writes go directly to MongoDB Primary. No proxy, no application code changes needed.

**Read path**: The application sends reads to MongoFlux (proxy). MongoFlux inspects the connection URI:
- `?clickhouse=true` (or `1` / `yes`) → translates the query to SQL, executes on ClickHouse, returns results
- Parameter absent or `false` → forwards the query to MongoDB Primary unchanged

**Query translation**: Two-phase AST approach — BSON is parsed into an expression tree (`ExprNode`), then emitted as ClickHouse SQL. Supports:
- `find()` with filter, projection, sort, limit, skip
- `aggregate()` with `$match`, `$group`, `$sort`, `$limit`, `$skip`, `$project`, `$addFields`, `$set`, `$unwind`, `$count`, `$sample`
- Filter operators: `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$and`, `$or`, `$nor`, `$exists`, `$regex`
- Accumulators: `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`, `$addToSet`, `$stdDevPop`, `$stdDevSamp`
- Arithmetic expressions: `$multiply`, `$add`, `$subtract`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$sqrt`, `$pow`, `$log10`, `$ln`
- String expressions: `$concat`, `$toUpper`, `$toLower`, `$trim`, `$ltrim`, `$rtrim`, `$substr`, `$split`, `$strLenBytes`, `$regexMatch`
- Date expressions: `$year`, `$month`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`
- Conditional expressions: `$cond`

**Observability**: Prometheus-format metrics at `/metrics` — rows synced (per collection), flush success/failure counts, oplog entries processed, reconnect attempts, pending rows gauge, oplog lag, flush duration, sync running state.

## Quick Start

```bash
docker compose up --build
```

Starts a 3-node MongoDB replica set (1 primary + 2 secondaries on ports 27017-27019), ClickHouse (ports 8123/9000), and MongoFlux (port 9090). The replica set initializes automatically. API at `http://localhost:9090`.

### Create a Mapping

```bash
curl -X POST http://localhost:9090/api/v1/mappings \
  -H "Content-Type: application/json" \
  -d '{
    "collection": "orders",
    "clickhouse_table": "orders",
    "clickhouse_database": "analytics",
    "fields": [
      {"mongo_field": "_id", "ch_column": "id", "ch_type": "String"},
      {"mongo_field": "amount", "ch_column": "amount", "ch_type": "Float64"},
      {"mongo_field": "status", "ch_column": "status", "ch_type": "LowCardinality(String)"},
      {"mongo_field": "created_at", "ch_column": "created_at", "ch_type": "DateTime CODEC(Delta(4), ZSTD(1))"}
    ],
    "engine": "ReplacingMergeTree",
    "order_by": ["created_at", "id"]
  }'

# Create the ClickHouse table
curl -X POST http://localhost:9090/api/v1/mappings/orders/sync
```

### Distributed (Clustered) Mapping

For multi-shard ClickHouse deployments, include `cluster` and optionally `sharding_key`. MongoFlux generates both the local MergeTree table and the Distributed table automatically:

```bash
curl -X POST http://localhost:9090/api/v1/mappings \
  -H "Content-Type: application/json" \
  -d '{
    "collection": "events",
    "clickhouse_table": "events",
    "clickhouse_database": "analytics",
    "cluster": "prod-cluster",
    "sharding_key": "cityHash64(user_id)",
    "fields": [
      {"mongo_field": "_id", "ch_column": "id", "ch_type": "String"},
      {"mongo_field": "user_id", "ch_column": "user_id", "ch_type": "String"},
      {"mongo_field": "event_type", "ch_column": "event_type", "ch_type": "LowCardinality(String)"},
      {"mongo_field": "timestamp", "ch_column": "ts", "ch_type": "DateTime64(3)"}
    ],
    "engine": "ReplacingMergeTree",
    "order_by": ["event_type", "ts"]
  }'
```

This creates `events_local` (MergeTree on each shard) and `events` (Distributed table routing by `cityHash64(user_id)`).

### Production Table (Advanced ClickHouse Features)

For workloads requiring codecs, bloom filters, TTL, and tiered storage — create the table directly:

```sql
CREATE TABLE IF NOT EXISTS analytics.k8s_logs ON CLUSTER 'prod-cluster'
(
    `Timestamp`          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TimestampTime`      DateTime DEFAULT toDateTime(Timestamp),
    `TraceId`            String CODEC(ZSTD(1)),
    `SpanId`             String CODEC(ZSTD(1)),
    `SeverityText`       LowCardinality(String) CODEC(ZSTD(1)),
    `ServiceName`        LowCardinality(String) CODEC(ZSTD(1)),
    `Body`               String CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `LogAttributes`      Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, Timestamp, TimestampTime)
TTL TimestampTime + toIntervalDay(7) TO VOLUME 'cold',
    TimestampTime + INTERVAL 1 YEAR
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192, ttl_only_drop_parts = 1;
```

Then register the field mapping:

```bash
curl -X POST http://localhost:9090/api/v1/mappings \
  -H "Content-Type: application/json" \
  -d '{
    "collection": "k8s_logs",
    "clickhouse_table": "k8s_logs",
    "clickhouse_database": "analytics",
    "fields": [
      {"mongo_field": "timestamp", "ch_column": "Timestamp", "ch_type": "DateTime64(9)"},
      {"mongo_field": "traceId", "ch_column": "TraceId", "ch_type": "String"},
      {"mongo_field": "service", "ch_column": "ServiceName", "ch_type": "LowCardinality(String)"},
      {"mongo_field": "body", "ch_column": "Body", "ch_type": "String"},
      {"mongo_field": "resource", "ch_column": "ResourceAttributes", "ch_type": "Map(LowCardinality(String), String)"},
      {"mongo_field": "attributes", "ch_column": "LogAttributes", "ch_type": "Map(LowCardinality(String), String)"}
    ],
    "engine": "MergeTree",
    "order_by": ["ServiceName", "Timestamp"]
  }'
```

## API Reference

| Method | Endpoint | Description |
|:-------|:---------|:------------|
| `GET` | `/api/v1/mappings` | List all mappings |
| `GET` | `/api/v1/mappings/:collection` | Get mapping for a collection |
| `POST` | `/api/v1/mappings` | Create or update a mapping |
| `DELETE` | `/api/v1/mappings/:collection` | Delete a mapping |
| `POST` | `/api/v1/mappings/:collection/sync` | Create ClickHouse table from mapping |
| `GET` | `/api/v1/status` | Health + sync status (oplog/changestream running, ClickHouse connectivity) |
| `POST` | `/api/v1/sync/restart` | Restart all sync threads |
| `GET` | `/health` | Liveness probe (always 200) |
| `GET` | `/ready` | Readiness probe (checks ClickHouse connectivity) |
| `GET` | `/metrics` | Prometheus metrics |

### Mapping Schema

```json
{
  "collection": "orders",
  "clickhouse_table": "orders",
  "clickhouse_database": "analytics",
  "fields": [
    {"mongo_field": "_id", "ch_column": "id", "ch_type": "String"}
  ],
  "engine": "ReplacingMergeTree",
  "order_by": ["created_at", "id"],
  "cluster": "",
  "sharding_key": "",
  "enabled": true
}
```

The `cluster` field triggers distributed DDL (ON CLUSTER) and auto-creates both a local MergeTree table and a Distributed table. The `sharding_key` controls data distribution across shards (defaults to `rand()` if empty).

## Configuration

```yaml
mongo:
  uri: "mongodb://mongo-primary:27017,mongo-secondary1:27017,mongo-secondary2:27017/?replicaSet=rs0"
  database: "myapp"

clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
  user: "default"
  password: ""
  cluster: ""                       # Cluster name for distributed DDL (empty = standalone)

sync:
  mode: "oplog"                     # "oplog" or "changestream" (Atlas/sharded)
  batch_size: 1000                  # Rows per flush (1-1000000)
  flush_interval_ms: 500            # Max ms between flushes (1-60000)
  resume_token_path: "/var/lib/mongoflux/resume_tokens"
  max_pending_rows: 100000          # Backpressure: max rows buffered before blocking
  propagate_deletes: false          # Insert tombstone rows on delete operations
  delete_column: "_deleted"         # Column name for soft-delete flag

api:
  port: 9090
  bind: "0.0.0.0"

routing:
  clickhouse_param: "clickhouse"    # URI parameter that triggers ClickHouse routing

logging:
  level: "info"                     # debug, info, warn, error
  file: ""                          # Empty = stdout only
```

All values can be overridden via environment variables with the `MG_` prefix (e.g., `MG_MONGO_URI`, `MG_CH_HOST`, `MG_CH_PASSWORD`).

| Sync Mode | Use Case | Requirement |
|:----------|:---------|:------------|
| `oplog` | Direct replica set, lowest latency | Access to `local.oplog.rs` |
| `changestream` | Atlas, sharded clusters | MongoDB 4.0+ |

## Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
./mongoflux /path/to/config.yaml
```

Prerequisites: C++17, CMake 3.16+, mongocxx 3.9+ (with mongo-c-driver 1.28+), libcurl, OpenSSL. The following are fetched automatically via CMake FetchContent: nlohmann/json 3.11.3, cpp-httplib 0.15.3, yaml-cpp 0.8.0.

## Deployment

```bash
docker build -t mongoflux .
docker run -v ./config.yaml:/etc/mongoflux/mongoflux.yaml -p 9090:9090 mongoflux
```

The Docker image uses a multi-stage build: the builder stage compiles mongo-c-driver 1.28.0 and mongo-cxx-driver 3.9.0 from source, then the runtime image is a minimal Ubuntu 22.04 with only shared libraries. Runs as non-root (`mongoflux` user), uses `tini` for signal handling, and performs graceful shutdown (flushes pending batches, persists oplog position).

Kubernetes probes:

```yaml
livenessProbe:
  httpGet: { path: /health, port: 9090 }
readinessProbe:
  httpGet: { path: /ready, port: 9090 }
```

### Distributed ClickHouse Cluster (Benchmarking)

For testing with a 3-shard ClickHouse cluster:

```bash
docker compose -f benchmark/docker-compose-cluster.yml up -d
```

This starts 3 ClickHouse shards (ports 8124-8126) with a ClickHouse Keeper for coordination, configured as `bench_cluster`.

## Observability

MongoFlux exposes Prometheus metrics at `GET /metrics`:

```
mongoflux_rows_synced_total          — Total rows synced to ClickHouse (counter)
mongoflux_rows_synced{collection}    — Rows synced per collection (counter)
mongoflux_flush_success_total        — Successful flush operations (counter)
mongoflux_flush_failure_total        — Failed flush operations (counter)
mongoflux_oplog_entries_total        — Oplog entries processed (counter)
mongoflux_oplog_reconnects_total     — Oplog reconnection attempts (counter)
mongoflux_pending_rows               — Current rows pending flush (gauge)
mongoflux_oplog_lag_ms               — Estimated oplog replication lag (gauge)
mongoflux_last_flush_duration_ms     — Duration of last flush (gauge)
mongoflux_sync_running               — Whether sync is currently active (gauge)
```

## Running Benchmarks

```bash
pip install pymongo requests

# Break-even benchmark (MongoDB vs MongoDB+Index vs MongoFlux)
python3 benchmark/breakeven_benchmark.py --max-size 2000000

# Read benchmark (12 queries across orders + events collections)
python3 benchmark/read_benchmark.py --records 1000000 --iterations 5

# Write overhead benchmark
python3 benchmark/write_benchmark.py --records 200000 --batch-size 1000

# Top 20 aggregation patterns benchmark
python3 benchmark/aggregation_benchmark.py --records 500000

# Distributed vs standalone comparison (requires cluster)
docker compose -f benchmark/docker-compose-cluster.yml up -d
python3 benchmark/distributed_benchmark.py --records 500000

# Real data benchmark (auto-discovers schema from your MongoDB)
export MONGO_URI="mongodb://user:pass@host:27017/db?authSource=admin"
export MONGO_DB="mydb"
export MONGO_COLLECTION="myCollection"
python3 benchmark/real_data_benchmark.py --limit 500000 --iterations 3
```

## Integration Tests

```bash
# Start the full stack
docker compose up --build -d

# Run integration tests (CRUD + sync verification + aggregation benchmark)
python3 test-app/test_mongoflux.py

# Cleanup test data after
python3 test-app/test_mongoflux.py --cleanup
```

The test suite verifies single/batch inserts sync to ClickHouse, updates propagate correctly, stream continuity after updates, and runs 10 aggregation queries comparing MongoDB vs ClickHouse latency.

## License

Apache-2.0
