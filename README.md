# mg-clickhouse

Real-time MongoDB to ClickHouse replication with transparent analytics query routing. Makes ClickHouse behave as a "virtual secondary" — same write stream as replica set members, zero write overhead, 12-84x faster analytical reads.

## Architecture

```
┌─────────────┐       ┌──────────────────┐       ┌────────────┐
│  Application│──────▶│   mg-clickhouse   │──────▶│  MongoDB   │
│  (Mongo URI)│       │   (proxy + sync)  │       │  (primary) │
└─────────────┘       └────────┬─────────┘       └─────┬──────┘
                               │                        │
                               │ query translation      │ oplog tailing
                               ▼                        │ (same as secondary)
                        ┌────────────┐                  │
                        │ ClickHouse │◀─────────────────┘
                        │ (virtual   │   real-time CDC via
                        │  secondary)│   local.oplog.rs
                        └────────────┘
```

## Benchmark Results

### Read Performance (1M records, 5 iterations)

| Query | MongoDB (ms) | ClickHouse (ms) | Speedup |
|:------|:-------------|:----------------|:--------|
| [Count by status (GROUP BY)](benchmark/results.json#L24) | 506.5 | 10.9 | 46.7x |
| [Avg amount by region](benchmark/results.json#L33) | 558.8 | 9.4 | 59.2x |
| [Top 10 customers by spend](benchmark/results.json#L42) | 855.5 | 40.8 | 21.0x |
| [Multi-filter compound](benchmark/results.json#L51) | 109.0 | 11.4 | 9.6x |
| [Date range scan (3 months)](benchmark/results.json#L60) | 973.5 | 16.8 | 58.1x |
| [2-dim GROUP BY (events)](benchmark/results.json#L69) | 628.8 | 14.0 | 45.1x |
| [Avg duration by event type](benchmark/results.json#L78) | 685.5 | 9.0 | 76.5x |
| [Top 20 users by event count](benchmark/results.json#L87) | 727.4 | 33.4 | 21.8x |
| [Full table count](benchmark/results.json#L96) | 262.3 | 3.1 | 84.1x |
| [Percentile + multi-agg](benchmark/results.json#L105) | 659.4 | 11.9 | 55.4x |

**Average read speedup: 39.9x** at 1M records. Scales superlinearly — 12x at 200K, 40x at 1M.

### Write Overhead (200K records)

| Metric | Standalone MongoDB | With mg-clickhouse | Overhead |
|:-------|:-------------------|:-------------------|:---------|
| Batch throughput | 28,639 docs/s | 31,858 docs/s | ~0% |
| Single insert avg latency | 2.67 ms | 2.60 ms | ~0% |
| Single insert P99 latency | 8.25 ms | 8.08 ms | ~0% |

**Zero write overhead.** mg-clickhouse tails the oplog asynchronously — MongoDB acknowledges writes before the sync layer sees them.

Full results: [`benchmark/results.json`](benchmark/results.json), [`benchmark/write_results.json`](benchmark/write_results.json)

## How It Works

**Replication**: Opens a tailable-await cursor on `local.oplog.rs` (same mechanism MongoDB secondaries use), extracts mapped fields, batches them, and flushes to ClickHouse via HTTP INSERT. Persists oplog timestamps for crash recovery.

**Write path**: All writes go to MongoDB primary unchanged. No application code changes needed.

**Read path**: Standard URI reads from MongoDB. Add `?clickhouse=true` to route reads through the query translator to ClickHouse.

**Query translation**: Two-phase AST approach — BSON is parsed into an expression tree, then emitted as ClickHouse SQL. Supports `find()`, `aggregate()` with `$match`, `$group`, `$sort`, `$limit`, `$project`, `$count`, and filter operators (`$gt`, `$in`, `$regex`, `$and`/`$or`, etc.).

## Quick Start

```bash
docker compose up --build
```

Starts MongoDB (replica set), ClickHouse, and mg-clickhouse. API at `http://localhost:9090`.

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
| `GET` | `/api/v1/mappings/:collection` | Get mapping |
| `POST` | `/api/v1/mappings` | Create/update mapping |
| `DELETE` | `/api/v1/mappings/:collection` | Delete mapping |
| `POST` | `/api/v1/mappings/:collection/sync` | Create ClickHouse table |
| `GET` | `/api/v1/status` | Health + sync status |
| `POST` | `/api/v1/sync/restart` | Restart sync threads |
| `GET` | `/health` | Liveness probe |
| `GET` | `/ready` | Readiness probe |

## Configuration

```yaml
mongo:
  uri: "mongodb://localhost:27017"
  database: "myapp"

clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
  user: "default"
  password: ""

sync:
  mode: "oplog"           # "oplog" or "changestream" (Atlas/sharded)
  batch_size: 1000
  flush_interval_ms: 500
  resume_token_path: "/var/lib/mg-clickhouse/resume_tokens"

api:
  port: 9090
  bind: "0.0.0.0"

routing:
  clickhouse_param: "clickhouse"
```

| Sync Mode | Use Case | Requirement |
|:----------|:---------|:------------|
| `oplog` | Direct replica set, lowest latency | Access to `local.oplog.rs` |
| `changestream` | Atlas, sharded clusters | MongoDB 4.0+ |

## Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
./mg_clickhouse /path/to/config.yaml
```

Prerequisites: C++17, CMake 3.16+, mongocxx 3.9+, libcurl, OpenSSL. nlohmann/json, cpp-httplib, and yaml-cpp are fetched via CMake FetchContent.

## Deployment

```bash
docker build -t mg-clickhouse .
docker run -v ./config.yaml:/etc/mg-clickhouse/mg-clickhouse.yaml -p 9090:9090 mg-clickhouse
```

Kubernetes probes:

```yaml
livenessProbe:
  httpGet: { path: /health, port: 9090 }
readinessProbe:
  httpGet: { path: /ready, port: 9090 }
```

Runs as non-root (`mgch`), uses `tini` for signal handling, graceful shutdown flushes pending batches and persists oplog position.

## Running Benchmarks

```bash
pip install pymongo requests

# Read benchmark
python3 benchmark/read_benchmark.py --records 1000000 --iterations 5

# Write overhead benchmark
python3 benchmark/write_benchmark.py --records 200000 --batch-size 1000
```

## License

Apache-2.0
