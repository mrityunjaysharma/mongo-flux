# MongoFlux

Real-time MongoDB → ClickHouse replication with transparent query routing.

ClickHouse becomes a "virtual secondary" in your replica set — same write stream, zero overhead on the write path, and analytical queries run 12-130x faster than MongoDB aggregations.

## The Problem

MongoDB is great for OLTP. Point lookups, single-doc writes, transactions — all fast. But the moment you need analytics (GROUP BY over millions of rows, time-range scans, percentiles), it falls apart. You end up building ETL pipelines, maintaining a separate warehouse, dealing with stale data.

MongoFlux fixes this. It tails the oplog in real-time (exactly like a secondary node does), replicates to ClickHouse, and routes analytical queries there transparently. Your app doesn't change. You just add `?clickhouse=true` to the connection string for reads that should go to the columnar engine.

No ETL. No batch jobs. No data staleness.

## How it works

```mermaid
flowchart TD
    %% Write + Sync
    APP[Your Application]
    APP -->|"writes (insert/update/delete)"| MGP[(MongoDB Primary)]
    MGP -->|replication| S1[(Secondary 1)]
    MGP -->|replication| S2[(Secondary 2)]
    MGP -.->|"oplog tail (async)"| MF[MongoFlux]
    MF -.->|"batch INSERT every 500ms"| CH[(ClickHouse)]

    %% Read routing
    APP -->|"analytical reads (find/aggregate)"| MF
    MF -->|"clickhouse=true → SQL"| CH
    MF -->|"clickhouse=false → passthrough"| MGP
```

Three things happen:

1. **Writes** go directly to MongoDB. MongoFlux is not in the write path at all.
2. **Replication** — MongoFlux opens a tailable-await cursor on `local.oplog.rs` (same mechanism secondaries use), extracts the fields you've mapped, batches them, and flushes to ClickHouse via HTTP. It persists oplog timestamps so it can resume after a crash.
3. **Reads** — your app sends queries to MongoFlux. If the URI has `?clickhouse=true` (or `1`/`yes`), the query gets translated to SQL and executed on ClickHouse. Otherwise it's forwarded to MongoDB unchanged.

The query translator uses a two-phase AST approach: BSON → expression tree → ClickHouse SQL. It handles `find()` and `aggregate()` with most common operators — see the full list below.

## Benchmarks

I ran these on a single machine (Docker Compose, no network hops). Real-world numbers will vary, but the relative differences hold.

### Break-even point

Tested 5 complex aggregation queries at increasing data sizes. The question: at what point does MongoFlux become faster than MongoDB?

```mermaid
xychart-beta
    title "Avg Query Latency by Data Size (ms)"
    x-axis ["100", "500", "1K", "5K", "10K", "50K", "100K", "500K", "1M", "2M"]
    y-axis "Latency (ms)" 0 --> 200
    line [1.1, 2.0, 2.6, 7.4, 13.3, 55.4, 102.1, 200, 200, 200]
    line [1.1, 1.8, 2.4, 9.2, 13.2, 70.6, 111.6, 200, 200, 200]
    line [5.4, 6.4, 4.8, 10.3, 5.4, 8.7, 8.4, 11.2, 13.1, 21.5]
```

> Chart is capped at 200ms. MongoDB actual values at 500K/1M/2M: 563ms, 1,139ms, 2,854ms. MongoFlux stays at 5-21ms regardless of size.

| Docs | MongoDB | Mongo+Index | MongoFlux | Winner |
|:-----|:--------|:------------|:----------|:-------|
| 100 | 1.1 ms | 1.1 ms | 5.4 ms | MongoDB |
| 1K | 2.6 ms | 2.4 ms | 4.8 ms | Mongo+Index |
| 10K | 13.3 ms | 13.2 ms | 5.4 ms | **MongoFlux 2.5x** |
| 100K | 102 ms | 112 ms | 8.4 ms | **MongoFlux 12x** |
| 1M | 1,139 ms | 1,085 ms | 13.1 ms | **MongoFlux 87x** |
| 2M | 2,854 ms | 2,745 ms | 21.5 ms | **MongoFlux 133x** |

**~10K documents is the crossover.** Below that, MongoDB wins because of HTTP round-trip overhead. Above that, MongoFlux wins and the gap keeps growing — columnar scans are O(columns), not O(rows).

Indexes don't help here. They're designed for point lookups, not full-collection aggregations.

### Read performance at scale (500K records)

| Query | MongoDB | MongoFlux | Speedup |
|:------|:--------|:----------|:--------|
| Count by status (GROUP BY) | 823 ms | 16.5 ms | 50x |
| Avg amount by region | 886 ms | 21.5 ms | 41x |
| Top 10 customers by spend | 1,097 ms | 88.6 ms | 12x |
| Date range scan (3 months) | 1,492 ms | 77.6 ms | 19x |
| Full table count | 448 ms | 11.8 ms | 38x |
| Percentile + multi-agg | 1,206 ms | 22.6 ms | 53x |
| Heavy aggregation (uniqExact) | 1,337 ms | 140.5 ms | 10x |

Average: **26.5x faster**. Full results in [`benchmark/`](benchmark/).

### Write overhead

| Metric | Without MongoFlux | With MongoFlux | Overhead |
|:-------|:------------------|:---------------|:---------|
| Batch throughput | 28,639 docs/s | 31,858 docs/s | ~0% |
| Single insert P99 | 8.25 ms | 8.08 ms | ~0% |

Zero. MongoFlux tails the oplog asynchronously — MongoDB acks writes before the sync layer even sees them.

## Quick start

```bash
docker compose up --build
```

This starts MongoDB (3-node replica set on ports 27017-27019), ClickHouse (8123), and MongoFlux (9090). The replica set initializes automatically.

Then create a mapping:

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
      {"mongo_field": "created_at", "ch_column": "created_at", "ch_type": "DateTime"}
    ],
    "engine": "ReplacingMergeTree",
    "order_by": ["created_at", "id"]
  }'

# Create the ClickHouse table
curl -X POST http://localhost:9090/api/v1/mappings/orders/sync
```

That's it. Inserts to `orders` in MongoDB now replicate to ClickHouse in real-time.

## Query translation

MongoFlux translates MongoDB queries to ClickHouse SQL via a two-phase AST:

**Supported `find()` features:** filter, projection, sort, limit, skip

**Supported `aggregate()` stages:** `$match`, `$group`, `$sort`, `$limit`, `$skip`, `$project`, `$addFields`, `$set`, `$unwind`, `$count`, `$sample`

**Filter operators:** `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`, `$in`, `$nin`, `$and`, `$or`, `$nor`, `$exists`, `$regex`

**Accumulators:** `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`, `$addToSet`, `$stdDevPop`, `$stdDevSamp`

**Expressions:** arithmetic (`$add`, `$multiply`, `$subtract`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round`, `$sqrt`, `$pow`), string (`$concat`, `$toUpper`, `$toLower`, `$trim`, `$substr`, `$split`, `$regexMatch`), date (`$year`, `$month`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`), conditional (`$cond`)

## Configuration

```yaml
mongo:
  uri: "mongodb://localhost:27017/?replicaSet=rs0"
  database: "myapp"

clickhouse:
  host: "localhost"
  port: 8123
  database: "analytics"
  user: "default"
  password: ""

sync:
  mode: "oplog"              # or "changestream" for Atlas/sharded
  batch_size: 1000
  flush_interval_ms: 500
  resume_token_path: "/var/lib/mongoflux/resume_tokens"
  max_pending_rows: 100000   # backpressure threshold
  propagate_deletes: false   # tombstone rows on delete
  delete_column: "_deleted"

api:
  port: 9090
  bind: "0.0.0.0"

routing:
  clickhouse_param: "clickhouse"

logging:
  level: "info"
  file: ""
```

| Sync mode | When to use | Requires |
|:----------|:------------|:---------|
| `oplog` | Direct replica set access, lowest latency | `local.oplog.rs` access |
| `changestream` | Atlas, sharded clusters | MongoDB 4.0+ |

Environment variable overrides use the `MG_` prefix: `MG_MONGO_URI`, `MG_CH_HOST`, `MG_CH_PASSWORD`, etc.

## API

| Method | Endpoint | What it does |
|:-------|:---------|:-------------|
| GET | `/api/v1/mappings` | List all mappings |
| GET | `/api/v1/mappings/:collection` | Get one mapping |
| POST | `/api/v1/mappings` | Create/update mapping |
| DELETE | `/api/v1/mappings/:collection` | Delete mapping |
| POST | `/api/v1/mappings/:collection/sync` | Create ClickHouse table |
| GET | `/api/v1/status` | Sync status + health |
| POST | `/api/v1/sync/restart` | Restart sync threads |
| GET | `/health` | Liveness probe |
| GET | `/ready` | Readiness probe (checks CH) |
| GET | `/metrics` | Prometheus metrics |

## Distributed ClickHouse

For multi-shard deployments, add `cluster` and `sharding_key` to your mapping:

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

MongoFlux generates both the local table (`events_local` on each shard) and the Distributed table (`events`) automatically with ON CLUSTER DDL.

## Observability

Prometheus metrics at `/metrics`:

```
mongoflux_rows_synced_total
mongoflux_rows_synced{collection="orders"}
mongoflux_flush_success_total
mongoflux_flush_failure_total
mongoflux_oplog_entries_total
mongoflux_oplog_reconnects_total
mongoflux_pending_rows
mongoflux_oplog_lag_ms
mongoflux_last_flush_duration_ms
mongoflux_sync_running
```

## Building from source

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
./mongoflux /path/to/config.yaml
```

Needs: C++17, CMake 3.16+, mongocxx 3.9+, libcurl, OpenSSL. These are fetched automatically: nlohmann/json, cpp-httplib, yaml-cpp.

## Docker

```bash
docker build -t mongoflux .
docker run -v ./config.yaml:/etc/mongoflux/mongoflux.yaml -p 9090:9090 mongoflux
```

Multi-stage build. Runtime image is minimal Ubuntu 22.04, runs as non-root, uses tini for signal handling. Graceful shutdown flushes pending batches and persists oplog position.

```yaml
# Kubernetes
livenessProbe:
  httpGet: { path: /health, port: 9090 }
readinessProbe:
  httpGet: { path: /ready, port: 9090 }
```

## Running the benchmarks

```bash
pip install pymongo requests

# Find the break-even point
python3 benchmark/breakeven_benchmark.py --max-size 2000000

# Read performance
python3 benchmark/read_benchmark.py --records 1000000 --iterations 5

# Write overhead
python3 benchmark/write_benchmark.py --records 200000

# 20 aggregation patterns
python3 benchmark/aggregation_benchmark.py --records 500000

# Distributed cluster (start cluster first)
docker compose -f benchmark/docker-compose-cluster.yml up -d
python3 benchmark/distributed_benchmark.py --records 500000

# Your own data (auto-discovers schema)
export MONGO_URI="mongodb://user:pass@host:27017/db?authSource=admin"
export MONGO_DB="mydb"
export MONGO_COLLECTION="myCollection"
python3 benchmark/real_data_benchmark.py --limit 500000
```

## Tests

```bash
docker compose up --build -d
python3 test-app/test_mongoflux.py          # run tests
python3 test-app/test_mongoflux.py --cleanup # clean up after
```

Verifies: single/batch insert sync, update propagation, stream continuity, and runs aggregation comparisons.

## License

Apache-2.0
