# mg-clickhouse

Real-time MongoDB to ClickHouse replication with transparent analytics query routing. mg-clickhouse makes ClickHouse behave as a "virtual secondary" that receives the same write stream as MongoDB replica set members, while transparently routing analytical reads to ClickHouse via a single URI parameter.

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

## Key Features

- Real-time CDC replication via oplog tailing (same mechanism as MongoDB secondaries)
- Transparent query routing with a single URI parameter (`?clickhouse=true`)
- MongoDB `find()` and `aggregate()` to ClickHouse SQL translation via expression tree AST
- Schema mapping management through a REST API
- Crash recovery with persisted oplog timestamps
- Dual sync modes: direct oplog tailing or change streams (for Atlas/sharded clusters)
- Health and readiness probes for Kubernetes deployments

## How It Works

### Replication

MongoDB secondaries replicate by tailing the primary's oplog (`local.oplog.rs`), a capped collection that records every write operation. mg-clickhouse does exactly the same thing: it opens a tailable-await cursor on the oplog and applies each operation to ClickHouse. This gives ClickHouse the same replication latency as real secondaries.

The sync pipeline:

1. Connects to the replica set (same as a secondary joining)
2. Opens a tailable-await cursor on `local.oplog.rs`
3. For each insert/update, extracts mapped fields and batches them
4. Flushes batches to ClickHouse via HTTP INSERT
5. Persists the oplog timestamp for crash recovery (like a secondary's checkpoint)

### Write Path

All writes go to MongoDB primary (normal MongoDB behavior). mg-clickhouse tails the oplog in real-time and syncs only mapped fields to ClickHouse. The application does not need any code changes for writes.

### Read Path

Reads are routed based on the connection URI:

- Standard URI → reads from MongoDB (default, no change required)
- URI with `?clickhouse=true` → reads are translated to ClickHouse SQL

### Query Translation

The translator uses a two-phase architecture with an expression tree (AST):

1. **Parse phase**: BSON query documents are parsed into an `ExprNode` tree
2. **Emit phase**: The tree is walked to produce ClickHouse SQL

Supported operations:

| MongoDB | ClickHouse |
|:--------|:-----------|
| `find()` with filter, projection, sort, limit, skip | `SELECT ... WHERE ... ORDER BY ... LIMIT` |
| `aggregate()` with `$match` | `WHERE` / `HAVING` |
| `aggregate()` with `$group` | `GROUP BY` with aggregation functions |
| `aggregate()` with `$sort`, `$limit` | `ORDER BY`, `LIMIT` |
| `aggregate()` with `$project` | `SELECT` column list |
| `aggregate()` with `$count` | `SELECT count(*)` |
| Filter operators: `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne` | Comparison operators |
| `$in`, `$nin` | `IN (...)`, `NOT IN (...)` |
| `$and`, `$or`, `$nor` | `AND`, `OR`, `NOT (... OR ...)` |
| `$exists` | `IS NULL` / `IS NOT NULL` |
| `$regex` | `match()` |

## Quick Start

### Docker Compose (recommended)

```bash
docker compose up --build
```

This starts MongoDB (replica set), ClickHouse, and mg-clickhouse. The management API is available at `http://localhost:9090`.

### Create a Schema Mapping

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
      {"mongo_field": "status", "ch_column": "status", "ch_type": "String"},
      {"mongo_field": "created_at", "ch_column": "created_at", "ch_type": "DateTime"}
    ],
    "engine": "ReplacingMergeTree",
    "order_by": ["created_at", "id"]
  }'
```

### Provision the ClickHouse Table

```bash
curl -X POST http://localhost:9090/api/v1/mappings/orders/sync
```

### Verify Sync Status

```bash
curl http://localhost:9090/api/v1/status
```

## Management API

| Method | Endpoint | Description |
|:-------|:---------|:------------|
| `GET` | `/api/v1/mappings` | List all schema mappings |
| `GET` | `/api/v1/mappings/:collection` | Get mapping for a collection |
| `POST` | `/api/v1/mappings` | Create or update a mapping |
| `DELETE` | `/api/v1/mappings/:collection` | Delete a mapping |
| `POST` | `/api/v1/mappings/:collection/sync` | Create ClickHouse table from mapping |
| `GET` | `/api/v1/status` | System health and sync status |
| `POST` | `/api/v1/sync/restart` | Restart all sync threads |
| `GET` | `/health` | Liveness probe (always 200) |
| `GET` | `/ready` | Readiness probe (checks ClickHouse connectivity) |

## Build from Source

### Prerequisites

- C++17 compiler (GCC 9+ or Clang 10+)
- CMake 3.16+
- MongoDB C Driver (libmongoc 1.28+)
- MongoDB C++ Driver (mongocxx 3.9+)
- libcurl with OpenSSL
- pkg-config

The following are fetched automatically via CMake FetchContent:

- nlohmann/json 3.11
- cpp-httplib 0.15
- yaml-cpp 0.8

### Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run

```bash
./build/mg_clickhouse config/mg-clickhouse.yaml
```

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
  mode: "oplog"           # "oplog" (direct tailing) or "changestream" (Atlas/sharded)
  batch_size: 1000        # Max rows per ClickHouse INSERT batch
  flush_interval_ms: 500  # Max time before flushing a partial batch
  resume_token_path: "/var/lib/mg-clickhouse/resume_tokens"

api:
  port: 9090
  bind: "0.0.0.0"

routing:
  clickhouse_param: "clickhouse"  # URI parameter that triggers ClickHouse reads

logging:
  level: "info"           # debug, info, warn, error
  file: ""                # Empty = stdout
```

### Sync Modes

| Mode | Use Case | Requirements |
|:-----|:---------|:-------------|
| `oplog` | Direct replica set access, lowest latency | Direct access to `local.oplog.rs` |
| `changestream` | MongoDB Atlas, sharded clusters | MongoDB 4.0+, change streams enabled |

## Deployment

### Docker

```bash
docker build -t mg-clickhouse .
docker run -v ./config/mg-clickhouse.yaml:/etc/mg-clickhouse/mg-clickhouse.yaml \
  -p 9090:9090 mg-clickhouse
```

### Kubernetes

The container exposes health endpoints for probe configuration:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

The container runs as non-root user `mgch` and uses `tini` as PID 1 for proper signal handling.

### Graceful Shutdown

mg-clickhouse handles `SIGINT` and `SIGTERM` for graceful shutdown:

1. Stops accepting new API requests
2. Flushes pending batches to ClickHouse
3. Persists the current oplog position
4. Closes all connections

## Benchmark: MongoDB vs ClickHouse Reads

Benchmark run on 200,000 records per collection, 5 iterations per query (median of runs).
Full results in [`benchmark/results.json`](benchmark/results.json).

| Query | MongoDB (ms) | ClickHouse (ms) | Speedup |
|:------|:-------------|:----------------|:--------|
| Simple filter (indexed point lookup) | 5.8 | 12.1 | 0.5x |
| Range scan (amount > 1000) | 16.0 | 14.4 | 1.1x |
| Count by status (GROUP BY) | 154.2 | 10.8 | 14.3x |
| Avg amount by region (GROUP BY) | 126.9 | 8.3 | 15.4x |
| Top 10 customers by total spend | 180.8 | 16.3 | 11.1x |
| Multi-filter compound query | 29.4 | 8.1 | 3.6x |
| Date range scan (3 months) | 268.3 | 14.3 | 18.7x |
| 2-dimensional GROUP BY (events) | 175.3 | 8.7 | 20.2x |
| Avg duration by event type | 172.1 | 10.6 | 16.2x |
| Top 20 users by event count | 169.9 | 10.4 | 16.4x |
| Full table count | 65.6 | 5.0 | 13.2x |
| Percentile + multi-agg (heavy analytics) | 155.3 | 12.4 | 12.5x |

**Average speedup: 11.9x** for analytical queries. MongoDB retains an advantage on simple indexed point lookups where HTTP round-trip overhead dominates. ClickHouse dominates on aggregations, full scans, and GROUP BY operations — the gap widens further at larger data volumes due to columnar compression and vectorized execution.

### Running the Benchmark

```bash
pip install pymongo requests
python3 benchmark/read_benchmark.py --records 200000 --iterations 5
```

| Flag | Description | Default |
|:-----|:------------|:--------|
| `--records N` | Records per collection | 100,000 |
| `--iterations N` | Query repetitions for stable timing | 5 |
| `--skip-load` | Reuse existing data without reloading | off |

## Project Structure

```
mg-clickhouse/
├── include/mg_clickhouse/     # Public headers
│   ├── config.h               # Configuration structs and loader
│   ├── schema_mapping.h       # Collection-to-table mapping registry
│   ├── expr_tree.h            # Expression tree AST for query translation
│   ├── query_translator.h     # BSON → ExprTree → SQL translator
│   ├── clickhouse_client.h    # ClickHouse HTTP client
│   ├── oplog_sync.h           # Oplog-based CDC replication
│   ├── change_stream_sync.h   # Change stream CDC (Atlas/sharded)
│   ├── mongo_proxy.h          # Read routing proxy
│   ├── management_api.h       # REST API server
│   └── routing.h              # URI parsing and routing detection
├── src/                       # Implementation files
├── config/                    # Example configuration files
├── benchmark/                 # Performance benchmarking tools
├── CMakeLists.txt             # Build configuration
├── Dockerfile                 # Multi-stage production image
└── docker-compose.yml         # Local development stack
```

## License

Apache-2.0
