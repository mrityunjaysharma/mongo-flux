# mg-clickhouse

A MongoDB plugin that transparently routes analytics queries to ClickHouse.

## Architecture

```
┌─────────────┐       ┌──────────────────┐       ┌────────────┐
│  Application│──────▶│   mg-clickhouse   │──────▶│  MongoDB   │
│  (Mongo URI)│       │   (proxy plugin)  │       │  (primary) │
└─────────────┘       └────────┬─────────┘       └─────┬──────┘
                               │                        │
                               │ query translation      │ oplog (same as
                               ▼                        │ secondary replication)
                        ┌────────────┐                  │
                        │ ClickHouse │◀─────────────────┘
                        │ (virtual   │   tailable cursor on
                        │  secondary)│   local.oplog.rs
                        └────────────┘
```

## How It Works

**Replication (same as MongoDB secondary nodes):**

MongoDB secondaries replicate by tailing the primary's oplog (`local.oplog.rs`)
— a capped collection that records every write operation. mg-clickhouse does
exactly the same thing: it opens a tailable-await cursor on the oplog and
applies each operation to ClickHouse. This makes ClickHouse behave as a
"virtual secondary" with the same replication latency as real secondaries.

The oplog tailing approach:
1. Connects to the replica set (same as a secondary joining)
2. Opens a tailable-await cursor on `local.oplog.rs`
3. For each insert/update, extracts mapped fields and batches them
4. Flushes batches to ClickHouse (INSERT via HTTP)
5. Saves the oplog timestamp for crash recovery (like a secondary's checkpoint)

**Write Path:**
- All writes go to MongoDB primary (normal MongoDB behavior)
- mg-clickhouse tails the oplog in real-time (same stream secondaries consume)
- Only mapped fields are synced to ClickHouse

**Read Path:**
- Default (standard MongoDB URI): reads from MongoDB
- With `clickhouse=true` URI option: reads are translated to ClickHouse SQL

**Query Translation:**
- MongoDB `find()` queries → ClickHouse `SELECT` with `WHERE`
- MongoDB `aggregate()` pipelines → ClickHouse `SELECT` with `GROUP BY`, `ORDER BY`, etc.
- Supports `$match`, `$group`, `$sort`, `$limit`, `$project`, `$unwind`

## Schema Mapping

Define which MongoDB collection fields map to ClickHouse columns via the management API:

```bash
# Create a mapping
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
    "engine": "MergeTree",
    "order_by": ["created_at", "id"]
  }'
```

## Build

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Dependencies

- C++17 compiler
- MongoDB C++ Driver (mongocxx 3.x)
- ClickHouse C++ Client (clickhouse-cpp)
- libcurl
- nlohmann/json
- cpp-httplib (for management API)

## Configuration

```yaml
# mg-clickhouse.yaml
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
  batch_size: 1000
  flush_interval_ms: 500

api:
  port: 9090
  bind: "0.0.0.0"

routing:
  # URI parameter that triggers ClickHouse reads
  clickhouse_param: "clickhouse"
```

## Usage

```cpp
// Standard MongoDB read (from MongoDB)
auto client = mongocxx::client{mongocxx::uri{"mongodb://localhost:27017"}};

// Analytics read (routed to ClickHouse)
auto client = mongocxx::client{mongocxx::uri{"mongodb://localhost:27017/?clickhouse=true"}};
```

## Benchmark: MongoDB vs ClickHouse Reads

Benchmark run on 200,000 records per collection, 5 iterations per query.
Full results in [`benchmark/results.json`](benchmark/results.json).

| Query | MongoDB (ms) | ClickHouse (ms) | Speedup |
|:------|:-------------|:----------------|:--------|
| Simple filter (status = 'shipped') | 5.8 | 12.1 | 0.5x |
| Range scan (amount > 1000) | 16.0 | 14.4 | 1.1x |
| Count by status (GROUP BY) | 154.2 | 10.8 | **14.3x** |
| Avg amount by region (GROUP BY) | 126.9 | 8.3 | **15.4x** |
| Top 10 customers by total spend | 180.8 | 16.3 | **11.1x** |
| Multi-filter (region + status + amount range) | 29.4 | 8.1 | 3.6x |
| Date range scan (3 months) | 268.3 | 14.3 | **18.7x** |
| Events: count by type and page (2-dim GROUP BY) | 175.3 | 8.7 | **20.2x** |
| Events: avg duration by event type | 172.1 | 10.6 | **16.2x** |
| Events: top 20 users by event count | 169.9 | 10.4 | **16.4x** |
| Full table scan count | 65.6 | 5.0 | **13.2x** |
| Percentile approximation (heavy analytics) | 155.3 | 12.4 | **12.5x** |

**Average speedup: 11.9x** for analytical queries. MongoDB wins on simple indexed point lookups where HTTP round-trip overhead dominates. ClickHouse dominates on aggregations, scans, and GROUP BY operations.

### Running the Benchmark

```bash
pip install pymongo requests
python3 benchmark/read_benchmark.py --records 200000 --iterations 5
```

Options:
- `--records N` — number of records per collection (default: 100,000)
- `--iterations N` — query repetitions for stable timing (default: 5)
- `--skip-load` — reuse existing data without reloading

## License

Apache-2.0
