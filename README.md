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

## License

Apache-2.0
