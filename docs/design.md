# MongoFlux — Design Document

**v1.2** | 2026-05-15 | Production

## 1. What It Does

MongoFlux replicates MongoDB to ClickHouse in real-time using oplog tailing (same as MongoDB secondaries), and routes analytical reads to ClickHouse transparently via a single URI parameter.

**Goals**: Zero write overhead, sub-second replication, no app code changes, standalone + clustered ClickHouse.

**Non-goals**: Full wire protocol proxy, real-time deletes, multi-tenancy.

## 2. Architecture

MongoDB runs as a 3-node replica set (1 primary + 2 secondaries). For reads, the application connects to MongoFlux which acts as a routing proxy. If the connection URI contains `?clickhouse=true`, MongoFlux translates the query to SQL and sends it to ClickHouse. If the parameter is absent or false, MongoFlux forwards the query to MongoDB Primary unchanged. Writes go directly to the primary.

```mermaid
flowchart TD
    APP[Application] -->|writes| MGP[(MongoDB Primary)]
    MGP -->|replication| MGS1[(MongoDB Secondary 1)]
    MGP -->|replication| MGS2[(MongoDB Secondary 2)]
    APP -->|"all reads (find/aggregate)"| ROUTER[MongoFlux<br/>Query Router]
    ROUTER -->|"?clickhouse=true → translate"| XLAT[Query Translator<br/>BSON → AST → SQL]
    XLAT -->|SELECT| CH[(ClickHouse)]
    ROUTER -->|"no flag → forward"| MGP
    MGP -->|oplog.rs| SYNC[Oplog / CS Sync]
    SYNC -->|batch INSERT| CH
    SYNC -.->|lookup| REG[Schema Registry]
    XLAT -.->|lookup| REG
```

## 3. Data Flow

### Writes (zero overhead)

Writes target the MongoDB primary. The two secondaries replicate via the standard MongoDB replication protocol. MongoFlux tails the primary's oplog independently.

```mermaid
sequenceDiagram
    participant App
    participant Primary as MongoDB Primary
    participant S1 as MongoDB Secondary 1
    participant S2 as MongoDB Secondary 2
    participant mf as MongoFlux
    participant CH as ClickHouse

    App->>Primary: insert/update
    Primary-->>App: ack ✓
    Primary->>S1: replication (async)
    Primary->>S2: replication (async)
    Note right of App: Done. MongoFlux<br/>not in write path.
    Primary->>mf: oplog entry (async)
    mf->>CH: batch INSERT
    mf->>mf: save position
```

### Reads (query routing)

The application sends all reads to MongoFlux. MongoFlux inspects the URI for `?clickhouse=true`. If present, it translates the query to SQL and executes on ClickHouse. If absent or false, it forwards the query to MongoDB Primary unchanged.

```mermaid
sequenceDiagram
    App->>MongoFlux: find({status:"shipped"}, ?clickhouse=true)
    MongoFlux->>MongoFlux: BSON → AST → SQL
    MongoFlux->>ClickHouse: SELECT * WHERE status='shipped'
    ClickHouse-->>App: results
```

```mermaid
sequenceDiagram
    App->>MongoFlux: find({_id: "abc123"}) [no clickhouse param]
    MongoFlux->>MongoDB Primary: forward query unchanged
    MongoDB Primary-->>App: results
```

## 4. Components

### 4.1 Oplog Sync

Tails `local.oplog.rs` with a tailable-await cursor. Same mechanism MongoDB secondaries use.

- Position saved AFTER successful flush (at-least-once delivery)
- Failed batches retained for retry (no data loss)
- Reconnects with 3s backoff on cursor death
- Configurable: `batch_size` (throughput) and `flush_interval_ms` (latency)

### 4.2 Query Translator (AST)

Two-phase: Parse BSON → ExprNode tree → Emit ClickHouse SQL.

```mermaid
graph LR
    BSON[BSON] --> |parse| TREE[ExprNode Tree] --> |emit| SQL[SQL]
```

Supports: `find()`, `aggregate($match, $group, $sort, $limit, $project, $count)`, operators `$gt/$lt/$in/$or/$regex/$exists`.

### 4.3 Schema Registry

Thread-safe `unordered_map<collection, CollectionMapping>` with mutex. Persisted to `mappings.json`. Shared by sync engine, translator, and API.

### 4.4 Cluster Support

| Mode | Config | What happens |
|:-----|:-------|:-------------|
| Standalone | No `cluster` field | Single MergeTree table |
| Clustered | `"cluster": "prod"` | Local table + Distributed table (auto-routes inserts to shards) |

### 4.5 Shared Utilities (`bson_utils.h`)

Three functions eliminate all code duplication between oplog sync and change stream sync:

- `extract_mapped_fields()` — BSON doc → JSON row
- `escape_ch_string()` — SQL string literal escaping
- `prepare_batch_for_insert()` — JSON batch → columns + values for INSERT

## 5. Crash Recovery

```mermaid
stateDiagram-v2
    [*] --> Tailing
    Tailing --> Flushing: batch full / interval
    Flushing --> Tailing: OK → save position
    Flushing --> Tailing: FAIL → keep rows, retry
    Tailing --> [*]: SIGTERM → flush + save + exit
```

Position saved only after successful flush. Crash before save → replay from last position. `ReplacingMergeTree` deduplicates replays.

## 6. Performance

### Reads (500K records)

| Query | MongoDB | Standalone CH | Distributed CH (3 shards) |
|:------|:--------|:--------------|:--------------------------|
| GROUP BY count | 823 ms | 17 ms (50x) | 61 ms (13x) |
| Avg by region | 886 ms | 22 ms (41x) | 44 ms (20x) |
| Top 10 customers | 1,097 ms | 89 ms (12x) | 178 ms (6x) |
| Date range scan | 1,492 ms | 78 ms (19x) | 63 ms (24x) |
| Full count | 448 ms | 12 ms (38x) | 37 ms (12x) |
| Percentile + agg | 1,206 ms | 23 ms (53x) | 58 ms (21x) |

Standalone avg: **26.5x** | Distributed avg: **12.3x** | Distributed wins on large parallel scans.

### Writes (200K records)

| Metric | MongoDB alone | With MongoFlux | Overhead |
|:-------|:-------------|:-------------------|:---------|
| Batch throughput | 28,639 docs/s | 31,858 docs/s | **0%** |
| Single insert P99 | 8.25 ms | 8.08 ms | **0%** |

## 7. Configuration

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
  cluster: ""              # empty = standalone

sync:
  mode: "oplog"            # or "changestream"
  batch_size: 1000
  flush_interval_ms: 500
  resume_token_path: "/var/lib/MongoFlux/resume_tokens"

api:
  port: 9090
  bind: "0.0.0.0"

routing:
  clickhouse_param: "clickhouse"
```

Validated at startup: required fields, port ranges (1-65535), batch_size (1-1M), flush_interval (1-60000), mode must be `oplog` or `changestream`.

## 8. Deployment

The default deployment uses a 3-node MongoDB replica set (`rs0`) with 1 primary and 2 secondaries. MongoFlux connects using the full replica set URI and automatically follows primary elections.

```mermaid
flowchart LR
    APP[Apps] --> MGP[(MongoDB Primary)]
    MGP --- MGS1[(Secondary 1)]
    MGP --- MGS2[(Secondary 2)]
    MGP -->|oplog| MGC[MongoFlux]
    MGC -->|INSERT| LB[CH Load Balancer]
    LB --> S1[Shard 1]
    LB --> S2[Shard 2]
    LB --> S3[Shard 3]
```

- Docker: non-root `mongoflux`, `tini` PID 1, graceful shutdown
- Docker Compose: 3 MongoDB nodes (`mongo-primary`, `mongo-secondary1`, `mongo-secondary2`) with automatic `rs.initiate()`
- K8s: `/health` (liveness), `/ready` (readiness, checks CH)
- Security: RAII CURL, URL-encoded credentials, no built-in API auth (use gateway)

## 9. Failure Modes

| Failure | Recovery | Data Loss |
|:--------|:---------|:----------|
| MongoFlux crash | Resume from saved position | None |
| ClickHouse down | Retry on next flush | None |
| MongoDB primary failover | Driver auto-discovers new primary via replica set URI (3s backoff) | None |
| MongoDB secondary down | No impact — MongoFlux tails primary only | None |
| Corrupted token | Start from oplog tail | Possible gap |

## 10. Roadmap

| Item | Priority |
|:-----|:---------|
| HA via leader election | High |
| Initial bulk sync | High |
| Partial update support ($set) | Medium |
| Prometheus metrics | Medium |
| API authentication (JWT/mTLS) | Medium |
| Soft-delete propagation | Low |

## 11. File Structure

```
include/mongoflux/
  config.h, schema_mapping.h, expr_tree.h, query_translator.h,
  clickhouse_client.h, oplog_sync.h, change_stream_sync.h,
  mongo_proxy.h, management_api.h, routing.h, bson_utils.h

src/
  main.cpp, config.cpp, schema_mapping.cpp, expr_tree.cpp,
  query_translator.cpp, clickhouse_client.cpp, oplog_sync.cpp,
  change_stream_sync.cpp, mongo_proxy.cpp, management_api.cpp, routing.cpp

benchmark/
  read_benchmark.py, write_benchmark.py, distributed_benchmark.py
```
