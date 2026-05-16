#!/usr/bin/env python3
"""
Real Data Benchmark: MongoDB vs ClickHouse

Connects to an existing MongoDB collection, auto-discovers the schema,
loads data into local ClickHouse, and benchmarks analytical queries on both.

Usage:
    export MONGO_URI="mongodb://user:pass@host:27017/db?authSource=admin"
    export MONGO_DB="mydb"
    export MONGO_COLLECTION="myCollection"

    python3 benchmark/real_data_benchmark.py --limit 500000 --iterations 3

Environment Variables:
    MONGO_URI          - Full MongoDB connection URI (required)
    MONGO_DB           - Database name (required)
    MONGO_COLLECTION   - Collection name (required)
    CH_URL             - ClickHouse HTTP URL (default: http://localhost:8123)
    CH_DB              - ClickHouse database (default: real_benchmark)
"""

import argparse
import json
import logging
import os
import time
import statistics
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from bson import ObjectId
except ImportError:
    sys.exit("ERROR: pymongo not installed. Run: pip install pymongo")

try:
    import pymongo
except ImportError:
    sys.exit("ERROR: pymongo not installed. Run: pip install pymongo")

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    sys.exit("ERROR: requests not installed. Run: pip install requests")

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================
# Configuration from environment
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB = os.environ.get("MONGO_DB", "")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "")
CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_DB = os.environ.get("CH_DB", "real_benchmark")

# HTTP session with connection pooling
SESSION = requests.Session()
SESSION.mount("http://", HTTPAdapter(pool_connections=5, pool_maxsize=5))
REQUEST_TIMEOUT = 30


def ch_query(sql: str) -> str:
    """Execute a ClickHouse query. Raises RuntimeError on failure."""
    resp = SESSION.post(CH_URL, data=sql, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error ({resp.status_code}): {resp.text.strip()[:300]}")
    return resp.text


# ============================================================
# Schema Discovery
# ============================================================

def discover_schema(coll, sample_size=1000):
    """Sample documents to discover field names and types."""
    print(f"    Sampling {sample_size} documents for schema discovery...")
    pipeline = [{"$sample": {"size": sample_size}}]
    samples = list(coll.aggregate(pipeline))

    if not samples:
        sys.exit("ERROR: Collection is empty or inaccessible")

    # Discover fields and their most common types
    field_types = {}
    for doc in samples:
        for key, value in doc.items():
            if key == "_id":
                field_types[key] = "ObjectId"
                continue
            t = type(value).__name__
            if key not in field_types:
                field_types[key] = {}
            if isinstance(field_types[key], dict):
                field_types[key][t] = field_types[key].get(t, 0) + 1
            # Keep most common type
    
    # Resolve to single type per field
    schema = {}
    for key, types in field_types.items():
        if key == "_id":
            schema[key] = "String"
            continue
        if isinstance(types, dict):
            dominant = max(types, key=types.get)
            schema[key] = map_python_type_to_ch(dominant)
        else:
            schema[key] = "Nullable(String)"

    return schema, samples[0]


def map_python_type_to_ch(python_type):
    """Map Python type names to ClickHouse types."""
    mapping = {
        "str": "Nullable(String)",
        "int": "Nullable(Int64)",
        "float": "Nullable(Float64)",
        "bool": "Nullable(UInt8)",
        "datetime": "Nullable(DateTime)",
        "date": "Nullable(Date)",
        "list": "Nullable(String)",  # JSON-serialize arrays
        "dict": "Nullable(String)",  # JSON-serialize nested docs
        "ObjectId": "Nullable(String)",
        "NoneType": "Nullable(String)",
        "Decimal128": "Nullable(Float64)",
    }
    return mapping.get(python_type, "Nullable(String)")


# ============================================================
# Data Loading (MongoDB → ClickHouse)
# ============================================================

def load_to_clickhouse(coll, schema, limit):
    """Read from MongoDB and load into ClickHouse — all fields as String for safety."""
    table_name = MONGO_COLLECTION.lower().replace("-", "_")

    # Create database
    ch_query(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
    ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{table_name}")

    # Build CREATE TABLE DDL — use String for everything to avoid type mismatches
    columns = []
    ch_fields = []
    for field, ch_type in schema.items():
        safe_name = field.replace(".", "_").replace("$", "").replace(" ", "_")
        if safe_name and safe_name[0].isdigit():
            safe_name = "f_" + safe_name
        # Force all to Nullable(String) except _id
        actual_type = "String" if field == "_id" else "Nullable(String)"
        columns.append(f"    `{safe_name}` {actual_type}")
        ch_fields.append((field, safe_name, actual_type))

    ddl = f"CREATE TABLE {CH_DB}.{table_name} (\n"
    ddl += ",\n".join(columns)
    ddl += f"\n) ENGINE = MergeTree()\nORDER BY tuple()\n"

    print(f"    Creating ClickHouse table: {CH_DB}.{table_name}")
    ch_query(ddl)

    # Stream data from MongoDB to ClickHouse in batches
    print(f"    Loading up to {limit:,} documents...")
    batch_size = 5000
    total_loaded = 0
    cursor = coll.find({}).limit(limit)

    batch = []
    for doc in cursor:
        row = {}
        for mongo_field, ch_name, ch_type in ch_fields:
            val = doc.get(mongo_field)
            if val is None:
                row[ch_name] = None
            elif isinstance(val, ObjectId):
                row[ch_name] = str(val)
            elif isinstance(val, datetime):
                row[ch_name] = val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            elif isinstance(val, (list, dict)):
                row[ch_name] = json.dumps(val, default=str)
            elif isinstance(val, bool):
                row[ch_name] = str(val).lower()
            else:
                row[ch_name] = str(val)

        batch.append(row)

        if len(batch) >= batch_size:
            _flush_batch(table_name, batch)
            total_loaded += len(batch)
            if total_loaded % 50000 == 0:
                print(f"      {total_loaded:,} loaded...")
            batch = []

    if batch:
        _flush_batch(table_name, batch)
        total_loaded += len(batch)

    print(f"    Total loaded: {total_loaded:,} documents")
    return table_name, ch_fields, total_loaded


def _flush_batch(table_name, batch):
    """Insert a batch into ClickHouse."""
    payload = "\n".join(json.dumps(row, default=str) for row in batch)
    url = f"{CH_URL}/?query=INSERT+INTO+{CH_DB}.{table_name}+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1&input_format_null_as_default=1"
    resp = requests.post(url, data=payload.encode("utf-8"))
    if resp.status_code != 200:
        raise RuntimeError(f"Insert error: {resp.text.strip()[:300]}")


# ============================================================
# Auto-Generate Benchmark Queries
# ============================================================

def generate_queries(schema, ch_fields, table_name, sample_doc):
    """Generate up to 20 analytical queries dynamically from discovered schema."""
    queries = []

    # Classify fields by type for query generation
    all_fields = [(mf, cn) for mf, cn, ct in ch_fields if mf != "_id"]
    string_fields = [(mf, cn) for mf, cn, ct in ch_fields
                     if mf != "_id" and "String" in ct]
    numeric_fields = [(mf, cn) for mf, cn, ct in ch_fields
                      if any(t in ct for t in ("Int", "Float", "UInt"))]
    date_fields = [(mf, cn) for mf, cn, ct in ch_fields
                   if any(t in ct for t in ("Date", "DateTime"))]

    # If all fields are Nullable(String), use heuristics to classify
    if not numeric_fields:
        numeric_fields = [(mf, cn) for mf, cn, ct in ch_fields
                          if mf in ("value", "count", "amount", "score", "total", "size")]
    if not date_fields:
        date_fields = [(mf, cn) for mf, cn, ct in ch_fields
                       if any(kw in mf.lower() for kw in ("date", "time", "at", "created", "updated"))]

    # Pick good GROUP BY candidates (low-cardinality string fields)
    low_card_keywords = ["status", "type", "category", "region", "segment", "level",
                         "priority", "source", "mode", "role", "env", "tier", "kind"]
    group_fields = []
    for mf, cn in string_fields:
        if any(kw in mf.lower() for kw in low_card_keywords):
            group_fields.append((mf, cn))
    # Fill with remaining string fields
    for mf, cn in string_fields:
        if len(group_fields) >= 10:
            break
        if (mf, cn) not in group_fields:
            group_fields.append((mf, cn))

    # Helper to get CH column name
    def col(mongo_field):
        for mf, cn, ct in ch_fields:
            if mf == mongo_field:
                return cn
        return mongo_field

    # 1. Full count
    queries.append({
        "name": "1. Total document count",
        "mongo": [{"$count": "total"}],
        "ch": f"SELECT count() as total FROM {CH_DB}.{table_name}",
    })

    # 2-5. GROUP BY on discovered fields
    for i, (mf, cf) in enumerate(group_fields[:4]):
        queries.append({
            "name": f"{len(queries)+1}. Count by {mf}",
            "mongo": [{"$group": {"_id": f"${mf}", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT `{cf}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{cf}` ORDER BY cnt DESC",
        })

    # 6. 2D GROUP BY
    if len(group_fields) >= 2:
        mf1, cf1 = group_fields[0]
        mf2, cf2 = group_fields[1]
        queries.append({
            "name": f"{len(queries)+1}. 2D GROUP BY ({mf1} × {mf2})",
            "mongo": [{"$group": {"_id": {"a": f"${mf1}", "b": f"${mf2}"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT `{cf1}`, `{cf2}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{cf1}`, `{cf2}` ORDER BY cnt DESC",
        })

    # 7. 3D GROUP BY
    if len(group_fields) >= 3:
        mf1, cf1 = group_fields[0]
        mf2, cf2 = group_fields[1]
        mf3, cf3 = group_fields[2]
        queries.append({
            "name": f"{len(queries)+1}. 3D GROUP BY ({mf1} × {mf2} × {mf3})",
            "mongo": [{"$group": {"_id": {"a": f"${mf1}", "b": f"${mf2}", "c": f"${mf3}"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT `{cf1}`, `{cf2}`, `{cf3}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{cf1}`, `{cf2}`, `{cf3}` ORDER BY cnt DESC",
        })

    # 8. Top N
    if group_fields:
        mf, cf = group_fields[0]
        queries.append({
            "name": f"{len(queries)+1}. Top 20 by {mf}",
            "mongo": [{"$group": {"_id": f"${mf}", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 20}],
            "ch": f"SELECT `{cf}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{cf}` ORDER BY cnt DESC LIMIT 20",
        })

    # 9. Unique count (cardinality)
    if len(string_fields) >= 2:
        mf_group, cf_group = group_fields[0]
        mf_uniq, cf_uniq = string_fields[1] if string_fields[1] != group_fields[0] else string_fields[0]
        queries.append({
            "name": f"{len(queries)+1}. Unique {mf_uniq} per {mf_group}",
            "mongo": [{"$group": {"_id": f"${mf_group}", "uniq": {"$addToSet": f"${mf_uniq}"}}}, {"$project": {"field": "$_id", "count": {"$size": "$uniq"}}}],
            "ch": f"SELECT `{cf_group}`, uniqExact(`{cf_uniq}`) as uniq_count FROM {CH_DB}.{table_name} GROUP BY `{cf_group}`",
        })

    # 10. Numeric aggregation
    if numeric_fields:
        mf_n, cf_n = numeric_fields[0]
        queries.append({
            "name": f"{len(queries)+1}. Stats on {mf_n} (avg/min/max)",
            "mongo": [{"$group": {"_id": None, "avg": {"$avg": f"${mf_n}"}, "min": {"$min": f"${mf_n}"}, "max": {"$max": f"${mf_n}"}}}],
            "ch": f"SELECT avg(toFloat64OrZero(`{cf_n}`)) as avg_val, min(toFloat64OrZero(`{cf_n}`)) as min_val, max(toFloat64OrZero(`{cf_n}`)) as max_val FROM {CH_DB}.{table_name}",
        })

    # 11. Numeric by group
    if numeric_fields and group_fields:
        mf_n, cf_n = numeric_fields[0]
        mf_g, cf_g = group_fields[0]
        queries.append({
            "name": f"{len(queries)+1}. Avg {mf_n} by {mf_g}",
            "mongo": [{"$group": {"_id": f"${mf_g}", "avg_val": {"$avg": f"${mf_n}"}, "count": {"$sum": 1}}}, {"$sort": {"avg_val": -1}}],
            "ch": f"SELECT `{cf_g}`, avg(toFloat64OrZero(`{cf_n}`)) as avg_val, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{cf_g}` ORDER BY avg_val DESC",
        })

    # 12. Filter + aggregate
    if group_fields and sample_doc:
        mf, cf = group_fields[0]
        sample_val = sample_doc.get(mf)
        if sample_val and isinstance(sample_val, str):
            escaped = sample_val.replace("'", "\\'")
            queries.append({
                "name": f"{len(queries)+1}. Filter {mf}='{sample_val[:15]}' + count",
                "mongo": [{"$match": {mf: sample_val}}, {"$count": "total"}],
                "ch": f"SELECT count() as total FROM {CH_DB}.{table_name} WHERE `{cf}` = '{escaped}'",
            })

    # 13. Conditional count (resolution/completion rate)
    if group_fields:
        mf_g, cf_g = group_fields[0]
        # Find a status-like field
        status_field = None
        for mf, cf in group_fields:
            if "status" in mf.lower():
                status_field = (mf, cf)
                break
        if status_field and status_field != (mf_g, cf_g):
            mf_s, cf_s = status_field
            # Get a sample value for the conditional
            sample_status = sample_doc.get(mf_s, "")
            if sample_status:
                queries.append({
                    "name": f"{len(queries)+1}. Rate of {mf_s}='{sample_status[:10]}' by {mf_g}",
                    "mongo": [{"$group": {"_id": f"${mf_g}", "total": {"$sum": 1}, "matched": {"$sum": {"$cond": [{"$eq": [f"${mf_s}", sample_status]}, 1, 0]}}}}],
                    "ch": f"SELECT `{cf_g}`, count() as total, countIf(`{cf_s}` = '{sample_status}') as matched, matched / total as rate FROM {CH_DB}.{table_name} GROUP BY `{cf_g}`",
                })

    # 14. Date-based grouping
    if date_fields:
        mf_d, cf_d = date_fields[0]
        queries.append({
            "name": f"{len(queries)+1}. Count by month ({mf_d})",
            "mongo": [{"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": f"${mf_d}"}}, "count": {"$sum": 1}}}, {"$sort": {"_id": -1}}, {"$limit": 12}],
            "ch": f"SELECT substring(`{cf_d}`, 1, 7) as month, count() as cnt FROM {CH_DB}.{table_name} WHERE `{cf_d}` IS NOT NULL AND `{cf_d}` != '' GROUP BY month ORDER BY month DESC LIMIT 12",
        })

    # 15-16. Multi-filter queries
    if len(group_fields) >= 2 and sample_doc:
        mf1, cf1 = group_fields[0]
        mf2, cf2 = group_fields[1]
        val1 = sample_doc.get(mf1)
        val2 = sample_doc.get(mf2)
        if val1 and val2 and isinstance(val1, str) and isinstance(val2, str):
            queries.append({
                "name": f"{len(queries)+1}. Multi-filter ({mf1} + {mf2})",
                "mongo": [{"$match": {mf1: val1, mf2: val2}}, {"$count": "total"}],
                "ch": f"SELECT count() as total FROM {CH_DB}.{table_name} WHERE `{cf1}` = '{val1}' AND `{cf2}` = '{val2}'",
            })

    # Fill remaining slots with additional GROUP BY combinations
    for i in range(4, min(len(group_fields), 8)):
        if len(queries) >= 20:
            break
        mf, cf = group_fields[i]
        queries.append({
            "name": f"{len(queries)+1}. Count by {mf}",
            "mongo": [{"$group": {"_id": f"${mf}", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT `{cf}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{cf}` ORDER BY cnt DESC",
        })

    return queries[:20]


# ============================================================
# Benchmark Runner
# ============================================================

def run_benchmark(coll, queries, iterations):
    results = []

    for q in queries:
        mongo_times = []
        ch_times = []

        # Warmup
        try:
            list(coll.aggregate(q["mongo"], allowDiskUse=True))
        except Exception:
            pass
        try:
            ch_query(q["ch"])
        except Exception:
            pass

        for _ in range(iterations):
            # MongoDB
            start = time.perf_counter()
            try:
                list(coll.aggregate(q["mongo"], allowDiskUse=True))
                mongo_times.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                mongo_times.append(float("inf"))

            # ClickHouse
            start = time.perf_counter()
            try:
                ch_query(q["ch"])
                ch_times.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                ch_times.append(float("inf"))

        m_avg = statistics.mean(mongo_times) if mongo_times else float("inf")
        c_avg = statistics.mean(ch_times) if ch_times else float("inf")

        results.append({
            "query": q["name"],
            "mongo_avg_ms": m_avg,
            "ch_avg_ms": c_avg,
            "speedup": m_avg / max(c_avg, 0.001),
        })

    return results


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Real data benchmark: MongoDB vs ClickHouse")
    parser.add_argument("--limit", type=int, default=0, help="Max documents to load (0 = all)")
    parser.add_argument("--iterations", type=int, default=3, help="Query iterations")
    parser.add_argument("--skip-load", action="store_true", help="Skip loading, use existing CH data")
    args = parser.parse_args()

    # Validate env vars
    if not MONGO_URI:
        sys.exit("ERROR: Set MONGO_URI environment variable\n"
                 "  export MONGO_URI='mongodb://user:pass@host:27017/db?authSource=admin'")
    if not MONGO_DB:
        sys.exit("ERROR: Set MONGO_DB environment variable")
    if not MONGO_COLLECTION:
        sys.exit("ERROR: Set MONGO_COLLECTION environment variable")

    print(f"\n{'=' * 70}")
    print(f"  Real Data Benchmark: MongoDB vs ClickHouse")
    print(f"  Database: {MONGO_DB}")
    print(f"  Collection: {MONGO_COLLECTION}")
    print(f"  Limit: {'all' if args.limit == 0 else f'{args.limit:,}'}")
    print(f"  Iterations: {args.iterations}")
    print(f"{'=' * 70}\n")

    # Connect to MongoDB
    print("[1/5] Connecting to MongoDB...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    doc_count = coll.estimated_document_count()
    print(f"      Collection has ~{doc_count:,} documents")

    # Discover schema
    print("[2/5] Discovering schema...")
    schema, sample_doc = discover_schema(coll)
    print(f"      Found {len(schema)} fields")
    for field, ch_type in list(schema.items())[:10]:
        print(f"        {field}: {ch_type}")
    if len(schema) > 10:
        print(f"        ... and {len(schema) - 10} more")

    # Load to ClickHouse
    if not args.skip_load:
        print("[3/5] Loading data into ClickHouse...")
        t0 = time.perf_counter()
        limit = args.limit if args.limit > 0 else 0
        table_name, ch_fields, total = load_to_clickhouse(coll, schema, limit if limit > 0 else doc_count)
        print(f"      Done in {time.perf_counter() - t0:.1f}s")
    else:
        print("[3/5] Skipping load (--skip-load)")
        table_name = MONGO_COLLECTION.lower().replace("-", "_")
        ch_fields = [(f, f.replace(".", "_").replace("$", "").replace(" ", "_"), t) for f, t in schema.items()]
        total = doc_count

    # Generate queries
    print("[4/5] Generating benchmark queries...")
    queries = generate_queries(schema, ch_fields, table_name, sample_doc)
    print(f"      Generated {len(queries)} queries")

    # Run benchmark
    print(f"[5/5] Running benchmark ({args.iterations} iterations)...\n")
    results = run_benchmark(coll, queries, args.iterations)

    # Print results
    print(f"\n{'Query':<50} {'MongoDB (ms)':<14} {'CH (ms)':<10} {'Speedup':<10}")
    print("-" * 84)
    for r in results:
        m = f"{r['mongo_avg_ms']:.1f}" if r['mongo_avg_ms'] != float("inf") else "ERROR"
        c = f"{r['ch_avg_ms']:.1f}" if r['ch_avg_ms'] != float("inf") else "ERROR"
        s = f"{r['speedup']:.1f}x" if r['speedup'] != float("inf") else "N/A"
        print(f"{r['query']:<50} {m:<14} {c:<10} {s:<10}")

    print("-" * 84)
    valid = [r for r in results if r["speedup"] != float("inf") and r["speedup"] > 0]
    if valid:
        avg_speedup = statistics.mean(r["speedup"] for r in valid)
        max_r = max(valid, key=lambda x: x["speedup"])
        print(f"\n  Documents benchmarked: {total:,}")
        print(f"  Average speedup: {avg_speedup:.1f}x")
        print(f"  Max speedup: {max_r['speedup']:.1f}x on '{max_r['query']}'")

    # Save
    output = {
        "timestamp": datetime.now().isoformat(),
        "mongo_db": MONGO_DB,
        "collection": MONGO_COLLECTION,
        "document_count": total,
        "schema_fields": len(schema),
        "results": results,
    }
    output_file = "benchmark/real_data_results.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved to {output_file}")

    client.close()


if __name__ == "__main__":
    main()
