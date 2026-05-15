#!/usr/bin/env python3
"""
Real Data Benchmark: MongoDB vs ClickHouse

Connects to an existing MongoDB collection (no inserts), auto-discovers the schema,
loads data into local ClickHouse, and benchmarks analytical queries on both.

Usage:
    export MONGO_URI="mongodb://user:pass@host1:27017,host2:27017,host3:27017/dbname?authSource=admin&readPreference=primary"
    export MONGO_DB="apm_db"
    export MONGO_COLLECTION="apmActiveAlertsProdUSMar10"

    python3 benchmark/real_data_benchmark.py --limit 5000000 --iterations 3

Environment Variables:
    MONGO_URI          - Full MongoDB connection URI (required)
    MONGO_DB           - Database name (required)
    MONGO_COLLECTION   - Collection name (required)
    CH_URL             - ClickHouse HTTP URL (default: http://localhost:8123)
    CH_DB              - ClickHouse database (default: real_benchmark)
"""

import argparse
import json
import os
import random
import time
import statistics
import sys
from datetime import datetime
from bson import ObjectId

try:
    import pymongo
except ImportError:
    sys.exit("pymongo not installed. Run: pip install pymongo")

try:
    import requests
except ImportError:
    sys.exit("requests not installed. Run: pip install requests")


# ============================================================
# Configuration from environment
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB = os.environ.get("MONGO_DB", "")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "")
CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_DB = os.environ.get("CH_DB", "real_benchmark")


def ch_query(sql):
    resp = requests.post(CH_URL, data=sql)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error: {resp.text.strip()[:300]}")
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
            schema[key] = "String"

    return schema, samples[0]


def map_python_type_to_ch(python_type):
    """Map Python type names to ClickHouse types."""
    mapping = {
        "str": "String",
        "int": "Int64",
        "float": "Float64",
        "bool": "UInt8",
        "datetime": "DateTime",
        "date": "Date",
        "list": "String",  # JSON-serialize arrays
        "dict": "String",  # JSON-serialize nested docs
        "ObjectId": "String",
        "NoneType": "Nullable(String)",
        "Decimal128": "Float64",
    }
    return mapping.get(python_type, "String")


# ============================================================
# Data Loading (MongoDB → ClickHouse)
# ============================================================

def load_to_clickhouse(coll, schema, limit):
    """Read from MongoDB and load into ClickHouse."""
    table_name = MONGO_COLLECTION.lower().replace("-", "_")

    # Create database
    ch_query(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
    ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{table_name}")

    # Build CREATE TABLE DDL
    columns = []
    ch_fields = []  # Fields we'll actually sync
    for field, ch_type in schema.items():
        safe_name = field.replace(".", "_").replace("$", "").replace(" ", "_")
        if safe_name and safe_name[0].isdigit():
            safe_name = "f_" + safe_name
        columns.append(f"    `{safe_name}` {ch_type}")
        ch_fields.append((field, safe_name, ch_type))

    ddl = f"CREATE TABLE {CH_DB}.{table_name} (\n"
    ddl += ",\n".join(columns)
    ddl += f"\n) ENGINE = MergeTree()\nORDER BY tuple()\n"

    print(f"    Creating ClickHouse table: {CH_DB}.{table_name}")
    ch_query(ddl)

    # Stream data from MongoDB to ClickHouse in batches
    print(f"    Loading up to {limit:,} documents...")
    batch_size = 10000
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
                row[ch_name] = val.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(val, (list, dict)):
                row[ch_name] = json.dumps(val, default=str)
            elif isinstance(val, bool):
                row[ch_name] = 1 if val else 0
            else:
                row[ch_name] = val

        batch.append(row)

        if len(batch) >= batch_size:
            _flush_batch(table_name, batch)
            total_loaded += len(batch)
            if total_loaded % 100000 == 0:
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
    url = f"{CH_URL}/?query=INSERT+INTO+{CH_DB}.{table_name}+FORMAT+JSONEachRow"
    resp = requests.post(url, data=payload.encode("utf-8"))
    if resp.status_code != 200:
        raise RuntimeError(f"Insert error: {resp.text.strip()[:200]}")


# ============================================================
# Auto-Generate Benchmark Queries
# ============================================================

def generate_queries(schema, ch_fields, table_name, sample_doc):
    """Generate analytical queries based on discovered schema."""
    queries = []

    # Find string fields (good for GROUP BY)
    string_fields = [(mf, cf) for mf, cf, ct in ch_fields if ct in ("String", "LowCardinality(String)") and mf != "_id"]
    # Find numeric fields (good for aggregations)
    numeric_fields = [(mf, cf) for mf, cf, ct in ch_fields if ct in ("Int64", "Float64")]
    # Find date fields
    date_fields = [(mf, cf) for mf, cf, ct in ch_fields if ct in ("DateTime", "Date")]

    # 1. Full count
    queries.append({
        "name": "Full table count",
        "mongo": [{"$count": "total"}],
        "ch": f"SELECT count() as total FROM {CH_DB}.{table_name} FORMAT Null",
    })

    # 2-5. GROUP BY on string fields
    for i, (mf, cf) in enumerate(string_fields[:4]):
        queries.append({
            "name": f"Count by {mf}",
            "mongo": [{"$group": {"_id": f"${mf}", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT `{cf}`, count() as count FROM {CH_DB}.{table_name} GROUP BY `{cf}` ORDER BY count DESC FORMAT Null",
        })

    # 6-8. Aggregations on numeric fields
    for i, (mf, cf) in enumerate(numeric_fields[:3]):
        queries.append({
            "name": f"Stats on {mf} (avg/min/max)",
            "mongo": [{"$group": {"_id": None, "avg": {"$avg": f"${mf}"}, "min": {"$min": f"${mf}"}, "max": {"$max": f"${mf}"}}}],
            "ch": f"SELECT avg(`{cf}`) as avg_val, min(`{cf}`) as min_val, max(`{cf}`) as max_val FROM {CH_DB}.{table_name} FORMAT Null",
        })

    # 9-11. GROUP BY string + aggregate numeric
    if string_fields and numeric_fields:
        for i in range(min(3, len(string_fields))):
            mf_s, cf_s = string_fields[i]
            mf_n, cf_n = numeric_fields[0] if numeric_fields else (None, None)
            if mf_n:
                queries.append({
                    "name": f"Avg {mf_n} by {mf_s}",
                    "mongo": [{"$group": {"_id": f"${mf_s}", "avg_val": {"$avg": f"${mf_n}"}}}],
                    "ch": f"SELECT `{cf_s}`, avg(`{cf_n}`) as avg_val FROM {CH_DB}.{table_name} GROUP BY `{cf_s}` FORMAT Null",
                })

    # 12-13. Two-dimensional GROUP BY
    if len(string_fields) >= 2:
        mf1, cf1 = string_fields[0]
        mf2, cf2 = string_fields[1]
        queries.append({
            "name": f"2D GROUP BY ({mf1} x {mf2})",
            "mongo": [{"$group": {"_id": {"a": f"${mf1}", "b": f"${mf2}"}, "count": {"$sum": 1}}}],
            "ch": f"SELECT `{cf1}`, `{cf2}`, count() as count FROM {CH_DB}.{table_name} GROUP BY `{cf1}`, `{cf2}` FORMAT Null",
        })

    # 14. Top N
    if string_fields:
        mf, cf = string_fields[0]
        queries.append({
            "name": f"Top 20 {mf} by count",
            "mongo": [{"$group": {"_id": f"${mf}", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 20}],
            "ch": f"SELECT `{cf}`, count() as count FROM {CH_DB}.{table_name} GROUP BY `{cf}` ORDER BY count DESC LIMIT 20 FORMAT Null",
        })

    # 15. Distinct count
    if string_fields:
        mf, cf = string_fields[0]
        queries.append({
            "name": f"Unique count of {mf}",
            "mongo": [{"$group": {"_id": None, "unique": {"$addToSet": f"${mf}"}}}],
            "ch": f"SELECT uniqExact(`{cf}`) as unique_count FROM {CH_DB}.{table_name} FORMAT Null",
        })

    # 16-17. Filter + aggregate
    if string_fields and sample_doc:
        mf, cf = string_fields[0]
        # Get a sample value to filter on
        sample_val = sample_doc.get(mf)
        if sample_val and isinstance(sample_val, str):
            queries.append({
                "name": f"Filter {mf}='{sample_val[:20]}' + count",
                "mongo": [{"$match": {mf: sample_val}}, {"$count": "total"}],
                "ch": f"SELECT count() as total FROM {CH_DB}.{table_name} WHERE `{cf}` = '{sample_val}' FORMAT Null",
            })

    # 18-20. Date-based queries
    if date_fields:
        mf, cf = date_fields[0]
        queries.append({
            "name": f"Count by date ({mf})",
            "mongo": [{"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${mf}"}}, "count": {"$sum": 1}}}, {"$sort": {"_id": -1}}, {"$limit": 30}],
            "ch": f"SELECT toDate(`{cf}`) as day, count() as count FROM {CH_DB}.{table_name} GROUP BY day ORDER BY day DESC LIMIT 30 FORMAT Null",
        })

    return queries[:20]  # Cap at 20


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
