#!/usr/bin/env python3
"""
Real Data Benchmark: MongoDB vs ClickHouse

Connects to an existing MongoDB collection (no inserts), auto-discovers the schema,
loads data into local ClickHouse, and benchmarks analytical queries on both.

Usage:
    export MONGO_URI="mongodb://user:pass@host1:27017,host2:27017,host3:27017/dbname?authSource=admin&readPreference=primary"
    export MONGO_DB="mydb"
    export MONGO_COLLECTION="myCollection"

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
    """Generate 20 complex aggregation queries for APM alerts benchmarking."""
    queries = []

    # Field name mapping helper
    def get_cf(mongo_field):
        """Get ClickHouse column name for a mongo field."""
        for mf, cn, ct in ch_fields:
            if mf == mongo_field:
                return cn
        return mongo_field

    # 1. Total alert count
    queries.append({
        "name": "1. Total alert count",
        "mongo": [{"$count": "total"}],
        "ch": f"SELECT count() as total FROM {CH_DB}.{table_name}",
    })

    # 2. Alerts by status
    queries.append({
        "name": "2. Count by status",
        "mongo": [{"$group": {"_id": "$status", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
        "ch": f"SELECT `{get_cf('status')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('status')}` ORDER BY cnt DESC",
    })

    # 3. Alerts by category × type (2D GROUP BY)
    queries.append({
        "name": "3. Alerts by category × type",
        "mongo": [{"$group": {"_id": {"cat": "$alertCategory", "type": "$alertType"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
        "ch": f"SELECT `{get_cf('alertCategory')}`, `{get_cf('alertType')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('alertCategory')}`, `{get_cf('alertType')}` ORDER BY cnt DESC",
    })

    # 4. Top 20 customers by alert volume
    queries.append({
        "name": "4. Top 20 customers by alert count",
        "mongo": [{"$group": {"_id": "$customerName", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 20}],
        "ch": f"SELECT `{get_cf('customerName')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('customerName')}` ORDER BY cnt DESC LIMIT 20",
    })

    # 5. Alerts by product × region × status (3D GROUP BY)
    queries.append({
        "name": "5. Alerts by product × region × status",
        "mongo": [{"$group": {"_id": {"product": "$product", "region": "$apmRegion", "status": "$status"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
        "ch": f"SELECT `{get_cf('product')}`, `{get_cf('apmRegion')}`, `{get_cf('status')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('product')}`, `{get_cf('apmRegion')}`, `{get_cf('status')}` ORDER BY cnt DESC",
    })

    # 6. Unique customers per segment
    queries.append({
        "name": "6. Unique customers per segment",
        "mongo": [{"$group": {"_id": "$customerSegment", "unique_customers": {"$addToSet": "$customerName"}}}, {"$project": {"segment": "$_id", "count": {"$size": "$unique_customers"}}}],
        "ch": f"SELECT `{get_cf('customerSegment')}`, uniqExact(`{get_cf('customerName')}`) as unique_customers FROM {CH_DB}.{table_name} GROUP BY `{get_cf('customerSegment')}`",
    })

    # 7. Avg alert value by product
    queries.append({
        "name": "7. Avg alert value by product",
        "mongo": [{"$group": {"_id": "$product", "avg_val": {"$avg": "$value"}, "max_val": {"$max": "$value"}, "count": {"$sum": 1}}}, {"$sort": {"avg_val": -1}}],
        "ch": f"SELECT `{get_cf('product')}`, avg(toFloat64OrZero(`{get_cf('value')}`)) as avg_val, max(toFloat64OrZero(`{get_cf('value')}`)) as max_val, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('product')}` ORDER BY avg_val DESC",
    })

    # 8. Alert trend by month
    queries.append({
        "name": "8. Alert trend by month (24 months)",
        "mongo": [{"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$createdAt"}}, "count": {"$sum": 1}}}, {"$sort": {"_id": -1}}, {"$limit": 24}],
        "ch": f"SELECT substring(`{get_cf('createdAt')}`, 1, 7) as month, count() as cnt FROM {CH_DB}.{table_name} WHERE `{get_cf('createdAt')}` IS NOT NULL AND `{get_cf('createdAt')}` != '' GROUP BY month ORDER BY month DESC LIMIT 24",
    })

    # 9. Open alerts by assignee
    queries.append({
        "name": "9. Open alerts by assignee (top 20)",
        "mongo": [{"$match": {"status": {"$ne": "resolved"}}}, {"$group": {"_id": "$assignee", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 20}],
        "ch": f"SELECT `{get_cf('assignee')}`, count() as cnt FROM {CH_DB}.{table_name} WHERE `{get_cf('status')}` != 'resolved' GROUP BY `{get_cf('assignee')}` ORDER BY cnt DESC LIMIT 20",
    })

    # 10. Resolution rate by product
    queries.append({
        "name": "10. Resolution rate by product",
        "mongo": [{"$group": {"_id": "$product", "total": {"$sum": 1}, "resolved": {"$sum": {"$cond": [{"$eq": ["$status", "resolved"]}, 1, 0]}}}}, {"$project": {"product": "$_id", "total": 1, "resolved": 1, "rate": {"$divide": ["$resolved", "$total"]}}}],
        "ch": f"SELECT `{get_cf('product')}`, count() as total, countIf(`{get_cf('status')}` = 'resolved') as resolved, resolved / total as rate FROM {CH_DB}.{table_name} GROUP BY `{get_cf('product')}` ORDER BY rate ASC",
    })

    # 11. Top 15 ATS by alert count + unique customers
    queries.append({
        "name": "11. Top 15 ATS by alerts + customers",
        "mongo": [{"$group": {"_id": "$ats", "count": {"$sum": 1}, "customers": {"$addToSet": "$customerName"}}}, {"$project": {"ats": "$_id", "count": 1, "customer_count": {"$size": "$customers"}}}, {"$sort": {"count": -1}}, {"$limit": 15}],
        "ch": f"SELECT `{get_cf('ats')}`, count() as cnt, uniqExact(`{get_cf('customerName')}`) as customer_count FROM {CH_DB}.{table_name} GROUP BY `{get_cf('ats')}` ORDER BY cnt DESC LIMIT 15",
    })

    # 12. Alerts by experience × pod
    queries.append({
        "name": "12. Alerts by experience × pod",
        "mongo": [{"$group": {"_id": {"exp": "$experience", "pod": "$pod"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 30}],
        "ch": f"SELECT `{get_cf('experience')}`, `{get_cf('pod')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('experience')}`, `{get_cf('pod')}` ORDER BY cnt DESC LIMIT 30",
    })

    # 13. Premier customers with open alerts
    queries.append({
        "name": "13. Premier customers with open alerts",
        "mongo": [{"$match": {"customerSegment": "Premier", "status": {"$ne": "resolved"}}}, {"$group": {"_id": "$customerName", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 10}],
        "ch": f"SELECT `{get_cf('customerName')}`, count() as cnt FROM {CH_DB}.{table_name} WHERE `{get_cf('customerSegment')}` = 'Premier' AND `{get_cf('status')}` != 'resolved' GROUP BY `{get_cf('customerName')}` ORDER BY cnt DESC LIMIT 10",
    })

    # 14. Alert types per data centre
    queries.append({
        "name": "14. Alert types per data centre",
        "mongo": [{"$group": {"_id": {"dc": "$dataCentre", "type": "$alertType"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
        "ch": f"SELECT `{get_cf('dataCentre')}`, `{get_cf('alertType')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('dataCentre')}`, `{get_cf('alertType')}` ORDER BY cnt DESC",
    })

    # 15. Jira status by product (filter non-empty tickets)
    queries.append({
        "name": "15. Jira status by product",
        "mongo": [{"$match": {"jiraTicketNumber": {"$exists": True, "$ne": ""}}}, {"$group": {"_id": {"product": "$product", "jira_status": "$jiraTicketStatus"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
        "ch": f"SELECT `{get_cf('product')}`, `{get_cf('jiraTicketStatus')}`, count() as cnt FROM {CH_DB}.{table_name} WHERE `{get_cf('jiraTicketNumber')}` IS NOT NULL AND `{get_cf('jiraTicketNumber')}` != '' GROUP BY `{get_cf('product')}`, `{get_cf('jiraTicketStatus')}` ORDER BY cnt DESC",
    })

    # 16. High-repeat alerts (value > 10) by customer
    queries.append({
        "name": "16. High-repeat alerts (value>10) by customer",
        "mongo": [{"$match": {"value": {"$gt": 10}}}, {"$group": {"_id": "$customerName", "count": {"$sum": 1}, "max_value": {"$max": "$value"}}}, {"$sort": {"count": -1}}, {"$limit": 20}],
        "ch": f"SELECT `{get_cf('customerName')}`, count() as cnt, max(toInt64OrZero(`{get_cf('value')}`)) as max_value FROM {CH_DB}.{table_name} WHERE toInt64OrZero(`{get_cf('value')}`) > 10 GROUP BY `{get_cf('customerName')}` ORDER BY cnt DESC LIMIT 20",
    })

    # 17. Service × alertType heatmap
    queries.append({
        "name": "17. Service × alertType heatmap",
        "mongo": [{"$group": {"_id": {"service": "$service", "type": "$alertType"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}, {"$limit": 50}],
        "ch": f"SELECT `{get_cf('service')}`, `{get_cf('alertType')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('service')}`, `{get_cf('alertType')}` ORDER BY cnt DESC LIMIT 50",
    })

    # 18. Unique alert names per product (cardinality)
    queries.append({
        "name": "18. Unique alert names per product",
        "mongo": [{"$group": {"_id": "$product", "unique_alerts": {"$addToSet": "$alertname"}}}, {"$project": {"product": "$_id", "cardinality": {"$size": "$unique_alerts"}}}, {"$sort": {"cardinality": -1}}],
        "ch": f"SELECT `{get_cf('product')}`, uniqExact(`{get_cf('alertname')}`) as cardinality FROM {CH_DB}.{table_name} GROUP BY `{get_cf('product')}` ORDER BY cardinality DESC",
    })

    # 19. Acknowledged ratio by region
    queries.append({
        "name": "19. Acknowledged ratio by region",
        "mongo": [{"$group": {"_id": "$apmRegion", "total": {"$sum": 1}, "acked": {"$sum": {"$cond": [{"$ne": ["$acknowledgedAt", None]}, 1, 0]}}}}, {"$project": {"region": "$_id", "total": 1, "acked": 1, "rate": {"$divide": ["$acked", "$total"]}}}],
        "ch": f"SELECT `{get_cf('apmRegion')}`, count() as total, countIf(`{get_cf('acknowledgedAt')}` IS NOT NULL AND `{get_cf('acknowledgedAt')}` != '') as acked, acked / total as rate FROM {CH_DB}.{table_name} GROUP BY `{get_cf('apmRegion')}`",
    })

    # 20. Full cross-tab: region × segment × status
    queries.append({
        "name": "20. Cross-tab: region × segment × status",
        "mongo": [{"$group": {"_id": {"region": "$apmRegion", "segment": "$customerSegment", "status": "$status"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
        "ch": f"SELECT `{get_cf('apmRegion')}`, `{get_cf('customerSegment')}`, `{get_cf('status')}`, count() as cnt FROM {CH_DB}.{table_name} GROUP BY `{get_cf('apmRegion')}`, `{get_cf('customerSegment')}`, `{get_cf('status')}` ORDER BY cnt DESC",
    })

    return queries


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
