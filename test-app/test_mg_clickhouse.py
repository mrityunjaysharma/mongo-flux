#!/usr/bin/env python3
"""
mg-clickhouse Test Application
Tests CRUD operations on MongoDB and verifies sync to ClickHouse,
then runs aggregation benchmarks comparing both.

Usage:
    python3 test-app/test_mg_clickhouse.py
"""

import pymongo
import requests
import time
import json
import sys
from datetime import datetime, timedelta
import random
import statistics

# ============================================================
# Configuration
# ============================================================

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"
MONGO_DB = "myapp"
MONGO_COLLECTION = "mg_clickhouse_test"
CH_URL = "http://localhost:8123"
CH_DB = "myapp"
CH_TABLE = "mg_clickhouse_test"
MG_CLICKHOUSE_API = "http://localhost:9090"

# ============================================================
# Helpers
# ============================================================

def ch_query(sql):
    resp = requests.post(CH_URL, data=sql)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error: {resp.text.strip()[:200]}")
    return resp.text.strip()


def api_call(method, path, data=None):
    url = f"{MG_CLICKHOUSE_API}{path}"
    if method == "GET":
        resp = requests.get(url)
    elif method == "POST":
        resp = requests.post(url, json=data, headers={"Content-Type": "application/json"})
    elif method == "DELETE":
        resp = requests.delete(url)
    else:
        raise ValueError(f"Unknown method: {method}")
    return resp.json() if resp.text else {}


def wait_for_sync(expected_count, timeout=15):
    """Wait until ClickHouse has the expected row count."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            count = int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}"))
            if count >= expected_count:
                return count
        except Exception:
            pass
        time.sleep(0.5)
    # Return whatever we got
    try:
        return int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}"))
    except Exception:
        return 0


def print_header(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def print_result(test_name, passed, detail=""):
    icon = "✓" if passed else "✗"
    status = "PASS" if passed else "FAIL"
    detail_str = f" — {detail}" if detail else ""
    print(f"  [{icon}] {status}: {test_name}{detail_str}")


# ============================================================
# Test Suite
# ============================================================

def setup():
    """Create mapping and ClickHouse table."""
    print("  Setting up mapping...")

    # Create mapping via mg-clickhouse API
    mapping = {
        "collection": MONGO_COLLECTION,
        "clickhouse_table": CH_TABLE,
        "clickhouse_database": CH_DB,
        "fields": [
            {"mongo_field": "_id", "ch_column": "id", "ch_type": "String"},
            {"mongo_field": "name", "ch_column": "name", "ch_type": "String"},
            {"mongo_field": "category", "ch_column": "category", "ch_type": "LowCardinality(String)"},
            {"mongo_field": "region", "ch_column": "region", "ch_type": "LowCardinality(String)"},
            {"mongo_field": "status", "ch_column": "status", "ch_type": "LowCardinality(String)"},
            {"mongo_field": "value", "ch_column": "value", "ch_type": "Int32"},
            {"mongo_field": "score", "ch_column": "score", "ch_type": "Float64"},
            {"mongo_field": "createdAt", "ch_column": "created_at", "ch_type": "DateTime64(3)"},
            {"mongo_field": "tags", "ch_column": "tags", "ch_type": "String"},
            {"mongo_field": "active", "ch_column": "active", "ch_type": "Bool"},
        ],
        "engine": "ReplacingMergeTree",
        "order_by": ["created_at", "id"],
    }

    result = api_call("POST", "/api/v1/mappings", mapping)
    print(f"    Mapping: {result.get('status', 'error')}")

    # Create ClickHouse table
    result = api_call("POST", f"/api/v1/mappings/{MONGO_COLLECTION}/sync")
    print(f"    Table: {result.get('status', 'error')}")

    # Restart sync to pick up new mapping
    api_call("POST", "/api/v1/sync/restart")
    time.sleep(2)
    print("    Sync restarted")


def teardown(coll):
    """Clean up test data."""
    coll.drop()
    try:
        ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
    except Exception:
        pass
    try:
        api_call("DELETE", f"/api/v1/mappings/{MONGO_COLLECTION}")
    except Exception:
        pass


# ============================================================
# CRUD Tests
# ============================================================

def test_insert_single(coll):
    """Test: Insert a single document and verify sync."""
    doc = {
        "name": "Test Alert 1",
        "category": "Infrastructure",
        "region": "US",
        "status": "active",
        "value": 42,
        "score": 95.5,
        "createdAt": datetime(2025, 5, 15, 10, 0, 0),
        "tags": json.dumps(["critical", "prod"]),
        "active": True,
    }
    result = coll.insert_one(doc)
    count = wait_for_sync(1)
    print_result("INSERT single document → synced to ClickHouse", count >= 1, f"CH count: {count}")
    return result.inserted_id


def test_insert_batch(coll):
    """Test: Insert 100 documents in batch and verify sync."""
    categories = ["Infrastructure", "Application", "Security", "Network"]
    regions = ["US", "EU", "APAC", "LATAM"]
    statuses = ["active", "resolved", "acknowledged", "suppressed"]

    docs = []
    for i in range(100):
        docs.append({
            "name": f"Batch Alert {i}",
            "category": random.choice(categories),
            "region": random.choice(regions),
            "status": random.choice(statuses),
            "value": random.randint(1, 100),
            "score": round(random.uniform(0, 100), 2),
            "createdAt": datetime(2025, 1, 1) + timedelta(days=random.randint(0, 150)),
            "tags": json.dumps(random.sample(["critical", "prod", "staging", "low", "high"], 2)),
            "active": random.choice([True, False]),
        })

    coll.insert_many(docs)
    count = wait_for_sync(101)  # 1 from previous + 100 new
    print_result("INSERT batch (100 docs) → synced to ClickHouse", count >= 101, f"CH count: {count}")


def test_update(coll):
    """Test: Update a document and verify sync."""
    # Update one document
    coll.update_one(
        {"name": "Test Alert 1"},
        {"$set": {"status": "resolved", "value": 99, "score": 100.0}}
    )
    time.sleep(2)  # Wait for change stream to pick up update

    # Check ClickHouse — ReplacingMergeTree will have both versions until merge
    result = ch_query(f"SELECT status, value FROM {CH_DB}.{CH_TABLE} WHERE name = 'Test Alert 1' ORDER BY value DESC LIMIT 1")
    has_update = "resolved" in result and "99" in result
    print_result("UPDATE document → synced to ClickHouse", has_update, f"Got: {result}")


def test_insert_after_update(coll):
    """Test: Insert after update to verify stream continuity."""
    coll.insert_one({
        "name": "Post-Update Alert",
        "category": "Security",
        "region": "EU",
        "status": "active",
        "value": 77,
        "score": 88.8,
        "createdAt": datetime(2025, 5, 16, 8, 0, 0),
        "tags": json.dumps(["new"]),
        "active": True,
    })
    count = wait_for_sync(103)  # 101 + update row + 1 new
    print_result("INSERT after UPDATE → stream continuity OK", count >= 102, f"CH count: {count}")


# ============================================================
# Aggregation Benchmark
# ============================================================

def test_aggregations(coll):
    """Run 10 aggregation queries and compare MongoDB vs ClickHouse."""
    print_header("Aggregation Benchmark (MongoDB vs ClickHouse)")

    queries = [
        {
            "name": "Count by status",
            "mongo": [{"$group": {"_id": "$status", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT status, count() as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY status ORDER BY cnt DESC",
        },
        {
            "name": "Count by region",
            "mongo": [{"$group": {"_id": "$region", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT region, count() as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY region ORDER BY cnt DESC",
        },
        {
            "name": "Avg value by category",
            "mongo": [{"$group": {"_id": "$category", "avg_val": {"$avg": "$value"}, "max_val": {"$max": "$value"}}}],
            "ch": f"SELECT category, avg(value) as avg_val, max(value) as max_val FROM {CH_DB}.{CH_TABLE} GROUP BY category",
        },
        {
            "name": "2D GROUP BY: category × region",
            "mongo": [{"$group": {"_id": {"cat": "$category", "reg": "$region"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT category, region, count() as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY category, region ORDER BY cnt DESC",
        },
        {
            "name": "Filter active + group by category",
            "mongo": [{"$match": {"status": "active"}}, {"$group": {"_id": "$category", "count": {"$sum": 1}}}],
            "ch": f"SELECT category, count() as cnt FROM {CH_DB}.{CH_TABLE} WHERE status = 'active' GROUP BY category",
        },
        {
            "name": "Top 5 by value sum",
            "mongo": [{"$group": {"_id": "$category", "total": {"$sum": "$value"}}}, {"$sort": {"total": -1}}, {"$limit": 5}],
            "ch": f"SELECT category, sum(value) as total FROM {CH_DB}.{CH_TABLE} GROUP BY category ORDER BY total DESC LIMIT 5",
        },
        {
            "name": "Unique names per region",
            "mongo": [{"$group": {"_id": "$region", "names": {"$addToSet": "$name"}}}, {"$project": {"region": "$_id", "count": {"$size": "$names"}}}],
            "ch": f"SELECT region, uniqExact(name) as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY region",
        },
        {
            "name": "Score percentiles by status",
            "mongo": [{"$group": {"_id": "$status", "avg_score": {"$avg": "$score"}, "min_score": {"$min": "$score"}, "max_score": {"$max": "$score"}}}],
            "ch": f"SELECT status, avg(score) as avg_score, min(score) as min_score, max(score) as max_score FROM {CH_DB}.{CH_TABLE} GROUP BY status",
        },
        {
            "name": "3D GROUP BY: status × region × active",
            "mongo": [{"$group": {"_id": {"s": "$status", "r": "$region", "a": "$active"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT status, region, active, count() as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY status, region, active ORDER BY cnt DESC",
        },
        {
            "name": "Full table count",
            "mongo": [{"$count": "total"}],
            "ch": f"SELECT count() as total FROM {CH_DB}.{CH_TABLE}",
        },
    ]

    print(f"  {'Query':<40} {'MongoDB (ms)':<14} {'CH (ms)':<10} {'Speedup':<10}")
    print(f"  {'-'*74}")

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

        for _ in range(3):
            start = time.perf_counter()
            try:
                list(coll.aggregate(q["mongo"], allowDiskUse=True))
            except Exception:
                pass
            mongo_times.append((time.perf_counter() - start) * 1000)

            start = time.perf_counter()
            try:
                ch_query(q["ch"])
            except Exception:
                pass
            ch_times.append((time.perf_counter() - start) * 1000)

        m = statistics.mean(mongo_times)
        c = statistics.mean(ch_times)
        speedup = m / max(c, 0.01)
        results.append({"name": q["name"], "mongo": m, "ch": c, "speedup": speedup})
        print(f"  {q['name']:<40} {m:<14.1f} {c:<10.1f} {speedup:.1f}x")

    print(f"  {'-'*74}")
    avg_speedup = statistics.mean(r["speedup"] for r in results)
    print(f"\n  Average speedup: {avg_speedup:.1f}x")
    return results


# ============================================================
# Main
# ============================================================

def main():
    print_header("mg-clickhouse Test Application")
    print("  Testing: CRUD sync + Aggregation benchmarks")
    print(f"  MongoDB: {MONGO_DB}.{MONGO_COLLECTION}")
    print(f"  ClickHouse: {CH_DB}.{CH_TABLE}")

    # Connect
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    # Clean slate
    coll.drop()
    try:
        ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
    except Exception:
        pass

    # Setup
    print_header("Setup")
    try:
        setup()
    except Exception as e:
        print(f"  Setup failed: {e}")
        print("  Make sure mg-clickhouse is running (docker compose up)")
        sys.exit(1)

    # CRUD Tests
    print_header("CRUD Tests")

    test_insert_single(coll)
    test_insert_batch(coll)
    test_update(coll)
    test_insert_after_update(coll)

    # Aggregation Benchmark
    agg_results = test_aggregations(coll)

    # Summary
    print_header("Summary")
    ch_count = int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}"))
    mongo_count = coll.count_documents({})
    print(f"  MongoDB documents: {mongo_count}")
    print(f"  ClickHouse rows:   {ch_count}")
    print(f"  Sync verified:     {'✓' if ch_count >= mongo_count else '✗'}")
    if agg_results:
        avg = statistics.mean(r["speedup"] for r in agg_results)
        print(f"  Avg query speedup: {avg:.1f}x")

    # Cleanup prompt
    print(f"\n  Test collection: {MONGO_DB}.{MONGO_COLLECTION}")
    print("  Run with --cleanup to remove test data after")

    if "--cleanup" in sys.argv:
        print("\n  Cleaning up...")
        teardown(coll)
        print("  Done.")

    client.close()


if __name__ == "__main__":
    main()
