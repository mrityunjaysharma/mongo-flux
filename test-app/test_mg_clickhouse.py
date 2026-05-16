#!/usr/bin/env python3
"""
mg-clickhouse Integration Test Suite

Tests CRUD operations on MongoDB, verifies real-time sync to ClickHouse,
and benchmarks aggregation queries comparing both engines.

Usage:
    python3 test-app/test_mg_clickhouse.py
    python3 test-app/test_mg_clickhouse.py --cleanup

Environment Variables (override defaults):
    MONGO_URI           - MongoDB connection string (default: localhost:27017)
    CH_URL              - ClickHouse HTTP URL (default: http://localhost:8123)
    MG_CLICKHOUSE_API   - mg-clickhouse API URL (default: http://localhost:9090)
"""

import json
import logging
import os
import random
import statistics
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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
# Configuration
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.environ.get("MONGO_DB", "myapp")
MONGO_COLLECTION = "mg_clickhouse_test"
CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_DB = os.environ.get("CH_DB", "myapp")
CH_TABLE = "mg_clickhouse_test"
MG_CLICKHOUSE_API = os.environ.get("MG_CLICKHOUSE_API", "http://localhost:9090")

# HTTP session with connection pooling and timeouts
SESSION = requests.Session()
SESSION.mount("http://", HTTPAdapter(pool_connections=5, pool_maxsize=5))
REQUEST_TIMEOUT = 30  # seconds

# ============================================================
# Helpers
# ============================================================


def ch_query(sql: str) -> str:
    """Execute a ClickHouse SQL query and return the response text.

    Raises:
        RuntimeError: If ClickHouse returns a non-200 status.
        requests.ConnectionError: If ClickHouse is unreachable.
    """
    resp = SESSION.post(CH_URL, data=sql, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error ({resp.status_code}): {resp.text.strip()[:300]}")
    return resp.text.strip()


def api_call(method: str, path: str, data: Optional[Dict] = None) -> Dict[str, Any]:
    """Call the mg-clickhouse management API.

    Raises:
        requests.ConnectionError: If mg-clickhouse API is unreachable.
        RuntimeError: If API returns an error response.
    """
    url = f"{MG_CLICKHOUSE_API}{path}"
    if method == "GET":
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    elif method == "POST":
        resp = SESSION.post(url, json=data, headers={"Content-Type": "application/json"}, timeout=REQUEST_TIMEOUT)
    elif method == "DELETE":
        resp = SESSION.delete(url, timeout=REQUEST_TIMEOUT)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")

    if resp.status_code >= 400:
        raise RuntimeError(f"API error ({resp.status_code}): {resp.text.strip()[:200]}")
    return resp.json() if resp.text.strip() else {}


def wait_for_sync(expected_count: int, timeout: int = 15) -> int:
    """Poll ClickHouse until row count reaches expected_count or timeout."""
    start = time.time()
    count = 0
    while time.time() - start < timeout:
        try:
            count = int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}"))
            if count >= expected_count:
                return count
        except Exception:
            pass
        time.sleep(0.5)
    logger.warning(f"Sync timeout: expected {expected_count}, got {count} after {timeout}s")
    return count


def print_header(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def print_result(test_name: str, passed: bool, detail: str = "") -> None:
    icon = "✓" if passed else "✗"
    status = "PASS" if passed else "FAIL"
    detail_str = f" — {detail}" if detail else ""
    print(f"  [{icon}] {status}: {test_name}{detail_str}")


# ============================================================
# Setup / Teardown
# ============================================================


def setup() -> None:
    """Create mapping and ClickHouse table for the test collection."""
    logger.info("Creating mapping and ClickHouse table...")

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
    logger.info(f"  Mapping: {result.get('status', 'unknown')}")

    result = api_call("POST", f"/api/v1/mappings/{MONGO_COLLECTION}/sync")
    logger.info(f"  Table: {result.get('status', 'unknown')}")

    # Restart sync — may briefly disconnect, so retry
    for attempt in range(3):
        try:
            api_call("POST", "/api/v1/sync/restart")
            break
        except (requests.ConnectionError, RuntimeError):
            time.sleep(2)
    time.sleep(4)
    logger.info("  Sync restarted")


def teardown(coll: pymongo.collection.Collection) -> None:
    """Remove test data from MongoDB and ClickHouse."""
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


def test_insert_single(coll: pymongo.collection.Collection) -> None:
    """Verify a single insert syncs to ClickHouse."""
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
    coll.insert_one(doc)
    count = wait_for_sync(1)
    print_result("INSERT single document → synced", count >= 1, f"CH rows: {count}")


def test_insert_batch(coll: pymongo.collection.Collection) -> None:
    """Verify a batch of 100 inserts syncs to ClickHouse."""
    categories = ["Infrastructure", "Application", "Security", "Network"]
    regions = ["US", "EU", "APAC", "LATAM"]
    statuses = ["active", "resolved", "acknowledged", "suppressed"]

    docs = [
        {
            "name": f"Batch Alert {i}",
            "category": random.choice(categories),
            "region": random.choice(regions),
            "status": random.choice(statuses),
            "value": random.randint(1, 100),
            "score": round(random.uniform(0, 100), 2),
            "createdAt": datetime(2025, 1, 1) + timedelta(days=random.randint(0, 150)),
            "tags": json.dumps(random.sample(["critical", "prod", "staging", "low", "high"], 2)),
            "active": random.choice([True, False]),
        }
        for i in range(100)
    ]

    coll.insert_many(docs)
    count = wait_for_sync(101)
    print_result("INSERT batch (100 docs) → synced", count >= 101, f"CH rows: {count}")


def test_update(coll: pymongo.collection.Collection) -> None:
    """Verify an update syncs to ClickHouse."""
    coll.update_one(
        {"name": "Test Alert 1"},
        {"$set": {"status": "resolved", "value": 99, "score": 100.0}},
    )
    time.sleep(2)

    result = ch_query(
        f"SELECT status, value FROM {CH_DB}.{CH_TABLE} "
        f"WHERE name = 'Test Alert 1' ORDER BY value DESC LIMIT 1"
    )
    has_update = "resolved" in result and "99" in result
    print_result("UPDATE document → synced", has_update, f"Latest: {result}")


def test_stream_continuity(coll: pymongo.collection.Collection) -> None:
    """Verify inserts after updates still sync (stream not broken)."""
    coll.insert_one({
        "name": "Post-Update Alert",
        "category": "Security",
        "region": "EU",
        "status": "active",
        "value": 77,
        "score": 88.8,
        "createdAt": datetime(2025, 5, 16, 8, 0, 0),
        "tags": json.dumps(["continuity-test"]),
        "active": True,
    })
    count = wait_for_sync(102)
    print_result("INSERT after UPDATE → stream continuity", count >= 102, f"CH rows: {count}")


# ============================================================
# Aggregation Benchmark
# ============================================================


def run_aggregation_benchmark(coll: pymongo.collection.Collection) -> List[Dict[str, Any]]:
    """Run 10 aggregation queries and compare MongoDB vs ClickHouse latency."""
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
            "name": "Filter active + group",
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
            "mongo": [{"$group": {"_id": "$region", "names": {"$addToSet": "$name"}}}, {"$project": {"count": {"$size": "$names"}}}],
            "ch": f"SELECT region, uniqExact(name) as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY region",
        },
        {
            "name": "Score stats by status",
            "mongo": [{"$group": {"_id": "$status", "avg": {"$avg": "$score"}, "min": {"$min": "$score"}, "max": {"$max": "$score"}}}],
            "ch": f"SELECT status, avg(score), min(score), max(score) FROM {CH_DB}.{CH_TABLE} GROUP BY status",
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

    print(f"  {'Query':<38} {'MongoDB (ms)':<14} {'CH (ms)':<10} {'Speedup':<8}")
    print(f"  {'-'*70}")

    results: List[Dict[str, Any]] = []
    for q in queries:
        mongo_times: List[float] = []
        ch_times: List[float] = []

        # Warmup (1 iteration, discard)
        try:
            list(coll.aggregate(q["mongo"], allowDiskUse=True))
        except Exception:
            pass
        try:
            ch_query(q["ch"])
        except Exception:
            pass

        # Measured iterations
        for _ in range(3):
            start = time.perf_counter()
            try:
                list(coll.aggregate(q["mongo"], allowDiskUse=True))
                mongo_times.append((time.perf_counter() - start) * 1000)
            except Exception:
                mongo_times.append(float("inf"))

            start = time.perf_counter()
            try:
                ch_query(q["ch"])
                ch_times.append((time.perf_counter() - start) * 1000)
            except Exception:
                ch_times.append(float("inf"))

        m_avg = statistics.mean(mongo_times)
        c_avg = statistics.mean(ch_times)
        speedup = m_avg / max(c_avg, 0.01)
        results.append({"name": q["name"], "mongo_ms": m_avg, "ch_ms": c_avg, "speedup": speedup})
        print(f"  {q['name']:<38} {m_avg:<14.1f} {c_avg:<10.1f} {speedup:.1f}x")

    print(f"  {'-'*70}")
    avg_speedup = statistics.mean(r["speedup"] for r in results)
    print(f"\n  Average speedup: {avg_speedup:.1f}x")
    return results


# ============================================================
# Main
# ============================================================


def main() -> None:
    print_header("mg-clickhouse Integration Test")
    print(f"  MongoDB:    {MONGO_URI}")
    print(f"  ClickHouse: {CH_URL}")
    print(f"  API:        {MG_CLICKHOUSE_API}")
    print(f"  Database:   {MONGO_DB}")
    print(f"  Collection: {MONGO_COLLECTION}")

    # Preflight checks
    try:
        SESSION.get(f"{MG_CLICKHOUSE_API}/health", timeout=5)
    except requests.ConnectionError:
        sys.exit(f"ERROR: mg-clickhouse not reachable at {MG_CLICKHOUSE_API}\n"
                 f"  Start it with: docker compose up --build")

    try:
        ch_query("SELECT 1")
    except Exception as e:
        sys.exit(f"ERROR: ClickHouse not reachable at {CH_URL}\n  {e}")

    # Connect to MongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        sys.exit(f"ERROR: MongoDB not reachable at {MONGO_URI}\n  {e}")

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
    except requests.ConnectionError:
        sys.exit(f"ERROR: Cannot connect to mg-clickhouse API at {MG_CLICKHOUSE_API}")
    except RuntimeError as e:
        sys.exit(f"ERROR: Setup failed: {e}")

    # CRUD Tests
    print_header("CRUD Tests")
    test_insert_single(coll)
    test_insert_batch(coll)
    test_update(coll)
    test_stream_continuity(coll)

    # Aggregation Benchmark
    print_header("Aggregation Benchmark (MongoDB vs ClickHouse)")
    agg_results = run_aggregation_benchmark(coll)

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

    # Cleanup
    if "--cleanup" in sys.argv:
        print("\n  Cleaning up test data...")
        teardown(coll)
        print("  Done.")
    else:
        print(f"\n  Test data retained in {MONGO_DB}.{MONGO_COLLECTION}")
        print("  Run with --cleanup to remove.")

    client.close()


if __name__ == "__main__":
    main()
