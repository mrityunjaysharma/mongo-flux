#!/usr/bin/env python3
"""
MongoFlux Integration Test Suite

Tests CRUD operations on MongoDB, verifies real-time sync to ClickHouse,
and benchmarks aggregation queries comparing both engines.

Runs as an HTTP server on port 8080 with endpoints:
    GET  /run          - Run full test suite, return JSON results
    GET  /benchmark    - Run only the aggregation benchmark
    GET  /health       - Health check
    GET  /status       - Show current data counts

Usage:
    python3 test-app/test_mongoflux.py                    # run as server on :8080
    python3 test-app/test_mongoflux.py --once             # run once and exit
    python3 test-app/test_mongoflux.py --once --cleanup   # run once, cleanup, exit

Environment Variables (override defaults):
    MONGO_URI           - MongoDB connection string (default: localhost:27017)
    CH_URL              - ClickHouse HTTP URL (default: http://localhost:8123)
    MONGOFLUX_API       - MongoFlux API URL (default: http://localhost:9090)
    TEST_RECORDS        - Number of records to insert (default: 1000000)
    SERVER_PORT         - HTTP server port (default: 8080)
"""

import json
import logging
import os
import random
import statistics
import sys
import time
import threading
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

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
MONGO_COLLECTION = "mongoflux_test"
CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_DB = os.environ.get("CH_DB", "myapp")
CH_TABLE = "mongoflux_test"
MONGOFLUX_API = os.environ.get("MONGOFLUX_API", "http://localhost:9090")
TEST_RECORDS = int(os.environ.get("TEST_RECORDS", "1000000"))
SERVER_PORT = int(os.environ.get("SERVER_PORT", "8080"))

# HTTP session with connection pooling
SESSION = requests.Session()
SESSION.mount("http://", HTTPAdapter(pool_connections=5, pool_maxsize=5))
REQUEST_TIMEOUT = 30

# ============================================================
# Helpers
# ============================================================


def ch_query(sql: str) -> str:
    resp = SESSION.post(CH_URL, data=sql, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error ({resp.status_code}): {resp.text.strip()[:300]}")
    return resp.text.strip()


def api_call(method: str, path: str, data: Optional[Dict] = None) -> Dict[str, Any]:
    url = f"{MONGOFLUX_API}{path}"
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
    return count


def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


# ============================================================
# Setup
# ============================================================


def setup_mapping() -> Dict[str, Any]:
    """Create mapping and ClickHouse table."""
    results = {"steps": []}

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
    results["steps"].append({"mapping": result.get("status", "unknown")})

    result = api_call("POST", f"/api/v1/mappings/{MONGO_COLLECTION}/sync")
    results["steps"].append({"table": result.get("status", "unknown")})

    for attempt in range(3):
        try:
            api_call("POST", "/api/v1/sync/restart")
            break
        except (requests.ConnectionError, RuntimeError):
            time.sleep(2)
    time.sleep(4)
    results["steps"].append({"sync": "restarted"})

    return results


# ============================================================
# Data Loading
# ============================================================


def load_test_data(coll, record_count: int) -> Dict[str, Any]:
    """Insert records into MongoDB and load into ClickHouse."""
    categories = ["Infrastructure", "Application", "Security", "Network", "Database", "Storage", "Compute", "Messaging"]
    regions = ["US", "EU", "APAC", "LATAM", "MEA", "ANZ"]
    statuses = ["active", "resolved", "acknowledged", "suppressed", "escalated", "closed"]

    batch_size = 10000
    inserted = 0
    t0 = time.perf_counter()

    logger.info(f"Inserting {record_count:,} documents into MongoDB...")
    for batch_start in range(0, record_count, batch_size):
        current_batch = min(batch_size, record_count - batch_start)
        docs = [
            {
                "name": f"Alert {batch_start + i}",
                "category": random.choice(categories),
                "region": random.choice(regions),
                "status": random.choice(statuses),
                "value": random.randint(1, 10000),
                "score": round(random.uniform(0, 100), 2),
                "createdAt": datetime(2024, 1, 1) + timedelta(seconds=random.randint(0, 365 * 24 * 3600)),
                "tags": json.dumps(random.sample(["critical", "prod", "staging", "low", "high", "p1", "p2", "infra"], 3)),
                "active": random.choice([True, False]),
            }
            for i in range(current_batch)
        ]
        coll.insert_many(docs, ordered=False)
        inserted += current_batch
        if inserted % 100000 == 0:
            elapsed = time.perf_counter() - t0
            logger.info(f"  {inserted:>10,} inserted ({inserted/elapsed:,.0f} docs/s)")

    mongo_elapsed = time.perf_counter() - t0
    mongo_rate = record_count / mongo_elapsed

    # Load into ClickHouse directly (bypass sync for reliable benchmarking)
    logger.info("Loading data into ClickHouse...")
    t1 = time.perf_counter()
    ch_batch_size = 50000
    ch_loaded = 0

    cursor = coll.find({})
    batch = []
    for doc in cursor:
        row = {
            "id": str(doc.get("_id", "")),
            "name": doc.get("name", ""),
            "category": doc.get("category", ""),
            "region": doc.get("region", ""),
            "status": doc.get("status", ""),
            "value": doc.get("value", 0),
            "score": doc.get("score", 0.0),
            "created_at": doc["createdAt"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if isinstance(doc.get("createdAt"), datetime) else "2024-01-01 00:00:00.000",
            "tags": doc.get("tags", "[]"),
            "active": 1 if doc.get("active") else 0,
        }
        batch.append(json.dumps(row))
        if len(batch) >= ch_batch_size:
            payload = "\n".join(batch)
            resp = SESSION.post(f"{CH_URL}/?query=INSERT+INTO+{CH_DB}.{CH_TABLE}+FORMAT+JSONEachRow", data=payload, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(f"CH insert error: {resp.text[:200]}")
            ch_loaded += len(batch)
            batch = []
            if ch_loaded % 200000 == 0:
                logger.info(f"  CH: {ch_loaded:,} loaded...")

    if batch:
        payload = "\n".join(batch)
        SESSION.post(f"{CH_URL}/?query=INSERT+INTO+{CH_DB}.{CH_TABLE}+FORMAT+JSONEachRow", data=payload, timeout=60)
        ch_loaded += len(batch)

    ch_elapsed = time.perf_counter() - t1

    return {
        "records": record_count,
        "mongo_insert_time_s": round(mongo_elapsed, 1),
        "mongo_insert_rate": int(mongo_rate),
        "ch_load_time_s": round(ch_elapsed, 1),
        "ch_load_rate": int(ch_loaded / ch_elapsed),
        "ch_rows_loaded": ch_loaded,
    }


# ============================================================
# Benchmark
# ============================================================


def run_benchmark(coll) -> Dict[str, Any]:
    """Run aggregation queries against both MongoDB and ClickHouse."""
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
            "name": "2D GROUP BY: category x region",
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
            "mongo": [{"$group": {"_id": "$region", "names": {"$addToSet": "$name"}}}],
            "ch": f"SELECT region, uniqExact(name) as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY region",
        },
        {
            "name": "Score stats by status",
            "mongo": [{"$group": {"_id": "$status", "avg": {"$avg": "$score"}, "min": {"$min": "$score"}, "max": {"$max": "$score"}}}],
            "ch": f"SELECT status, avg(score), min(score), max(score) FROM {CH_DB}.{CH_TABLE} GROUP BY status",
        },
        {
            "name": "3D GROUP BY: status x region x active",
            "mongo": [{"$group": {"_id": {"s": "$status", "r": "$region", "a": "$active"}, "count": {"$sum": 1}}}, {"$sort": {"count": -1}}],
            "ch": f"SELECT status, region, active, count() as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY status, region, active ORDER BY cnt DESC",
        },
        {
            "name": "Full table count",
            "mongo": [{"$count": "total"}],
            "ch": f"SELECT count() as total FROM {CH_DB}.{CH_TABLE}",
        },
    ]

    results = []
    for q in queries:
        # Warmup
        try:
            list(coll.aggregate(q["mongo"], allowDiskUse=True))
        except Exception:
            pass
        try:
            ch_query(q["ch"])
        except Exception:
            pass

        mongo_times = []
        ch_times = []
        for _ in range(3):
            t = time.perf_counter()
            try:
                list(coll.aggregate(q["mongo"], allowDiskUse=True))
                mongo_times.append((time.perf_counter() - t) * 1000)
            except Exception:
                mongo_times.append(float("inf"))

            t = time.perf_counter()
            try:
                ch_query(q["ch"])
                ch_times.append((time.perf_counter() - t) * 1000)
            except Exception:
                ch_times.append(float("inf"))

        m_avg = statistics.mean(mongo_times)
        c_avg = statistics.mean(ch_times)
        speedup = m_avg / max(c_avg, 0.01)
        results.append({
            "query": q["name"],
            "mongo_ms": round(m_avg, 1),
            "clickhouse_ms": round(c_avg, 1),
            "speedup": round(speedup, 1),
        })

    avg_speedup = statistics.mean(r["speedup"] for r in results)
    peak_speedup = max(r["speedup"] for r in results)
    peak_query = next(r["query"] for r in results if r["speedup"] == peak_speedup)

    return {
        "queries": results,
        "avg_speedup": round(avg_speedup, 1),
        "peak_speedup": round(peak_speedup, 1),
        "peak_query": peak_query,
        "record_count": int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}")),
    }


# ============================================================
# Full Test Suite
# ============================================================


def run_full_test(record_count: int = TEST_RECORDS) -> Dict[str, Any]:
    """Run the complete test suite: setup, load, benchmark."""
    results = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "mongo_uri": MONGO_URI,
            "ch_url": CH_URL,
            "mongoflux_api": MONGOFLUX_API,
            "record_count": record_count,
        },
    }

    # Preflight
    try:
        SESSION.get(f"{MONGOFLUX_API}/health", timeout=5)
    except Exception as e:
        return {"error": f"MongoFlux not reachable: {e}"}
    try:
        ch_query("SELECT 1")
    except Exception as e:
        return {"error": f"ClickHouse not reachable: {e}"}

    client = get_mongo_client()
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    # Clean slate
    coll.drop()
    try:
        ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
    except Exception:
        pass
    try:
        ch_query(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
    except Exception:
        pass

    # Setup
    logger.info("Setting up mapping...")
    try:
        results["setup"] = setup_mapping()
    except Exception as e:
        client.close()
        return {"error": f"Setup failed: {e}"}

    # Load data
    logger.info(f"Loading {record_count:,} records...")
    try:
        results["load"] = load_test_data(coll, record_count)
    except Exception as e:
        client.close()
        return {"error": f"Data load failed: {e}"}

    # Benchmark
    logger.info("Running benchmark...")
    results["benchmark"] = run_benchmark(coll)

    # Summary
    mongo_count = coll.count_documents({})
    ch_count = int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}"))
    results["summary"] = {
        "mongodb_documents": mongo_count,
        "clickhouse_rows": ch_count,
        "data_synced": ch_count >= mongo_count,
        "avg_speedup": results["benchmark"]["avg_speedup"],
        "peak_speedup": results["benchmark"]["peak_speedup"],
    }

    client.close()
    return results


# ============================================================
# HTTP Server
# ============================================================


class TestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logger.info(f"{self.address_string()} - {format % args}")

    def send_json(self, data: Any, status: int = 200):
        body = json.dumps(data, indent=2, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/health":
            self.send_json({"status": "ok", "service": "mongoflux-test-runner"})

        elif path == "/status":
            try:
                client = get_mongo_client()
                db = client[MONGO_DB]
                mongo_count = db[MONGO_COLLECTION].count_documents({})
                client.close()
                ch_count = int(ch_query(f"SELECT count() FROM {CH_DB}.{CH_TABLE}"))
                self.send_json({
                    "mongodb_documents": mongo_count,
                    "clickhouse_rows": ch_count,
                    "collection": MONGO_COLLECTION,
                })
            except Exception as e:
                self.send_json({"error": str(e)}, 500)

        elif path == "/run":
            records = int(params.get("records", [str(TEST_RECORDS)])[0])
            logger.info(f"Starting full test with {records:,} records...")
            result = run_full_test(records)
            status = 200 if "error" not in result else 500
            self.send_json(result, status)

        elif path == "/benchmark":
            try:
                client = get_mongo_client()
                coll = client[MONGO_DB][MONGO_COLLECTION]
                result = run_benchmark(coll)
                client.close()
                self.send_json(result)
            except Exception as e:
                self.send_json({"error": str(e)}, 500)

        elif path == "/cleanup":
            try:
                client = get_mongo_client()
                client[MONGO_DB][MONGO_COLLECTION].drop()
                client.close()
                ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
                api_call("DELETE", f"/api/v1/mappings/{MONGO_COLLECTION}")
                self.send_json({"status": "cleaned"})
            except Exception as e:
                self.send_json({"error": str(e)}, 500)

        else:
            self.send_json({
                "endpoints": {
                    "GET /health": "Health check",
                    "GET /status": "Show current data counts",
                    "GET /run?records=1000000": "Run full test suite (setup + load + benchmark)",
                    "GET /benchmark": "Run benchmark only (data must exist)",
                    "GET /cleanup": "Remove test data",
                },
                "config": {
                    "mongo_uri": MONGO_URI,
                    "ch_url": CH_URL,
                    "mongoflux_api": MONGOFLUX_API,
                    "default_records": TEST_RECORDS,
                },
            })


# ============================================================
# CLI Output (for --once mode)
# ============================================================


def print_results(results: Dict[str, Any]) -> None:
    if "error" in results:
        print(f"\n  ERROR: {results['error']}")
        return

    print(f"\n{'='*75}")
    print(f"  MongoFlux Integration Test Results")
    print(f"{'='*75}")

    if "load" in results:
        load = results["load"]
        print(f"\n  Data Load:")
        print(f"    Records:          {load['records']:,}")
        print(f"    MongoDB insert:   {load['mongo_insert_time_s']}s ({load['mongo_insert_rate']:,} docs/s)")
        print(f"    ClickHouse load:  {load['ch_load_time_s']}s ({load['ch_load_rate']:,} rows/s)")

    if "benchmark" in results:
        bench = results["benchmark"]
        print(f"\n  Aggregation Benchmark ({bench['record_count']:,} records):")
        print(f"    {'Query':<38} {'MongoDB':<12} {'ClickHouse':<12} {'Speedup':<8}")
        print(f"    {'-'*70}")
        for q in bench["queries"]:
            print(f"    {q['query']:<38} {q['mongo_ms']:<12.1f} {q['clickhouse_ms']:<12.1f} {q['speedup']:.1f}x")
        print(f"    {'-'*70}")
        print(f"\n    Average speedup: {bench['avg_speedup']}x")
        print(f"    Peak speedup:    {bench['peak_speedup']}x ({bench['peak_query']})")

    if "summary" in results:
        s = results["summary"]
        print(f"\n  Summary:")
        print(f"    MongoDB documents:  {s['mongodb_documents']:,}")
        print(f"    ClickHouse rows:    {s['clickhouse_rows']:,}")
        print(f"    Data synced:        {'✓' if s['data_synced'] else '✗'}")
        print(f"    Avg query speedup:  {s['avg_speedup']}x")
        print(f"    Peak query speedup: {s['peak_speedup']}x")

    print()


# ============================================================
# Main
# ============================================================


def main() -> None:
    if "--once" in sys.argv:
        # Run once and exit (CLI mode)
        records = TEST_RECORDS
        for arg in sys.argv:
            if arg.startswith("--records="):
                records = int(arg.split("=")[1])

        results = run_full_test(records)
        print_results(results)

        if "--cleanup" in sys.argv and "error" not in results:
            logger.info("Cleaning up...")
            client = get_mongo_client()
            client[MONGO_DB][MONGO_COLLECTION].drop()
            client.close()
            try:
                ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
            except Exception:
                pass
            try:
                api_call("DELETE", f"/api/v1/mappings/{MONGO_COLLECTION}")
            except Exception:
                pass
            print("  Cleaned up.")

        sys.exit(0 if "error" not in results else 1)

    # Server mode
    server = HTTPServer(("0.0.0.0", SERVER_PORT), TestHandler)
    print(f"\n{'='*60}")
    print(f"  MongoFlux Test Server")
    print(f"  Listening on http://0.0.0.0:{SERVER_PORT}")
    print(f"{'='*60}")
    print(f"\n  Endpoints:")
    print(f"    GET /              - Show available endpoints")
    print(f"    GET /health        - Health check")
    print(f"    GET /status        - Current data counts")
    print(f"    GET /run           - Run full test ({TEST_RECORDS:,} records)")
    print(f"    GET /run?records=N - Run with custom record count")
    print(f"    GET /benchmark     - Run benchmark only")
    print(f"    GET /cleanup       - Remove test data")
    print(f"\n  Config:")
    print(f"    MongoDB:    {MONGO_URI}")
    print(f"    ClickHouse: {CH_URL}")
    print(f"    MongoFlux:  {MONGOFLUX_API}")
    print(f"    Records:    {TEST_RECORDS:,}")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
