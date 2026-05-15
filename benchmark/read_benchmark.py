#!/usr/bin/env python3
"""
Read Benchmark: MongoDB vs ClickHouse

Generates extensive sample data, loads it into both MongoDB and ClickHouse,
then runs identical analytical queries against both to compare read performance.

Usage:
    python3 benchmark/read_benchmark.py [--records 100000] [--iterations 5]
"""

import argparse
import json
import random
import string
import time
import statistics
import sys
from datetime import datetime, timedelta

try:
    import pymongo
except ImportError:
    sys.exit("pymongo not installed. Run: pip install pymongo")

try:
    import requests
except ImportError:
    sys.exit("requests not installed. Run: pip install requests")


# ============================================================
# Configuration
# ============================================================

MONGO_URI = "mongodb://localhost:27017/?directConnection=true"
MONGO_DB = "benchmark"
CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_DB = "benchmark"

COLLECTIONS = {
    "orders": {
        "ch_table": "orders",
        "fields": ["order_id", "customer_id", "amount", "status", "region", "created_at"],
    },
    "events": {
        "ch_table": "events",
        "fields": ["event_id", "user_id", "event_type", "page", "duration_ms", "ts"],
    },
}

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]
EVENT_TYPES = ["page_view", "click", "scroll", "form_submit", "purchase", "logout"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/profile", "/search", "/about"]


# ============================================================
# Data Generation
# ============================================================

def generate_orders(n):
    """Generate n order documents."""
    base_date = datetime(2023, 1, 1)
    records = []
    for i in range(n):
        records.append({
            "order_id": f"ORD-{i:08d}",
            "customer_id": f"CUST-{random.randint(1, n // 10):06d}",
            "amount": round(random.uniform(5.0, 5000.0), 2),
            "status": random.choice(STATUSES),
            "region": random.choice(REGIONS),
            "created_at": (base_date + timedelta(
                seconds=random.randint(0, 365 * 24 * 3600)
            )).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


def generate_events(n):
    """Generate n event documents."""
    base_date = datetime(2023, 6, 1)
    records = []
    for i in range(n):
        records.append({
            "event_id": f"EVT-{i:08d}",
            "user_id": f"USR-{random.randint(1, n // 20):06d}",
            "event_type": random.choice(EVENT_TYPES),
            "page": random.choice(PAGES),
            "duration_ms": random.randint(50, 30000),
            "ts": (base_date + timedelta(
                seconds=random.randint(0, 180 * 24 * 3600)
            )).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


# ============================================================
# Data Loading
# ============================================================

def load_mongo(collection_name, records):
    """Bulk insert records into MongoDB."""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[collection_name]
    coll.drop()

    # Create indexes for common query patterns
    if collection_name == "orders":
        coll.create_index("status")
        coll.create_index("region")
        coll.create_index("customer_id")
        coll.create_index("created_at")
        coll.create_index([("region", 1), ("status", 1)])
    elif collection_name == "events":
        coll.create_index("event_type")
        coll.create_index("user_id")
        coll.create_index("page")
        coll.create_index("ts")
        coll.create_index([("event_type", 1), ("page", 1)])

    batch_size = 10000
    for i in range(0, len(records), batch_size):
        coll.insert_many(records[i:i + batch_size])

    client.close()


def ch_query(sql):
    """Execute a ClickHouse query and return the response."""
    resp = requests.post(CLICKHOUSE_URL, data=sql)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error: {resp.text}")
    return resp.text


def load_clickhouse(collection_name, records):
    """Bulk insert records into ClickHouse."""
    cfg = COLLECTIONS[collection_name]
    table = cfg["ch_table"]

    ch_query(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    ch_query(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{table}")

    if collection_name == "orders":
        ch_query(f"""
            CREATE TABLE {CLICKHOUSE_DB}.{table} (
                order_id String,
                customer_id String,
                amount Float64,
                status String,
                region String,
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY (region, created_at)
        """)
    elif collection_name == "events":
        ch_query(f"""
            CREATE TABLE {CLICKHOUSE_DB}.{table} (
                event_id String,
                user_id String,
                event_type String,
                page String,
                duration_ms UInt32,
                ts DateTime
            ) ENGINE = MergeTree()
            ORDER BY (event_type, ts)
        """)

    # Insert in batches using JSONEachRow format
    batch_size = 50000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        # Remove MongoDB _id field if present
        payload = "\n".join(
            json.dumps({k: v for k, v in r.items() if k != "_id"})
            for r in batch
        )
        url = f"{CLICKHOUSE_URL}/?query=INSERT+INTO+{CLICKHOUSE_DB}.{table}+FORMAT+JSONEachRow"
        resp = requests.post(url, data=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"ClickHouse insert error: {resp.text}")


# ============================================================
# Benchmark Queries
# ============================================================

QUERIES = [
    {
        "name": "Simple filter (status = 'shipped')",
        "mongo": lambda db: list(db.orders.find({"status": "shipped"}).limit(1000)),
        "clickhouse": f"SELECT * FROM {CLICKHOUSE_DB}.orders WHERE status = 'shipped' LIMIT 1000 FORMAT Null",
    },
    {
        "name": "Range scan (amount > 1000)",
        "mongo": lambda db: list(db.orders.find({"amount": {"$gt": 1000}}).limit(5000)),
        "clickhouse": f"SELECT * FROM {CLICKHOUSE_DB}.orders WHERE amount > 1000 LIMIT 5000 FORMAT Null",
    },
    {
        "name": "Count by status (GROUP BY)",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ])),
        "clickhouse": f"SELECT status, count() as count FROM {CLICKHOUSE_DB}.orders GROUP BY status FORMAT Null",
    },
    {
        "name": "Avg amount by region (GROUP BY)",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {"_id": "$region", "avg_amount": {"$avg": "$amount"}}}
        ])),
        "clickhouse": f"SELECT region, avg(amount) as avg_amount FROM {CLICKHOUSE_DB}.orders GROUP BY region FORMAT Null",
    },
    {
        "name": "Top 10 customers by total spend",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ])),
        "clickhouse": f"SELECT customer_id, sum(amount) as total FROM {CLICKHOUSE_DB}.orders GROUP BY customer_id ORDER BY total DESC LIMIT 10 FORMAT Null",
    },
    {
        "name": "Multi-filter (region + status + amount range)",
        "mongo": lambda db: list(db.orders.find({
            "region": "us-east",
            "status": {"$in": ["shipped", "delivered"]},
            "amount": {"$gte": 100, "$lte": 2000}
        })),
        "clickhouse": f"SELECT * FROM {CLICKHOUSE_DB}.orders WHERE region = 'us-east' AND status IN ('shipped', 'delivered') AND amount >= 100 AND amount <= 2000 FORMAT Null",
    },
    {
        "name": "Date range scan (3 months)",
        "mongo": lambda db: list(db.orders.find({
            "created_at": {"$gte": "2023-04-01 00:00:00", "$lt": "2023-07-01 00:00:00"}
        })),
        "clickhouse": f"SELECT * FROM {CLICKHOUSE_DB}.orders WHERE created_at >= '2023-04-01 00:00:00' AND created_at < '2023-07-01 00:00:00' FORMAT Null",
    },
    {
        "name": "Events: count by type and page (2-dim GROUP BY)",
        "mongo": lambda db: list(db.events.aggregate([
            {"$group": {"_id": {"type": "$event_type", "page": "$page"}, "count": {"$sum": 1}}}
        ])),
        "clickhouse": f"SELECT event_type, page, count() as count FROM {CLICKHOUSE_DB}.events GROUP BY event_type, page FORMAT Null",
    },
    {
        "name": "Events: avg duration by event type",
        "mongo": lambda db: list(db.events.aggregate([
            {"$group": {"_id": "$event_type", "avg_dur": {"$avg": "$duration_ms"}}}
        ])),
        "clickhouse": f"SELECT event_type, avg(duration_ms) as avg_dur FROM {CLICKHOUSE_DB}.events GROUP BY event_type FORMAT Null",
    },
    {
        "name": "Events: top 20 users by event count",
        "mongo": lambda db: list(db.events.aggregate([
            {"$group": {"_id": "$user_id", "cnt": {"$sum": 1}}},
            {"$sort": {"cnt": -1}},
            {"$limit": 20}
        ])),
        "clickhouse": f"SELECT user_id, count() as cnt FROM {CLICKHOUSE_DB}.events GROUP BY user_id ORDER BY cnt DESC LIMIT 20 FORMAT Null",
    },
    {
        "name": "Full table scan count",
        "mongo": lambda db: db.orders.count_documents({}),
        "clickhouse": f"SELECT count() FROM {CLICKHOUSE_DB}.orders FORMAT Null",
    },
    {
        "name": "Percentile approximation (heavy analytics)",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {
                "_id": "$region",
                "p50": {"$avg": "$amount"},
                "max_amount": {"$max": "$amount"},
                "min_amount": {"$min": "$amount"},
                "total_orders": {"$sum": 1}
            }},
            {"$sort": {"total_orders": -1}}
        ])),
        "clickhouse": f"SELECT region, quantile(0.5)(amount) as p50, max(amount), min(amount), count() as total_orders FROM {CLICKHOUSE_DB}.orders GROUP BY region ORDER BY total_orders DESC FORMAT Null",
    },
]


# ============================================================
# Benchmark Runner
# ============================================================

def run_benchmark(iterations):
    """Run all queries and collect timing data."""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    results = []

    for q in QUERIES:
        mongo_times = []
        ch_times = []

        # Warmup
        try:
            q["mongo"](db)
        except Exception:
            pass
        try:
            ch_query(q["clickhouse"])
        except Exception:
            pass

        for _ in range(iterations):
            # MongoDB
            start = time.perf_counter()
            try:
                q["mongo"](db)
                elapsed = (time.perf_counter() - start) * 1000
                mongo_times.append(elapsed)
            except Exception as e:
                mongo_times.append(float("inf"))

            # ClickHouse
            start = time.perf_counter()
            try:
                ch_query(q["clickhouse"])
                elapsed = (time.perf_counter() - start) * 1000
                ch_times.append(elapsed)
            except Exception as e:
                ch_times.append(float("inf"))

        results.append({
            "query": q["name"],
            "mongo_avg_ms": statistics.mean(mongo_times),
            "mongo_p50_ms": statistics.median(mongo_times),
            "mongo_min_ms": min(mongo_times),
            "ch_avg_ms": statistics.mean(ch_times),
            "ch_p50_ms": statistics.median(ch_times),
            "ch_min_ms": min(ch_times),
            "speedup": statistics.mean(mongo_times) / max(statistics.mean(ch_times), 0.001),
        })

    client.close()
    return results


def print_results(results, record_count):
    """Print benchmark results as a formatted table."""
    print("\n" + "=" * 100)
    print(f"  READ BENCHMARK RESULTS — {record_count:,} records per collection")
    print("=" * 100)
    print(f"\n{'Query':<50} {'MongoDB (ms)':<15} {'ClickHouse (ms)':<17} {'Speedup':<10}")
    print("-" * 92)

    for r in results:
        mongo_str = f"{r['mongo_avg_ms']:.1f}"
        ch_str = f"{r['ch_avg_ms']:.1f}"
        speedup_str = f"{r['speedup']:.1f}x"
        print(f"{r['query']:<50} {mongo_str:<15} {ch_str:<17} {speedup_str:<10}")

    print("-" * 92)

    # Summary
    avg_speedup = statistics.mean(r["speedup"] for r in results)
    max_speedup = max(r["speedup"] for r in results)
    max_query = next(r["query"] for r in results if r["speedup"] == max_speedup)

    print(f"\n  Average speedup (ClickHouse vs MongoDB): {avg_speedup:.1f}x")
    print(f"  Max speedup: {max_speedup:.1f}x on '{max_query}'")
    print()

    # Detailed breakdown
    print("\nDetailed Timing (avg / p50 / min):")
    print(f"{'Query':<50} {'MongoDB':<25} {'ClickHouse':<25}")
    print("-" * 100)
    for r in results:
        m = f"{r['mongo_avg_ms']:.1f} / {r['mongo_p50_ms']:.1f} / {r['mongo_min_ms']:.1f}"
        c = f"{r['ch_avg_ms']:.1f} / {r['ch_p50_ms']:.1f} / {r['ch_min_ms']:.1f}"
        print(f"{r['query']:<50} {m:<25} {c:<25}")

    print()


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="MongoDB vs ClickHouse read benchmark")
    parser.add_argument("--records", type=int, default=100000,
                        help="Number of records per collection (default: 100000)")
    parser.add_argument("--iterations", type=int, default=5,
                        help="Number of iterations per query (default: 5)")
    parser.add_argument("--skip-load", action="store_true",
                        help="Skip data loading (use existing data)")
    args = parser.parse_args()

    record_count = args.records
    iterations = args.iterations

    print(f"\n{'='*60}")
    print(f"  mg-clickhouse Read Benchmark")
    print(f"  Records per collection: {record_count:,}")
    print(f"  Query iterations: {iterations}")
    print(f"{'='*60}\n")

    if not args.skip_load:
        # Generate data
        print("[1/4] Generating sample data...")
        orders = generate_orders(record_count)
        events = generate_events(record_count)
        print(f"      Generated {len(orders):,} orders + {len(events):,} events")

        # Load into MongoDB
        print("[2/4] Loading data into MongoDB...")
        t0 = time.perf_counter()
        load_mongo("orders", orders)
        load_mongo("events", events)
        mongo_load_time = time.perf_counter() - t0
        print(f"      Done in {mongo_load_time:.1f}s")

        # Load into ClickHouse
        print("[3/4] Loading data into ClickHouse...")
        t0 = time.perf_counter()
        load_clickhouse("orders", orders)
        load_clickhouse("events", events)
        ch_load_time = time.perf_counter() - t0
        print(f"      Done in {ch_load_time:.1f}s")
    else:
        print("[1/4] Skipping data generation (--skip-load)")
        print("[2/4] Skipping MongoDB load")
        print("[3/4] Skipping ClickHouse load")

    # Run benchmark
    print(f"[4/4] Running benchmark ({iterations} iterations per query)...")
    results = run_benchmark(iterations)

    # Print results
    print_results(results, record_count)

    # Save results to JSON
    output_file = "benchmark/results.json"
    with open(output_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "record_count": record_count,
            "iterations": iterations,
            "results": results,
        }, f, indent=2)
    print(f"  Results saved to {output_file}")


if __name__ == "__main__":
    main()
