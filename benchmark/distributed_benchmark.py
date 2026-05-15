#!/usr/bin/env python3
"""
Distributed vs Standalone ClickHouse Benchmark

Compares read performance between:
  1. Standalone ClickHouse (single node, MergeTree)
  2. Distributed ClickHouse (3 shards, Distributed engine)

Both are compared against MongoDB as baseline.

Usage:
    # Start the cluster first:
    #   docker compose -f benchmark/docker-compose-cluster.yml up -d
    # Then run:
    python3 benchmark/distributed_benchmark.py --records 500000
"""

import argparse
import json
import random
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


MONGO_URI = "mongodb://localhost:27017/?directConnection=true"
MONGO_DB = "dist_benchmark"

# Standalone ClickHouse (from main docker-compose)
CH_STANDALONE_URL = "http://localhost:8123"
CH_STANDALONE_DB = "bench_standalone"

# Distributed ClickHouse (shard 1 as entry point)
CH_DISTRIBUTED_URL = "http://localhost:8124"
CH_DISTRIBUTED_DB = "bench_distributed"
CH_CLUSTER_NAME = "bench_cluster"

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]


def generate_orders(n):
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


def ch_query(url, sql):
    resp = requests.post(url, data=sql)
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error: {resp.text.strip()}")
    return resp.text


def load_mongo(records):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db["orders"]
    coll.drop()
    coll.create_index("status")
    coll.create_index("region")
    coll.create_index("customer_id")
    coll.create_index("created_at")
    coll.create_index([("region", 1), ("status", 1)])

    batch_size = 10000
    for i in range(0, len(records), batch_size):
        coll.insert_many(records[i:i + batch_size])
    client.close()


def load_standalone(records):
    ch_query(CH_STANDALONE_URL, f"CREATE DATABASE IF NOT EXISTS {CH_STANDALONE_DB}")
    ch_query(CH_STANDALONE_URL, f"DROP TABLE IF EXISTS {CH_STANDALONE_DB}.orders")
    ch_query(CH_STANDALONE_URL, f"""
        CREATE TABLE {CH_STANDALONE_DB}.orders (
            order_id String,
            customer_id String,
            amount Float64,
            status LowCardinality(String),
            region LowCardinality(String),
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (region, created_at)
    """)

    batch_size = 50000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = "\n".join(
            json.dumps({k: v for k, v in r.items() if k != "_id"}) for r in batch
        )
        url = f"{CH_STANDALONE_URL}/?query=INSERT+INTO+{CH_STANDALONE_DB}.orders+FORMAT+JSONEachRow"
        resp = requests.post(url, data=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Standalone insert error: {resp.text.strip()}")


def load_distributed(records):
    # Create database on each shard individually
    shard_urls = [
        "http://localhost:8124",
        "http://localhost:8125",
        "http://localhost:8126",
    ]

    for url in shard_urls:
        ch_query(url, f"CREATE DATABASE IF NOT EXISTS {CH_DISTRIBUTED_DB}")
        ch_query(url, f"DROP TABLE IF EXISTS {CH_DISTRIBUTED_DB}.orders_local")
        ch_query(url, f"DROP TABLE IF EXISTS {CH_DISTRIBUTED_DB}.orders")
        ch_query(url, f"""
            CREATE TABLE {CH_DISTRIBUTED_DB}.orders_local (
                order_id String,
                customer_id String,
                amount Float64,
                status LowCardinality(String),
                region LowCardinality(String),
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY (region, created_at)
        """)
        ch_query(url, f"""
            CREATE TABLE {CH_DISTRIBUTED_DB}.orders
            AS {CH_DISTRIBUTED_DB}.orders_local
            ENGINE = Distributed('{CH_CLUSTER_NAME}', '{CH_DISTRIBUTED_DB}', 'orders_local', cityHash64(customer_id))
        """)

    # Insert via distributed table on shard1 (auto-routes to all shards)
    batch_size = 50000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = "\n".join(
            json.dumps({k: v for k, v in r.items() if k != "_id"}) for r in batch
        )
        url = f"{CH_DISTRIBUTED_URL}/?query=INSERT+INTO+{CH_DISTRIBUTED_DB}.orders+FORMAT+JSONEachRow"
        resp = requests.post(url, data=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Distributed insert error: {resp.text.strip()}")

    # Wait for distributed inserts to propagate
    time.sleep(3)


QUERIES = [
    {
        "name": "Count by status (GROUP BY)",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ])),
        "sql": "SELECT status, count() as cnt FROM {db}.orders GROUP BY status FORMAT Null",
    },
    {
        "name": "Avg amount by region",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {"_id": "$region", "avg_amount": {"$avg": "$amount"}}}
        ])),
        "sql": "SELECT region, avg(amount) FROM {db}.orders GROUP BY region FORMAT Null",
    },
    {
        "name": "Top 10 customers by spend",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ])),
        "sql": "SELECT customer_id, sum(amount) as total FROM {db}.orders GROUP BY customer_id ORDER BY total DESC LIMIT 10 FORMAT Null",
    },
    {
        "name": "Multi-filter compound",
        "mongo": lambda db: list(db.orders.find({
            "region": "us-east",
            "status": {"$in": ["shipped", "delivered"]},
            "amount": {"$gte": 100, "$lte": 2000}
        })),
        "sql": "SELECT * FROM {db}.orders WHERE region = 'us-east' AND status IN ('shipped', 'delivered') AND amount >= 100 AND amount <= 2000 FORMAT Null",
    },
    {
        "name": "Date range scan (3 months)",
        "mongo": lambda db: list(db.orders.find({
            "created_at": {"$gte": "2023-04-01 00:00:00", "$lt": "2023-07-01 00:00:00"}
        })),
        "sql": "SELECT * FROM {db}.orders WHERE created_at >= '2023-04-01' AND created_at < '2023-07-01' FORMAT Null",
    },
    {
        "name": "Full table count",
        "mongo": lambda db: db.orders.count_documents({}),
        "sql": "SELECT count() FROM {db}.orders FORMAT Null",
    },
    {
        "name": "Percentile + multi-agg",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {
                "_id": "$region",
                "avg_amount": {"$avg": "$amount"},
                "max_amount": {"$max": "$amount"},
                "min_amount": {"$min": "$amount"},
                "total_orders": {"$sum": 1}
            }},
            {"$sort": {"total_orders": -1}}
        ])),
        "sql": "SELECT region, avg(amount), max(amount), min(amount), count() as total FROM {db}.orders GROUP BY region ORDER BY total DESC FORMAT Null",
    },
    {
        "name": "Heavy aggregation (uniqExact)",
        "mongo": lambda db: list(db.orders.aggregate([
            {"$group": {
                "_id": "$region",
                "unique_customers": {"$addToSet": "$customer_id"}
            }},
        ])),
        "sql": "SELECT region, uniqExact(customer_id) as unique_customers FROM {db}.orders GROUP BY region FORMAT Null",
    },
]


def run_queries(label, url, db_name, iterations):
    times = {}
    for q in QUERIES:
        sql = q["sql"].format(db=db_name)
        # Warmup
        try:
            ch_query(url, sql)
        except Exception:
            pass

        query_times = []
        for _ in range(iterations):
            start = time.perf_counter()
            try:
                ch_query(url, sql)
                query_times.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                query_times.append(float("inf"))

        times[q["name"]] = {
            "avg_ms": statistics.mean(query_times),
            "p50_ms": statistics.median(query_times),
            "min_ms": min(query_times),
        }
    return times


def run_mongo_queries(iterations):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    times = {}

    for q in QUERIES:
        # Warmup
        try:
            q["mongo"](db)
        except Exception:
            pass

        query_times = []
        for _ in range(iterations):
            start = time.perf_counter()
            try:
                q["mongo"](db)
                query_times.append((time.perf_counter() - start) * 1000)
            except Exception:
                query_times.append(float("inf"))

        times[q["name"]] = {
            "avg_ms": statistics.mean(query_times),
            "p50_ms": statistics.median(query_times),
            "min_ms": min(query_times),
        }

    client.close()
    return times


def print_results(mongo_times, standalone_times, distributed_times, record_count):
    print(f"\n{'=' * 110}")
    print(f"  DISTRIBUTED vs STANDALONE BENCHMARK — {record_count:,} records")
    print(f"{'=' * 110}")
    print(f"\n{'Query':<35} {'MongoDB (ms)':<14} {'Standalone (ms)':<17} {'Distributed (ms)':<18} {'S speedup':<11} {'D speedup':<11}")
    print(f"{'-' * 110}")

    for q in QUERIES:
        name = q["name"]
        m = mongo_times[name]["avg_ms"]
        s = standalone_times[name]["avg_ms"]
        d = distributed_times[name]["avg_ms"]
        s_speed = m / max(s, 0.001)
        d_speed = m / max(d, 0.001)
        print(f"{name:<35} {m:<14.1f} {s:<17.1f} {d:<18.1f} {s_speed:<11.1f}x {d_speed:<11.1f}x")

    print(f"{'-' * 110}")

    avg_s = statistics.mean(mongo_times[q["name"]]["avg_ms"] / max(standalone_times[q["name"]]["avg_ms"], 0.001) for q in QUERIES)
    avg_d = statistics.mean(mongo_times[q["name"]]["avg_ms"] / max(distributed_times[q["name"]]["avg_ms"], 0.001) for q in QUERIES)

    print(f"\n  Avg speedup (Standalone CH vs MongoDB): {avg_s:.1f}x")
    print(f"  Avg speedup (Distributed CH vs MongoDB): {avg_d:.1f}x")

    # Distributed vs Standalone comparison
    print(f"\n{'Query':<35} {'Standalone (ms)':<17} {'Distributed (ms)':<18} {'Dist/Standalone':<15}")
    print(f"{'-' * 85}")
    for q in QUERIES:
        name = q["name"]
        s = standalone_times[name]["avg_ms"]
        d = distributed_times[name]["avg_ms"]
        ratio = d / max(s, 0.001)
        indicator = "faster" if ratio < 1 else "slower"
        print(f"{name:<35} {s:<17.1f} {d:<18.1f} {ratio:.2f}x ({indicator})")

    print()


def main():
    parser = argparse.ArgumentParser(description="Standalone vs Distributed ClickHouse benchmark")
    parser.add_argument("--records", type=int, default=500000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args()

    # Check connectivity
    print(f"\n{'=' * 60}")
    print(f"  Distributed vs Standalone ClickHouse Benchmark")
    print(f"  Records: {args.records:,}, Iterations: {args.iterations}")
    print(f"{'=' * 60}\n")

    # Check standalone
    try:
        ch_query(CH_STANDALONE_URL, "SELECT 1 FORMAT Null")
        print("[✓] Standalone ClickHouse (port 8123) — connected")
    except Exception as e:
        sys.exit(f"[✗] Standalone ClickHouse not reachable: {e}")

    # Check distributed
    try:
        ch_query(CH_DISTRIBUTED_URL, "SELECT 1 FORMAT Null")
        print("[✓] Distributed ClickHouse shard1 (port 8124) — connected")
    except Exception as e:
        sys.exit(f"[✗] Distributed ClickHouse not reachable at port 8124.\n"
                 f"    Start the cluster: docker compose -f benchmark/docker-compose-cluster.yml up -d\n"
                 f"    Error: {e}")

    # Check cluster
    try:
        result = ch_query(CH_DISTRIBUTED_URL, f"SELECT count() FROM system.clusters WHERE cluster = '{CH_CLUSTER_NAME}' FORMAT TabSeparated")
        shard_count = int(result.strip())
        print(f"[✓] Cluster '{CH_CLUSTER_NAME}' — {shard_count} nodes")
    except Exception as e:
        sys.exit(f"[✗] Cluster not configured: {e}")

    if not args.skip_load:
        print("\n[1/5] Generating data...")
        orders = generate_orders(args.records)
        print(f"      {len(orders):,} orders generated")

        print("[2/5] Loading MongoDB...")
        t0 = time.perf_counter()
        load_mongo(orders)
        print(f"      Done in {time.perf_counter() - t0:.1f}s")

        print("[3/5] Loading Standalone ClickHouse...")
        t0 = time.perf_counter()
        load_standalone(orders)
        print(f"      Done in {time.perf_counter() - t0:.1f}s")

        print("[4/5] Loading Distributed ClickHouse (3 shards)...")
        t0 = time.perf_counter()
        load_distributed(orders)
        print(f"      Done in {time.perf_counter() - t0:.1f}s")
    else:
        print("\n[1-4] Skipping data load (--skip-load)")

    print(f"[5/5] Running queries ({args.iterations} iterations)...")

    mongo_times = run_mongo_queries(args.iterations)
    standalone_times = run_queries("Standalone", CH_STANDALONE_URL, CH_STANDALONE_DB, args.iterations)
    distributed_times = run_queries("Distributed", CH_DISTRIBUTED_URL, CH_DISTRIBUTED_DB, args.iterations)

    print_results(mongo_times, standalone_times, distributed_times, args.records)

    # Save results
    output = {
        "timestamp": datetime.now().isoformat(),
        "record_count": args.records,
        "iterations": args.iterations,
        "mongo": {k: v for k, v in mongo_times.items()},
        "standalone_clickhouse": {k: v for k, v in standalone_times.items()},
        "distributed_clickhouse": {k: v for k, v in distributed_times.items()},
    }
    with open("benchmark/distributed_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print("  Results saved to benchmark/distributed_results.json")


if __name__ == "__main__":
    main()
