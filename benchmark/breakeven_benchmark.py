#!/usr/bin/env python3
"""
Break-Even Benchmark: MongoDB vs MongoDB+Index vs mg-clickhouse

Tests complex aggregation queries at increasing data sizes to find the
break-even point where mg-clickhouse becomes faster than MongoDB.

Compares three scenarios:
  1. MongoDB (no index) — raw collection scan
  2. MongoDB (with index) — compound index on GROUP BY fields
  3. mg-clickhouse — columnar analytics via real-time CDC sync

Data sizes: 100, 500, 1K, 5K, 10K, 50K, 100K, 500K, 1M, 5M

Output: JSON results + ASCII chart showing the crossover point.

Usage:
    python3 benchmark/breakeven_benchmark.py
    python3 benchmark/breakeven_benchmark.py --max-size 100000
"""

import argparse
import json
import logging
import os
import random
import statistics
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

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
# Configuration
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.environ.get("MONGO_DB", "breakeven_bench")
CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_DB = os.environ.get("CH_DB", "breakeven_bench")
COLLECTION = "orders"
CH_TABLE = "orders"

DATA_SIZES = [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 2_000_000, 5_000_000]
ITERATIONS = 3

SESSION = requests.Session()
SESSION.mount("http://", HTTPAdapter(pool_connections=5, pool_maxsize=5))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ============================================================
# Data Generation
# ============================================================

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]
CATEGORIES = ["electronics", "clothing", "food", "furniture", "books", "sports", "toys", "health"]
CUSTOMERS = [f"customer_{i:04d}" for i in range(500)]


def generate_batch(size: int, offset: int = 0) -> List[Dict[str, Any]]:
    """Generate a batch of order documents."""
    base_date = datetime(2023, 1, 1)
    docs = []
    for i in range(size):
        docs.append({
            "order_id": f"ORD-{offset + i:08d}",
            "customer_id": random.choice(CUSTOMERS),
            "amount": round(random.uniform(5.0, 2000.0), 2),
            "status": random.choice(STATUSES),
            "region": random.choice(REGIONS),
            "category": random.choice(CATEGORIES),
            "quantity": random.randint(1, 50),
            "discount": round(random.uniform(0, 0.3), 2),
            "created_at": base_date + timedelta(seconds=random.randint(0, 63_072_000)),
        })
    return docs


# ============================================================
# ClickHouse Helpers
# ============================================================


def ch_query(sql: str) -> str:
    """Execute ClickHouse query."""
    resp = SESSION.post(CH_URL, data=sql, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"CH error: {resp.text.strip()[:200]}")
    return resp.text


def ch_setup_table() -> None:
    """Create ClickHouse database and table."""
    ch_query(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
    ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
    ch_query(f"""
        CREATE TABLE {CH_DB}.{CH_TABLE} (
            order_id String,
            customer_id LowCardinality(String),
            amount Float64,
            status LowCardinality(String),
            region LowCardinality(String),
            category LowCardinality(String),
            quantity Int32,
            discount Float64,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (region, status, created_at)
    """)


def ch_load_data(docs: List[Dict[str, Any]]) -> None:
    """Load documents into ClickHouse in batches."""
    batch_size = 10_000
    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        rows = []
        for d in batch:
            rows.append(json.dumps({
                "order_id": d["order_id"],
                "customer_id": d["customer_id"],
                "amount": d["amount"],
                "status": d["status"],
                "region": d["region"],
                "category": d["category"],
                "quantity": d["quantity"],
                "discount": d["discount"],
                "created_at": d["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
            }))
        payload = "\n".join(rows)
        url = f"{CH_URL}/?query=INSERT+INTO+{CH_DB}.{CH_TABLE}+FORMAT+JSONEachRow"
        resp = SESSION.post(url, data=payload.encode("utf-8"), timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"CH insert error: {resp.text.strip()[:200]}")


# ============================================================
# Aggregation Queries
# ============================================================

QUERIES = [
    {
        "name": "Revenue by region + status (2D GROUP BY)",
        "mongo": [
            {"$group": {"_id": {"region": "$region", "status": "$status"}, "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total": -1}},
        ],
        "ch": f"SELECT region, status, sum(amount) as total, count() as cnt FROM {CH_DB}.{CH_TABLE} GROUP BY region, status ORDER BY total DESC",
    },
    {
        "name": "Top 20 customers by spend",
        "mongo": [
            {"$group": {"_id": "$customer_id", "total_spend": {"$sum": "$amount"}, "order_count": {"$sum": 1}}},
            {"$sort": {"total_spend": -1}},
            {"$limit": 20},
        ],
        "ch": f"SELECT customer_id, sum(amount) as total_spend, count() as order_count FROM {CH_DB}.{CH_TABLE} GROUP BY customer_id ORDER BY total_spend DESC LIMIT 20",
    },
    {
        "name": "Avg order value by category + region (3D)",
        "mongo": [
            {"$group": {"_id": {"cat": "$category", "reg": "$region"}, "avg_amount": {"$avg": "$amount"}, "avg_qty": {"$avg": "$quantity"}}},
            {"$sort": {"avg_amount": -1}},
        ],
        "ch": f"SELECT category, region, avg(amount) as avg_amount, avg(quantity) as avg_qty FROM {CH_DB}.{CH_TABLE} GROUP BY category, region ORDER BY avg_amount DESC",
    },
    {
        "name": "Monthly revenue trend (date bucketing)",
        "mongo": [
            {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$created_at"}}, "revenue": {"$sum": "$amount"}, "orders": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
            {"$limit": 24},
        ],
        "ch": f"SELECT toYYYYMM(created_at) as month, sum(amount) as revenue, count() as orders FROM {CH_DB}.{CH_TABLE} GROUP BY month ORDER BY month DESC LIMIT 24",
    },
    {
        "name": "Conditional: delivery rate by region",
        "mongo": [
            {"$group": {"_id": "$region", "total": {"$sum": 1}, "delivered": {"$sum": {"$cond": [{"$eq": ["$status", "delivered"]}, 1, 0]}}}},
            {"$project": {"region": "$_id", "total": 1, "delivered": 1, "rate": {"$divide": ["$delivered", "$total"]}}},
        ],
        "ch": f"SELECT region, count() as total, countIf(status='delivered') as delivered, delivered/total as rate FROM {CH_DB}.{CH_TABLE} GROUP BY region",
    },
]


# ============================================================
# Benchmark Runner
# ============================================================


def benchmark_mongo(coll: pymongo.collection.Collection, queries: List[Dict], iterations: int) -> List[float]:
    """Run queries against MongoDB, return avg latency per query in ms."""
    times = []
    for q in queries:
        q_times = []
        # Warmup
        try:
            list(coll.aggregate(q["mongo"], allowDiskUse=True))
        except Exception:
            pass
        for _ in range(iterations):
            start = time.perf_counter()
            try:
                list(coll.aggregate(q["mongo"], allowDiskUse=True))
                q_times.append((time.perf_counter() - start) * 1000)
            except Exception:
                q_times.append(float("inf"))
        times.append(statistics.mean(q_times))
    return times


def benchmark_clickhouse(queries: List[Dict], iterations: int) -> List[float]:
    """Run queries against ClickHouse, return avg latency per query in ms."""
    times = []
    for q in queries:
        q_times = []
        # Warmup
        try:
            ch_query(q["ch"])
        except Exception:
            pass
        for _ in range(iterations):
            start = time.perf_counter()
            try:
                ch_query(q["ch"])
                q_times.append((time.perf_counter() - start) * 1000)
            except Exception:
                q_times.append(float("inf"))
        times.append(statistics.mean(q_times))
    return times


# ============================================================
# Chart Rendering
# ============================================================


def render_chart(results: List[Dict]) -> None:
    """Render ASCII chart of results."""
    print(f"\n{'='*90}")
    print("  BREAK-EVEN CHART: Avg Query Latency (ms) by Data Size")
    print(f"{'='*90}\n")
    print(f"  {'Size':<12} {'MongoDB':<12} {'Mongo+Idx':<12} {'mg-clickhouse':<14} {'Speedup':<12} {'Winner'}")
    print(f"  {'-'*74}")

    for r in results:
        size_str = f"{r['size']:,}"
        mongo_str = f"{r['mongo_avg']:.1f}" if r['mongo_avg'] != float("inf") else "ERR"
        idx_str = f"{r['mongo_idx_avg']:.1f}" if r['mongo_idx_avg'] != float("inf") else "ERR"
        ch_str = f"{r['ch_avg']:.1f}" if r['ch_avg'] != float("inf") else "ERR"

        best = min(r['mongo_avg'], r['mongo_idx_avg'], r['ch_avg'])
        if best == r['ch_avg']:
            winner = "mg-clickhouse ⚡"
            speedup = r['mongo_avg'] / max(r['ch_avg'], 0.01)
        elif best == r['mongo_idx_avg']:
            winner = "Mongo+Index"
            speedup = r['mongo_avg'] / max(r['mongo_idx_avg'], 0.01)
        else:
            winner = "MongoDB"
            speedup = 1.0

        print(f"  {size_str:<12} {mongo_str:<12} {idx_str:<12} {ch_str:<14} {speedup:<12.1f}x {winner}")

    print(f"  {'-'*74}")

    # Find break-even point
    for r in results:
        if r['ch_avg'] < r['mongo_avg'] and r['ch_avg'] < r['mongo_idx_avg']:
            print(f"\n  ⚡ Break-even point: ~{r['size']:,} documents")
            print(f"     At this size, mg-clickhouse becomes faster than MongoDB (with or without indexes)")
            break
    else:
        print(f"\n  ℹ  mg-clickhouse did not surpass MongoDB in the tested range.")
        print(f"     Try larger data sizes with --max-size")

    # Bar chart
    print(f"\n  Latency comparison (log scale, shorter = faster):")
    print(f"  {'Size':<10} MongoDB          Mongo+Idx        mg-clickhouse")
    for r in results:
        size_str = f"{r['size']:,}"
        m_bar = "█" * min(int(r['mongo_avg'] / 5), 40) if r['mongo_avg'] < 10000 else "█" * 40 + "→"
        i_bar = "▓" * min(int(r['mongo_idx_avg'] / 5), 40) if r['mongo_idx_avg'] < 10000 else "▓" * 40 + "→"
        c_bar = "░" * min(int(r['ch_avg'] / 5), 40) if r['ch_avg'] < 10000 else "░" * 40 + "→"
        print(f"  {size_str:<10} {m_bar}")
        print(f"  {'':10} {i_bar}")
        print(f"  {'':10} {c_bar}")
        print()


# ============================================================
# Main
# ============================================================


def main() -> None:
    parser = argparse.ArgumentParser(description="Break-even benchmark: MongoDB vs mg-clickhouse")
    parser.add_argument("--max-size", type=int, default=100_000, help="Max data size to test (default: 100000)")
    parser.add_argument("--iterations", type=int, default=3, help="Query iterations per size")
    args = parser.parse_args()

    sizes = [s for s in DATA_SIZES if s <= args.max_size]
    if not sizes:
        sys.exit(f"ERROR: --max-size must be >= {DATA_SIZES[0]}")

    print(f"\n{'='*90}")
    print(f"  Break-Even Benchmark: MongoDB vs MongoDB+Index vs mg-clickhouse")
    print(f"  Data sizes: {', '.join(f'{s:,}' for s in sizes)}")
    print(f"  Queries: {len(QUERIES)} complex aggregations × {args.iterations} iterations")
    print(f"{'='*90}\n")

    # Preflight
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        sys.exit(f"ERROR: MongoDB not reachable: {e}")

    try:
        ch_query("SELECT 1")
    except Exception as e:
        sys.exit(f"ERROR: mg-clickhouse (ClickHouse) not reachable: {e}")

    db = client[MONGO_DB]
    results: List[Dict[str, Any]] = []

    for size in sizes:
        logger.info(f"Testing size: {size:,} documents...")

        # Generate data
        docs = generate_batch(size)

        # --- MongoDB (no index) ---
        coll = db[COLLECTION]
        coll.drop()
        # Insert in batches
        batch_size = min(10_000, size)
        for i in range(0, size, batch_size):
            coll.insert_many(docs[i:i + batch_size])

        mongo_times = benchmark_mongo(coll, QUERIES, args.iterations)
        mongo_avg = statistics.mean(mongo_times)

        # --- MongoDB (with compound index) ---
        coll.create_index([("region", 1), ("status", 1), ("created_at", -1)])
        coll.create_index([("customer_id", 1), ("amount", -1)])
        coll.create_index([("category", 1), ("region", 1)])

        mongo_idx_times = benchmark_mongo(coll, QUERIES, args.iterations)
        mongo_idx_avg = statistics.mean(mongo_idx_times)

        # --- mg-clickhouse ---
        ch_setup_table()
        ch_load_data(docs)

        ch_times = benchmark_clickhouse(QUERIES, args.iterations)
        ch_avg = statistics.mean(ch_times)

        results.append({
            "size": size,
            "mongo_avg": mongo_avg,
            "mongo_idx_avg": mongo_idx_avg,
            "ch_avg": ch_avg,
            "mongo_per_query": mongo_times,
            "mongo_idx_per_query": mongo_idx_times,
            "ch_per_query": ch_times,
        })

        logger.info(f"  Mongo: {mongo_avg:.1f}ms | Mongo+Idx: {mongo_idx_avg:.1f}ms | CH: {ch_avg:.1f}ms")

    # Render results
    render_chart(results)

    # Save JSON
    output = {
        "timestamp": datetime.now().isoformat(),
        "config": {"mongo_uri": MONGO_URI, "ch_url": CH_URL, "iterations": args.iterations},
        "queries": [q["name"] for q in QUERIES],
        "results": [{k: v for k, v in r.items() if k != "mongo_per_query" and k != "mongo_idx_per_query" and k != "ch_per_query"}
                    for r in results],
        "detailed_results": results,
    }
    output_file = "benchmark/breakeven_results.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  Results saved to {output_file}")

    # Cleanup
    db.drop_collection(COLLECTION)
    ch_query(f"DROP TABLE IF EXISTS {CH_DB}.{CH_TABLE}")
    ch_query(f"DROP DATABASE IF EXISTS {CH_DB}")
    client.close()


if __name__ == "__main__":
    main()
