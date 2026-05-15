#!/usr/bin/env python3
"""
Top 20 MongoDB Aggregation Queries Benchmark

Tests the 20 most common/complex MongoDB aggregation patterns against both
MongoDB and ClickHouse to measure the performance difference.

Usage:
    python3 benchmark/aggregation_benchmark.py --records 500000
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
MONGO_DB = "agg_benchmark"
CH_URL = "http://localhost:8123"
CH_DB = "agg_benchmark"

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]
CATEGORIES = ["electronics", "clothing", "food", "books", "sports", "home", "toys", "auto"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]


def generate_data(n):
    base_date = datetime(2022, 1, 1)
    records = []
    for i in range(n):
        qty = random.randint(1, 20)
        price = round(random.uniform(5.0, 500.0), 2)
        records.append({
            "order_id": f"ORD-{i:08d}",
            "customer_id": f"CUST-{random.randint(1, n // 10):06d}",
            "amount": round(price * qty, 2),
            "quantity": qty,
            "unit_price": price,
            "discount": round(random.uniform(0, 0.3), 2),
            "tax": round(price * qty * 0.08, 2),
            "status": random.choice(STATUSES),
            "region": random.choice(REGIONS),
            "category": random.choice(CATEGORIES),
            "payment_method": random.choice(PAYMENT_METHODS),
            "is_prime": random.random() < 0.3,
            "rating": random.randint(1, 5),
            "created_at": (base_date + timedelta(
                seconds=random.randint(0, 730 * 24 * 3600)
            )).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


def ch_query(sql):
    resp = requests.post(CH_URL, data=sql)
    if resp.status_code != 200:
        raise RuntimeError(f"CH error: {resp.text.strip()[:200]}")
    return resp.text


def load_mongo(records):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db["orders"]
    coll.drop()
    coll.create_index("status")
    coll.create_index("region")
    coll.create_index("category")
    coll.create_index("customer_id")
    coll.create_index("created_at")
    coll.create_index("payment_method")
    coll.create_index([("region", 1), ("category", 1)])
    coll.create_index([("status", 1), ("created_at", 1)])

    for i in range(0, len(records), 10000):
        coll.insert_many(records[i:i + 10000])
    client.close()


def load_clickhouse(records):
    ch_query(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
    ch_query(f"DROP TABLE IF EXISTS {CH_DB}.orders")
    ch_query(f"""
        CREATE TABLE {CH_DB}.orders (
            order_id String,
            customer_id String,
            amount Float64,
            quantity UInt16,
            unit_price Float64,
            discount Float64,
            tax Float64,
            status LowCardinality(String),
            region LowCardinality(String),
            category LowCardinality(String),
            payment_method LowCardinality(String),
            is_prime UInt8,
            rating UInt8,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (region, category, created_at)
    """)

    batch_size = 50000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = "\n".join(
            json.dumps({k: v for k, v in r.items() if k != "_id"}) for r in batch
        )
        url = f"{CH_URL}/?query=INSERT+INTO+{CH_DB}.orders+FORMAT+JSONEachRow"
        resp = requests.post(url, data=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Insert error: {resp.text.strip()[:200]}")


# ============================================================
# 20 Aggregation Queries
# ============================================================

QUERIES = [
    {
        "name": "1. Count by status",
        "mongo": [{"$group": {"_id": "$status", "count": {"$sum": 1}}}],
        "ch": f"SELECT status, count() as count FROM {CH_DB}.orders GROUP BY status FORMAT Null",
    },
    {
        "name": "2. Revenue by region",
        "mongo": [{"$group": {"_id": "$region", "revenue": {"$sum": "$amount"}}}],
        "ch": f"SELECT region, sum(amount) as revenue FROM {CH_DB}.orders GROUP BY region FORMAT Null",
    },
    {
        "name": "3. Avg order value by category",
        "mongo": [{"$group": {"_id": "$category", "avg_order": {"$avg": "$amount"}}}],
        "ch": f"SELECT category, avg(amount) as avg_order FROM {CH_DB}.orders GROUP BY category FORMAT Null",
    },
    {
        "name": "4. Top 10 customers by spend",
        "mongo": [
            {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ],
        "ch": f"SELECT customer_id, sum(amount) as total FROM {CH_DB}.orders GROUP BY customer_id ORDER BY total DESC LIMIT 10 FORMAT Null",
    },
    {
        "name": "5. Monthly revenue trend",
        "mongo": [
            {"$group": {
                "_id": {"$substr": ["$created_at", 0, 7]},
                "revenue": {"$sum": "$amount"},
                "orders": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ],
        "ch": f"SELECT toYYYYMM(created_at) as month, sum(amount) as revenue, count() as orders FROM {CH_DB}.orders GROUP BY month ORDER BY month FORMAT Null",
    },
    {
        "name": "6. Revenue by region + category (2D)",
        "mongo": [
            {"$group": {"_id": {"region": "$region", "category": "$category"}, "revenue": {"$sum": "$amount"}}}
        ],
        "ch": f"SELECT region, category, sum(amount) as revenue FROM {CH_DB}.orders GROUP BY region, category FORMAT Null",
    },
    {
        "name": "7. Avg rating by category + status",
        "mongo": [
            {"$group": {"_id": {"category": "$category", "status": "$status"}, "avg_rating": {"$avg": "$rating"}}}
        ],
        "ch": f"SELECT category, status, avg(rating) as avg_rating FROM {CH_DB}.orders GROUP BY category, status FORMAT Null",
    },
    {
        "name": "8. Filter + aggregate (shipped, amount > 100)",
        "mongo": [
            {"$match": {"status": "shipped", "amount": {"$gt": 100}}},
            {"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}}
        ],
        "ch": f"SELECT region, sum(amount) as total, count() as count FROM {CH_DB}.orders WHERE status = 'shipped' AND amount > 100 GROUP BY region FORMAT Null",
    },
    {
        "name": "9. Date range + group (2023 Q1)",
        "mongo": [
            {"$match": {"created_at": {"$gte": "2023-01-01 00:00:00", "$lt": "2023-04-01 00:00:00"}}},
            {"$group": {"_id": "$category", "revenue": {"$sum": "$amount"}}}
        ],
        "ch": f"SELECT category, sum(amount) as revenue FROM {CH_DB}.orders WHERE created_at >= '2023-01-01' AND created_at < '2023-04-01' GROUP BY category FORMAT Null",
    },
    {
        "name": "10. Unique customers per region",
        "mongo": [
            {"$group": {"_id": "$region", "unique_customers": {"$addToSet": "$customer_id"}}}
        ],
        "ch": f"SELECT region, uniqExact(customer_id) as unique_customers FROM {CH_DB}.orders GROUP BY region FORMAT Null",
    },
    {
        "name": "11. Percentiles (p50, p90, p99 of amount)",
        "mongo": [
            {"$group": {"_id": None, "avg": {"$avg": "$amount"}, "min": {"$min": "$amount"}, "max": {"$max": "$amount"}}}
        ],
        "ch": f"SELECT quantile(0.5)(amount) as p50, quantile(0.9)(amount) as p90, quantile(0.99)(amount) as p99, avg(amount), min(amount), max(amount) FROM {CH_DB}.orders FORMAT Null",
    },
    {
        "name": "12. Payment method distribution",
        "mongo": [
            {"$group": {"_id": "$payment_method", "count": {"$sum": 1}, "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}}
        ],
        "ch": f"SELECT payment_method, count() as count, sum(amount) as total FROM {CH_DB}.orders GROUP BY payment_method ORDER BY total DESC FORMAT Null",
    },
    {
        "name": "13. Prime vs non-prime comparison",
        "mongo": [
            {"$group": {"_id": "$is_prime", "avg_amount": {"$avg": "$amount"}, "count": {"$sum": 1}}}
        ],
        "ch": f"SELECT is_prime, avg(amount) as avg_amount, count() as count FROM {CH_DB}.orders GROUP BY is_prime FORMAT Null",
    },
    {
        "name": "14. Top 5 categories by order count per region",
        "mongo": [
            {"$group": {"_id": {"region": "$region", "category": "$category"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 30}
        ],
        "ch": f"SELECT region, category, count() as count FROM {CH_DB}.orders GROUP BY region, category ORDER BY count DESC LIMIT 30 FORMAT Null",
    },
    {
        "name": "15. Discount impact analysis",
        "mongo": [
            {"$match": {"discount": {"$gt": 0}}},
            {"$group": {"_id": "$category", "avg_discount": {"$avg": "$discount"}, "total_saved": {"$sum": {"$multiply": ["$amount", "$discount"]}}}}
        ],
        "ch": f"SELECT category, avg(discount) as avg_discount, sum(amount * discount) as total_saved FROM {CH_DB}.orders WHERE discount > 0 GROUP BY category FORMAT Null",
    },
    {
        "name": "16. Hourly order distribution",
        "mongo": [
            {"$group": {"_id": {"$substr": ["$created_at", 11, 2]}, "count": {"$sum": 1}}}
        ],
        "ch": f"SELECT toHour(created_at) as hour, count() as count FROM {CH_DB}.orders GROUP BY hour ORDER BY hour FORMAT Null",
    },
    {
        "name": "17. Customer lifetime value (top 20)",
        "mongo": [
            {"$group": {"_id": "$customer_id", "total_spend": {"$sum": "$amount"}, "order_count": {"$sum": 1}, "avg_order": {"$avg": "$amount"}}},
            {"$sort": {"total_spend": -1}},
            {"$limit": 20}
        ],
        "ch": f"SELECT customer_id, sum(amount) as total_spend, count() as order_count, avg(amount) as avg_order FROM {CH_DB}.orders GROUP BY customer_id ORDER BY total_spend DESC LIMIT 20 FORMAT Null",
    },
    {
        "name": "18. Multi-match pipeline (region + status + date)",
        "mongo": [
            {"$match": {"region": {"$in": ["us-east", "eu-west"]}, "status": "delivered"}},
            {"$match": {"created_at": {"$gte": "2023-01-01 00:00:00"}}},
            {"$group": {"_id": "$category", "revenue": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"revenue": -1}}
        ],
        "ch": f"SELECT category, sum(amount) as revenue, count() as count FROM {CH_DB}.orders WHERE region IN ('us-east', 'eu-west') AND status = 'delivered' AND created_at >= '2023-01-01' GROUP BY category ORDER BY revenue DESC FORMAT Null",
    },
    {
        "name": "19. Revenue with tax calculation",
        "mongo": [
            {"$group": {"_id": "$region", "gross": {"$sum": "$amount"}, "tax_total": {"$sum": "$tax"}, "net": {"$sum": {"$subtract": ["$amount", "$tax"]}}}}
        ],
        "ch": f"SELECT region, sum(amount) as gross, sum(tax) as tax_total, sum(amount - tax) as net FROM {CH_DB}.orders GROUP BY region FORMAT Null",
    },
    {
        "name": "20. Full analytics dashboard query",
        "mongo": [
            {"$group": {
                "_id": "$region",
                "total_revenue": {"$sum": "$amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$amount"},
                "max_order": {"$max": "$amount"},
                "min_order": {"$min": "$amount"},
            }},
            {"$sort": {"total_revenue": -1}}
        ],
        "ch": f"SELECT region, sum(amount) as total_revenue, count() as order_count, avg(amount) as avg_order_value, max(amount) as max_order, min(amount) as min_order FROM {CH_DB}.orders GROUP BY region ORDER BY total_revenue DESC FORMAT Null",
    },
]


def run_benchmark(iterations):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db["orders"]

    results = []

    for q in QUERIES:
        # Warmup
        try:
            list(coll.aggregate(q["mongo"]))
        except Exception:
            pass
        try:
            ch_query(q["ch"])
        except Exception:
            pass

        mongo_times = []
        ch_times = []

        for _ in range(iterations):
            start = time.perf_counter()
            try:
                list(coll.aggregate(q["mongo"]))
                mongo_times.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                mongo_times.append(float("inf"))

            start = time.perf_counter()
            try:
                ch_query(q["ch"])
                ch_times.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                ch_times.append(float("inf"))

        m_avg = statistics.mean(mongo_times)
        c_avg = statistics.mean(ch_times)
        results.append({
            "query": q["name"],
            "mongo_avg_ms": m_avg,
            "ch_avg_ms": c_avg,
            "speedup": m_avg / max(c_avg, 0.001),
        })

    client.close()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=500000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args()

    print(f"\n{'=' * 70}")
    print(f"  Top 20 MongoDB Aggregation Benchmark")
    print(f"  Records: {args.records:,} | Iterations: {args.iterations}")
    print(f"{'=' * 70}\n")

    if not args.skip_load:
        print("[1/3] Generating data...")
        data = generate_data(args.records)

        print("[2/3] Loading MongoDB...")
        t0 = time.perf_counter()
        load_mongo(data)
        print(f"      Done in {time.perf_counter() - t0:.1f}s")

        print("[3/3] Loading ClickHouse...")
        t0 = time.perf_counter()
        load_clickhouse(data)
        print(f"      Done in {time.perf_counter() - t0:.1f}s")
    else:
        print("[1-3] Skipping load (--skip-load)")

    print(f"\nRunning {len(QUERIES)} queries x {args.iterations} iterations...\n")
    results = run_benchmark(args.iterations)

    # Print results
    print(f"{'Query':<50} {'MongoDB (ms)':<14} {'CH (ms)':<10} {'Speedup':<10}")
    print("-" * 84)
    for r in results:
        print(f"{r['query']:<50} {r['mongo_avg_ms']:<14.1f} {r['ch_avg_ms']:<10.1f} {r['speedup']:<10.1f}x")

    print("-" * 84)
    avg_speedup = statistics.mean(r["speedup"] for r in results)
    max_r = max(results, key=lambda x: x["speedup"])
    print(f"\n  Average speedup: {avg_speedup:.1f}x")
    print(f"  Max speedup: {max_r['speedup']:.1f}x on '{max_r['query']}'")

    # Save
    with open("benchmark/aggregation_results.json", "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "records": args.records, "results": results}, f, indent=2)
    print(f"\n  Saved to benchmark/aggregation_results.json")


if __name__ == "__main__":
    main()
