#!/usr/bin/env python3
"""
Write Benchmark: Standalone MongoDB vs MongoFlux Architecture

Measures the write overhead introduced by MongoFlux's oplog tailing.
Since MongoFlux tails the oplog asynchronously (like a secondary),
writes to MongoDB should have near-zero overhead — this benchmark verifies that.

Compares:
  1. Standalone MongoDB inserts (no MongoFlux running)
  2. MongoDB inserts with MongoFlux actively tailing the oplog

Usage:
    python3 benchmark/write_benchmark.py [--records 100000] [--batch-size 1000]
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
MONGO_DB = "write_benchmark"
MONGOFLUX_API = "http://localhost:9090"

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]


def generate_order_batch(batch_size, offset=0):
    """Generate a batch of order documents."""
    base_date = datetime(2023, 1, 1)
    records = []
    for i in range(batch_size):
        records.append({
            "order_id": f"ORD-{offset + i:08d}",
            "customer_id": f"CUST-{random.randint(1, 100000):06d}",
            "amount": round(random.uniform(5.0, 5000.0), 2),
            "status": random.choice(STATUSES),
            "region": random.choice(REGIONS),
            "created_at": (base_date + timedelta(
                seconds=random.randint(0, 365 * 24 * 3600)
            )).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


def check_mongoflux_running():
    """Check if MongoFlux is running and reachable."""
    try:
        resp = requests.get(f"{MONGOFLUX_API}/api/v1/status", timeout=2)
        return resp.status_code == 200
    except Exception:
        return False


def setup_mapping():
    """Ensure MongoFlux has a mapping for the benchmark collection."""
    mapping = {
        "collection": "orders",
        "clickhouse_table": "write_bench_orders",
        "clickhouse_database": "benchmark",
        "fields": [
            {"mongo_field": "order_id", "ch_column": "order_id", "ch_type": "String"},
            {"mongo_field": "customer_id", "ch_column": "customer_id", "ch_type": "String"},
            {"mongo_field": "amount", "ch_column": "amount", "ch_type": "Float64"},
            {"mongo_field": "status", "ch_column": "status", "ch_type": "String"},
            {"mongo_field": "region", "ch_column": "region", "ch_type": "String"},
            {"mongo_field": "created_at", "ch_column": "created_at", "ch_type": "String"},
        ],
        "engine": "MergeTree",
        "order_by": ["order_id"],
    }
    try:
        resp = requests.post(
            f"{MONGOFLUX_API}/api/v1/mappings",
            json=mapping,
            timeout=5
        )
        if resp.status_code in (200, 201):
            # Sync table
            requests.post(
                f"{MONGOFLUX_API}/api/v1/mappings/orders/sync",
                timeout=10
            )
    except Exception:
        pass


def benchmark_inserts(total_records, batch_size, collection_name):
    """
    Benchmark insert performance.
    Returns dict with timing metrics.
    """
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[collection_name]
    coll.drop()

    batch_times = []
    total_start = time.perf_counter()

    for offset in range(0, total_records, batch_size):
        current_batch_size = min(batch_size, total_records - offset)
        batch = generate_order_batch(current_batch_size, offset)

        start = time.perf_counter()
        coll.insert_many(batch, ordered=False)
        elapsed_ms = (time.perf_counter() - start) * 1000
        batch_times.append(elapsed_ms)

    total_elapsed = (time.perf_counter() - total_start) * 1000
    client.close()

    return {
        "total_ms": total_elapsed,
        "total_records": total_records,
        "batch_size": batch_size,
        "num_batches": len(batch_times),
        "avg_batch_ms": statistics.mean(batch_times),
        "p50_batch_ms": statistics.median(batch_times),
        "p95_batch_ms": sorted(batch_times)[int(len(batch_times) * 0.95)] if len(batch_times) > 1 else batch_times[0],
        "min_batch_ms": min(batch_times),
        "max_batch_ms": max(batch_times),
        "throughput_docs_per_sec": total_records / (total_elapsed / 1000),
    }


def benchmark_single_inserts(num_inserts, collection_name):
    """
    Benchmark single-document insert latency.
    Returns dict with timing metrics.
    """
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[collection_name]
    coll.drop()

    insert_times = []
    base_date = datetime(2023, 1, 1)

    for i in range(num_inserts):
        doc = {
            "order_id": f"SINGLE-{i:08d}",
            "customer_id": f"CUST-{random.randint(1, 100000):06d}",
            "amount": round(random.uniform(5.0, 5000.0), 2),
            "status": random.choice(STATUSES),
            "region": random.choice(REGIONS),
            "created_at": (base_date + timedelta(
                seconds=random.randint(0, 365 * 24 * 3600)
            )).strftime("%Y-%m-%d %H:%M:%S"),
        }

        start = time.perf_counter()
        coll.insert_one(doc)
        elapsed_ms = (time.perf_counter() - start) * 1000
        insert_times.append(elapsed_ms)

    client.close()

    return {
        "num_inserts": num_inserts,
        "avg_ms": statistics.mean(insert_times),
        "p50_ms": statistics.median(insert_times),
        "p95_ms": sorted(insert_times)[int(len(insert_times) * 0.95)],
        "p99_ms": sorted(insert_times)[int(len(insert_times) * 0.99)],
        "min_ms": min(insert_times),
        "max_ms": max(insert_times),
        "throughput_docs_per_sec": num_inserts / (sum(insert_times) / 1000),
    }


def print_results(standalone_batch, with_sync_batch, standalone_single, with_sync_single, total_records):
    """Print formatted benchmark results."""
    print("\n" + "=" * 90)
    print(f"  WRITE BENCHMARK RESULTS — {total_records:,} records")
    print("=" * 90)

    # Batch insert comparison
    print(f"\n{'─' * 90}")
    print(f"  BATCH INSERTS (batch_size={standalone_batch['batch_size']:,})")
    print(f"{'─' * 90}")
    print(f"\n{'Metric':<35} {'Standalone MongoDB':<25} {'With MongoFlux':<25} {'Overhead':<15}")
    print(f"{'-' * 90}")

    overhead_total = ((with_sync_batch["total_ms"] - standalone_batch["total_ms"]) / standalone_batch["total_ms"]) * 100
    overhead_avg = ((with_sync_batch["avg_batch_ms"] - standalone_batch["avg_batch_ms"]) / standalone_batch["avg_batch_ms"]) * 100

    print(f"{'Total time':<35} {standalone_batch['total_ms']:.0f} ms{'':<17} {with_sync_batch['total_ms']:.0f} ms{'':<17} {overhead_total:+.1f}%")
    print(f"{'Avg batch latency':<35} {standalone_batch['avg_batch_ms']:.1f} ms{'':<17} {with_sync_batch['avg_batch_ms']:.1f} ms{'':<17} {overhead_avg:+.1f}%")
    print(f"{'P50 batch latency':<35} {standalone_batch['p50_batch_ms']:.1f} ms{'':<17} {with_sync_batch['p50_batch_ms']:.1f} ms")
    print(f"{'P95 batch latency':<35} {standalone_batch['p95_batch_ms']:.1f} ms{'':<17} {with_sync_batch['p95_batch_ms']:.1f} ms")
    print(f"{'Max batch latency':<35} {standalone_batch['max_batch_ms']:.1f} ms{'':<17} {with_sync_batch['max_batch_ms']:.1f} ms")
    print(f"{'Throughput':<35} {standalone_batch['throughput_docs_per_sec']:.0f} docs/s{'':<11} {with_sync_batch['throughput_docs_per_sec']:.0f} docs/s")

    # Single insert comparison
    print(f"\n{'─' * 90}")
    print(f"  SINGLE-DOCUMENT INSERTS ({standalone_single['num_inserts']:,} operations)")
    print(f"{'─' * 90}")
    print(f"\n{'Metric':<35} {'Standalone MongoDB':<25} {'With MongoFlux':<25} {'Overhead':<15}")
    print(f"{'-' * 90}")

    overhead_single_avg = ((with_sync_single["avg_ms"] - standalone_single["avg_ms"]) / standalone_single["avg_ms"]) * 100
    overhead_single_p50 = ((with_sync_single["p50_ms"] - standalone_single["p50_ms"]) / standalone_single["p50_ms"]) * 100

    print(f"{'Avg latency':<35} {standalone_single['avg_ms']:.2f} ms{'':<17} {with_sync_single['avg_ms']:.2f} ms{'':<17} {overhead_single_avg:+.1f}%")
    print(f"{'P50 latency':<35} {standalone_single['p50_ms']:.2f} ms{'':<17} {with_sync_single['p50_ms']:.2f} ms{'':<17} {overhead_single_p50:+.1f}%")
    print(f"{'P95 latency':<35} {standalone_single['p95_ms']:.2f} ms{'':<17} {with_sync_single['p95_ms']:.2f} ms")
    print(f"{'P99 latency':<35} {standalone_single['p99_ms']:.2f} ms{'':<17} {with_sync_single['p99_ms']:.2f} ms")
    print(f"{'Max latency':<35} {standalone_single['max_ms']:.2f} ms{'':<17} {with_sync_single['max_ms']:.2f} ms")
    print(f"{'Throughput':<35} {standalone_single['throughput_docs_per_sec']:.0f} docs/s{'':<11} {with_sync_single['throughput_docs_per_sec']:.0f} docs/s")

    # Summary
    print(f"\n{'─' * 90}")
    print(f"  SUMMARY")
    print(f"{'─' * 90}")
    print(f"\n  Batch insert overhead:          {overhead_total:+.1f}% total time, {overhead_avg:+.1f}% avg latency")
    print(f"  Single insert overhead:         {overhead_single_avg:+.1f}% avg latency, {overhead_single_p50:+.1f}% p50 latency")
    print(f"\n  MongoFlux tails the oplog asynchronously (like a MongoDB secondary).")
    print(f"  Write overhead is expected to be near-zero since the sync is decoupled from")
    print(f"  the write path — MongoDB acknowledges writes before MongoFlux processes them.")
    print()


def main():
    parser = argparse.ArgumentParser(description="MongoDB write benchmark: standalone vs MongoFlux")
    parser.add_argument("--records", type=int, default=100000,
                        help="Total records for batch insert test (default: 100000)")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="Batch size for bulk inserts (default: 1000)")
    parser.add_argument("--single-inserts", type=int, default=2000,
                        help="Number of single-document inserts (default: 2000)")
    args = parser.parse_args()

    mg_running = check_mongoflux_running()

    print(f"\n{'=' * 60}")
    print(f"  MongoFlux Write Benchmark")
    print(f"  Total records (batch test): {args.records:,}")
    print(f"  Batch size: {args.batch_size:,}")
    print(f"  Single inserts: {args.single_inserts:,}")
    print(f"  MongoFlux status: {'RUNNING' if mg_running else 'NOT RUNNING'}")
    print(f"{'=' * 60}\n")

    if mg_running:
        setup_mapping()

    # Phase 1: Benchmark WITHOUT MongoFlux sync (use a different collection)
    # Even with MongoFlux running, if the collection has no mapping, it won't sync
    print("[1/4] Batch inserts — standalone (no sync mapping)...")
    standalone_batch = benchmark_inserts(args.records, args.batch_size, "bench_standalone_orders")
    print(f"      {standalone_batch['throughput_docs_per_sec']:.0f} docs/s, "
          f"{standalone_batch['total_ms']:.0f} ms total")

    # Phase 2: Benchmark WITH MongoFlux sync (use the mapped collection)
    print("[2/4] Batch inserts — with MongoFlux sync...")
    if mg_running:
        with_sync_batch = benchmark_inserts(args.records, args.batch_size, "orders")
    else:
        print("      (MongoFlux not running, using same collection)")
        with_sync_batch = benchmark_inserts(args.records, args.batch_size, "orders")
    print(f"      {with_sync_batch['throughput_docs_per_sec']:.0f} docs/s, "
          f"{with_sync_batch['total_ms']:.0f} ms total")

    # Phase 3: Single inserts — standalone
    print(f"[3/4] Single inserts — standalone ({args.single_inserts:,} ops)...")
    standalone_single = benchmark_single_inserts(args.single_inserts, "bench_standalone_single")
    print(f"      {standalone_single['throughput_docs_per_sec']:.0f} docs/s, "
          f"avg {standalone_single['avg_ms']:.2f} ms")

    # Phase 4: Single inserts — with sync
    print(f"[4/4] Single inserts — with MongoFlux sync ({args.single_inserts:,} ops)...")
    with_sync_single = benchmark_single_inserts(args.single_inserts, "orders")
    print(f"      {with_sync_single['throughput_docs_per_sec']:.0f} docs/s, "
          f"avg {with_sync_single['avg_ms']:.2f} ms")

    # Print results
    print_results(standalone_batch, with_sync_batch, standalone_single, with_sync_single, args.records)

    # Save results
    output = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "total_records": args.records,
            "batch_size": args.batch_size,
            "single_inserts": args.single_inserts,
            "mongoflux_running": mg_running,
        },
        "batch_inserts": {
            "standalone": standalone_batch,
            "with_mongoflux": with_sync_batch,
        },
        "single_inserts": {
            "standalone": standalone_single,
            "with_mongoflux": with_sync_single,
        },
    }

    output_file = "benchmark/write_results.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Results saved to {output_file}")


if __name__ == "__main__":
    main()
