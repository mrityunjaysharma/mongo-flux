package main

import (
	"context"
	"log"
	"time"
)

func runWriteBenchmark() {
	log.Printf("=== Write Benchmark: %d records ===", *records)

	client, err := connectMongo()
	if err != nil {
		log.Fatalf("MongoDB connect: %v", err)
	}
	defer client.Disconnect(context.Background())

	ctx := context.Background()
	coll := client.Database(*mongoDB).Collection("bench_write")
	coll.Drop(ctx)

	docs := generateDocs(*records)
	batch := 1000
	start := time.Now()
	for i := 0; i < len(docs); i += batch {
		end := min(i+batch, len(docs))
		coll.InsertMany(ctx, docs[i:end])
	}
	batchDuration := time.Since(start)
	batchRate := float64(*records) / batchDuration.Seconds()
	log.Printf("  Batch insert: %d docs/s (%.1fs)", int(batchRate), batchDuration.Seconds())

	singleCount := min(*records, 2000)
	singleDocs := generateDocs(singleCount)
	coll.Drop(ctx)

	start = time.Now()
	for _, d := range singleDocs {
		coll.InsertOne(ctx, d)
	}
	singleDuration := time.Since(start)
	singleRate := float64(singleCount) / singleDuration.Seconds()
	avgLatency := float64(singleDuration.Milliseconds()) / float64(singleCount)
	log.Printf("  Single insert: %d docs/s, avg %.2fms", int(singleRate), avgLatency)

	writeOutput(map[string]any{
		"batch_rate_docs_s":     int(batchRate),
		"batch_duration_s":      round1(batchDuration.Seconds()),
		"single_rate_docs_s":    int(singleRate),
		"single_avg_latency_ms": round1(avgLatency),
		"records":               *records,
	})
}
