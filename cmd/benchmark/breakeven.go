package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func runBreakevenBenchmark() {
	sizes := []int{100, 500, 1000, 5000, 10000, 50000, 100000}
	for _, s := range []int{500000, 1000000, 2000000} {
		if s <= *maxSize {
			sizes = append(sizes, s)
		}
	}

	log.Printf("=== Breakeven Benchmark: sizes up to %d ===", *maxSize)

	client, err := connectMongo()
	if err != nil {
		log.Fatalf("MongoDB connect: %v", err)
	}
	defer client.Disconnect(context.Background())

	ctx := context.Background()
	coll := client.Database(*mongoDB).Collection("bench_breakeven")
	table := "bench_breakeven"

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}},
	}
	chSQL := fmt.Sprintf("SELECT status, sum(amount) as total FROM %s.%s GROUP BY status ORDER BY total DESC", *chDB, table)

	type SizeResult struct {
		Size    int     `json:"size"`
		MongoMs float64 `json:"mongo_ms"`
		CHMs    float64 `json:"ch_ms"`
		Speedup float64 `json:"speedup"`
		Winner  string  `json:"winner"`
	}
	var results []SizeResult

	for _, size := range sizes {
		docs := generateDocs(size)
		loadMongoDB(coll, docs)
		loadClickHouse(table, docs)

		var mongoTimes, chTimes []float64
		for i := 0; i < *iterations+1; i++ {
			mongoTimes = append(mongoTimes, timeMongoAgg(coll, pipeline))
			chTimes = append(chTimes, timeCHQuery(chSQL))
		}

		mAvg := avgExcludeFirst(mongoTimes)
		cAvg := avgExcludeFirst(chTimes)
		speedup := mAvg / fmax(cAvg, 0.01)
		winner := "MongoDB"
		if speedup > 1.0 {
			winner = "MongoFlux"
		}

		results = append(results, SizeResult{size, round1(mAvg), round1(cAvg), round1(speedup), winner})
		log.Printf("  %8d docs: Mongo %6.1fms  CH %6.1fms  → %s (%.1fx)", size, mAvg, cAvg, winner, speedup)
		coll.Drop(ctx)
	}

	writeOutput(map[string]any{"results": results})
}
