package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func runReadBenchmark() {
	log.Printf("=== Read Benchmark: %d records, %d iterations ===", *records, *iterations)

	client, err := connectMongo()
	if err != nil {
		log.Fatalf("MongoDB connect: %v", err)
	}
	defer client.Disconnect(context.Background())

	coll := client.Database(*mongoDB).Collection("bench_read")
	table := "bench_read"

	docs := generateDocs(*records)
	log.Printf("Loading MongoDB...")
	mongoTime := loadMongoDB(coll, docs)
	log.Printf("  MongoDB: %v", mongoTime.Round(time.Millisecond))
	log.Printf("Loading ClickHouse...")
	chTime := loadClickHouse(table, docs)
	log.Printf("  ClickHouse: %v", chTime.Round(time.Millisecond))

	queries := []struct {
		Name  string
		Mongo mongo.Pipeline
		CH    string
	}{
		{"Count by status",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}}},
			fmt.Sprintf("SELECT status, count() FROM %s.%s GROUP BY status", *chDB, table)},
		{"Avg amount by region",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$region"}, {Key: "avg", Value: bson.D{{Key: "$avg", Value: "$amount"}}}}}}},
			fmt.Sprintf("SELECT region, avg(amount) FROM %s.%s GROUP BY region", *chDB, table)},
		{"Top 10 by spend",
			mongo.Pipeline{
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$category"}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}},
				bson.D{{Key: "$limit", Value: 10}},
			},
			fmt.Sprintf("SELECT category, sum(amount) as total FROM %s.%s GROUP BY category ORDER BY total DESC LIMIT 10", *chDB, table)},
		{"Date range scan",
			mongo.Pipeline{bson.D{{Key: "$match", Value: bson.D{{Key: "createdAt", Value: bson.D{{Key: "$gte", Value: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)}, {Key: "$lt", Value: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)}}}}}}},
			fmt.Sprintf("SELECT count() FROM %s.%s WHERE createdAt >= '2024-04-01' AND createdAt < '2024-07-01'", *chDB, table)},
		{"Full count",
			mongo.Pipeline{bson.D{{Key: "$count", Value: "total"}}},
			fmt.Sprintf("SELECT count() FROM %s.%s", *chDB, table)},
		{"2D GROUP BY",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "c", Value: "$category"}, {Key: "r", Value: "$region"}}}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}}},
			fmt.Sprintf("SELECT category, region, count() FROM %s.%s GROUP BY category, region", *chDB, table)},
	}

	type Result struct {
		Query   string  `json:"query"`
		MongoMs float64 `json:"mongo_ms"`
		CHMs    float64 `json:"clickhouse_ms"`
		Speedup float64 `json:"speedup"`
	}
	var results []Result

	log.Printf("Running queries...")
	for _, q := range queries {
		var mongoTimes, chTimes []float64
		for i := 0; i < *iterations+1; i++ {
			mongoTimes = append(mongoTimes, timeMongoAgg(coll, q.Mongo))
			chTimes = append(chTimes, timeCHQuery(q.CH))
		}
		mAvg := avgExcludeFirst(mongoTimes)
		cAvg := avgExcludeFirst(chTimes)
		speedup := mAvg / fmax(cAvg, 0.01)
		results = append(results, Result{q.Name, round1(mAvg), round1(cAvg), round1(speedup)})
		log.Printf("  %-30s  Mongo: %6.1fms  CH: %6.1fms  %5.1fx", q.Name, mAvg, cAvg, speedup)
	}

	totalSpeedup := 0.0
	for _, r := range results {
		totalSpeedup += r.Speedup
	}
	log.Printf("\n  Average speedup: %.1fx", totalSpeedup/float64(len(results)))
	writeOutput(map[string]any{"queries": results, "avg_speedup": round1(totalSpeedup / float64(len(results))), "records": *records})
}
