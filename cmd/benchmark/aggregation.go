package main

import (
	"context"
	"fmt"
	"log"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func runAggregationBenchmark() {
	log.Printf("=== Aggregation Benchmark: %d records, %d iterations ===", *records, *iterations)

	client, err := connectMongo()
	if err != nil {
		log.Fatalf("MongoDB connect: %v", err)
	}
	defer client.Disconnect(context.Background())

	coll := client.Database(*mongoDB).Collection("bench_agg")
	table := "bench_agg"

	docs := generateDocs(*records)
	log.Printf("Loading data...")
	loadMongoDB(coll, docs)
	loadClickHouse(table, docs)

	queries := []struct {
		Name  string
		Mongo mongo.Pipeline
		CH    string
	}{
		{"Count by status",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "c", Value: bson.D{{Key: "$sum", Value: 1}}}}}}},
			fmt.Sprintf("SELECT status, count() FROM %s.%s GROUP BY status", *chDB, table)},
		{"Revenue by region",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$region"}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}}}}}},
			fmt.Sprintf("SELECT region, sum(amount) FROM %s.%s GROUP BY region", *chDB, table)},
		{"Avg by category",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$category"}, {Key: "avg", Value: bson.D{{Key: "$avg", Value: "$amount"}}}}}}},
			fmt.Sprintf("SELECT category, avg(amount) FROM %s.%s GROUP BY category", *chDB, table)},
		{"Top 10 spenders",
			mongo.Pipeline{
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$name"}, {Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "total", Value: -1}}}},
				bson.D{{Key: "$limit", Value: 10}},
			},
			fmt.Sprintf("SELECT name, sum(amount) as t FROM %s.%s GROUP BY name ORDER BY t DESC LIMIT 10", *chDB, table)},
		{"Filter + group",
			mongo.Pipeline{
				bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "active"}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$category"}, {Key: "c", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
			},
			fmt.Sprintf("SELECT category, count() FROM %s.%s WHERE status='active' GROUP BY category", *chDB, table)},
		{"2D GROUP BY",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "c", Value: "$category"}, {Key: "r", Value: "$region"}}}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}}},
			fmt.Sprintf("SELECT category, region, count() FROM %s.%s GROUP BY category, region", *chDB, table)},
		{"Min/Max/Avg score",
			mongo.Pipeline{bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$status"}, {Key: "avg", Value: bson.D{{Key: "$avg", Value: "$score"}}}, {Key: "min", Value: bson.D{{Key: "$min", Value: "$score"}}}, {Key: "max", Value: bson.D{{Key: "$max", Value: "$score"}}}}}}},
			fmt.Sprintf("SELECT status, avg(score), min(score), max(score) FROM %s.%s GROUP BY status", *chDB, table)},
		{"Full count",
			mongo.Pipeline{bson.D{{Key: "$count", Value: "total"}}},
			fmt.Sprintf("SELECT count() FROM %s.%s", *chDB, table)},
	}

	type Result struct {
		Query   string  `json:"query"`
		MongoMs float64 `json:"mongo_ms"`
		CHMs    float64 `json:"clickhouse_ms"`
		Speedup float64 `json:"speedup"`
	}
	var results []Result

	log.Printf("Running %d queries x %d iterations...", len(queries), *iterations)
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
		log.Printf("  %-25s  Mongo: %7.1fms  CH: %6.1fms  %5.1fx", q.Name, mAvg, cAvg, speedup)
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Speedup > results[j].Speedup })
	totalSpeedup := 0.0
	for _, r := range results {
		totalSpeedup += r.Speedup
	}
	avgSpeedup := totalSpeedup / float64(len(results))
	log.Printf("\n  Average speedup: %.1fx | Peak: %.1fx on '%s'", avgSpeedup, results[0].Speedup, results[0].Query)

	writeOutput(map[string]any{
		"queries": results, "avg_speedup": round1(avgSpeedup),
		"peak_speedup": round1(results[0].Speedup), "peak_query": results[0].Query, "records": *records,
	})
}
