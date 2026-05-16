// Command benchmark runs MongoFlux performance benchmarks.
//
// Usage:
//
//	go run ./cmd/benchmark -type read -records 500000
//	go run ./cmd/benchmark -type write -records 100000
//	go run ./cmd/benchmark -type breakeven -max-size 100000
//	go run ./cmd/benchmark -type aggregation -records 500000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	benchType  = flag.String("type", "read", "Benchmark type: read, write, breakeven, aggregation")
	records    = flag.Int("records", 500000, "Number of records to generate")
	maxSize    = flag.Int("max-size", 2000000, "Max data size for breakeven benchmark")
	iterations = flag.Int("iterations", 3, "Query iterations for averaging")
	mongoURI   = flag.String("mongo-uri", envOr("MONGO_URI", "mongodb://localhost:27017/?directConnection=true"), "MongoDB URI")
	mongoDB    = flag.String("mongo-db", envOr("MONGO_DB", "benchmark"), "MongoDB database")
	chURL      = flag.String("ch-url", envOr("CH_URL", "http://localhost:8123"), "ClickHouse HTTP URL")
	chDB       = flag.String("ch-db", envOr("CH_DB", "benchmark"), "ClickHouse database")
	outputFile = flag.String("output", "", "Output JSON file (default: stdout)")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime)

	switch *benchType {
	case "read":
		runReadBenchmark()
	case "write":
		runWriteBenchmark()
	case "breakeven":
		runBreakevenBenchmark()
	case "aggregation":
		runAggregationBenchmark()
	default:
		log.Fatalf("Unknown benchmark type: %s. Use: read, write, breakeven, aggregation", *benchType)
	}
}

// --- Shared Infrastructure ---

func connectMongo() (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
}

func chQuery(sql string) (string, error) {
	resp, err := http.Post(*chURL+"/?database="+*chDB, "text/plain", strings.NewReader(sql))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var buf strings.Builder
	io.Copy(&buf, resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("CH error (%d): %s", resp.StatusCode, strings.TrimSpace(buf.String()))
	}
	return strings.TrimSpace(buf.String()), nil
}

func chExec(sql string) error {
	_, err := chQuery(sql)
	return err
}

func writeOutput(data interface{}) {
	out, _ := json.MarshalIndent(data, "", "  ")
	if *outputFile != "" {
		os.WriteFile(*outputFile, out, 0644)
		log.Printf("Results saved to %s", *outputFile)
	} else {
		fmt.Println(string(out))
	}
}

// --- Data Generation ---

var (
	categories = []string{"Infrastructure", "Application", "Security", "Network", "Database", "Storage", "Compute", "Messaging"}
	regions    = []string{"us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"}
	statuses   = []string{"active", "resolved", "acknowledged", "suppressed", "escalated", "closed"}
)

func generateDocs(n int) []interface{} {
	docs := make([]interface{}, n)
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range docs {
		docs[i] = bson.M{
			"name":      fmt.Sprintf("Record-%d", i),
			"category":  categories[rand.Intn(len(categories))],
			"region":    regions[rand.Intn(len(regions))],
			"status":    statuses[rand.Intn(len(statuses))],
			"amount":    rand.Float64() * 5000,
			"value":     int32(rand.Intn(10000)),
			"score":     rand.Float64() * 100,
			"active":    rand.Intn(2) == 1,
			"createdAt": baseDate.Add(time.Duration(rand.Intn(365*24*3600)) * time.Second),
		}
	}
	return docs
}

func loadMongoDB(coll *mongo.Collection, docs []interface{}) time.Duration {
	ctx := context.Background()
	coll.Drop(ctx)
	start := time.Now()
	batch := 10000
	for i := 0; i < len(docs); i += batch {
		end := i + batch
		if end > len(docs) {
			end = len(docs)
		}
		coll.InsertMany(ctx, docs[i:end])
	}
	return time.Since(start)
}

func loadClickHouse(table string, docs []interface{}) time.Duration {
	chExec(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", *chDB, table))
	chExec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *chDB))
	chExec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		name String, category LowCardinality(String), region LowCardinality(String),
		status LowCardinality(String), amount Float64, value Int32, score Float64,
		active UInt8, createdAt DateTime
	) ENGINE = MergeTree() ORDER BY (category, region, createdAt)`, *chDB, table))

	start := time.Now()
	batch := 50000
	for i := 0; i < len(docs); i += batch {
		end := i + batch
		if end > len(docs) {
			end = len(docs)
		}
		var sb strings.Builder
		for _, d := range docs[i:end] {
			doc := d.(bson.M)
			active := 0
			if b, ok := doc["active"].(bool); ok && b {
				active = 1
			}
			ts := doc["createdAt"].(time.Time).Format("2006-01-02 15:04:05")
			fmt.Fprintf(&sb, `{"name":"%s","category":"%s","region":"%s","status":"%s","amount":%f,"value":%d,"score":%f,"active":%d,"createdAt":"%s"}`+"\n",
				doc["name"], doc["category"], doc["region"], doc["status"],
				doc["amount"], doc["value"], doc["score"], active, ts)
		}
		resp, err := http.Post(
			fmt.Sprintf("%s/?query=INSERT+INTO+%s.%s+FORMAT+JSONEachRow&database=%s", *chURL, *chDB, table, *chDB),
			"text/plain", strings.NewReader(sb.String()))
		if err == nil {
			resp.Body.Close()
		}
	}
	return time.Since(start)
}

func timeMongoAgg(coll *mongo.Collection, pipeline mongo.Pipeline) float64 {
	ctx := context.Background()
	start := time.Now()
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return -1
	}
	var results []bson.M
	cursor.All(ctx, &results)
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func timeCHQuery(sql string) float64 {
	start := time.Now()
	_, err := chQuery(sql)
	if err != nil {
		return -1
	}
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func avgExcludeFirst(times []float64) float64 {
	if len(times) <= 1 {
		if len(times) == 1 {
			return times[0]
		}
		return 0
	}
	sum := 0.0
	for _, t := range times[1:] {
		sum += t
	}
	return sum / float64(len(times)-1)
}

// --- Read Benchmark ---

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
	writeOutput(map[string]interface{}{"queries": results, "avg_speedup": round1(totalSpeedup / float64(len(results))), "records": *records})
}

// --- Write Benchmark ---

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
		end := i + batch
		if end > len(docs) {
			end = len(docs)
		}
		coll.InsertMany(ctx, docs[i:end])
	}
	batchDuration := time.Since(start)
	batchRate := float64(*records) / batchDuration.Seconds()
	log.Printf("  Batch insert: %d docs/s (%.1fs)", int(batchRate), batchDuration.Seconds())

	singleCount := imin(*records, 2000)
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

	writeOutput(map[string]interface{}{
		"batch_rate_docs_s":     int(batchRate),
		"batch_duration_s":      round1(batchDuration.Seconds()),
		"single_rate_docs_s":    int(singleRate),
		"single_avg_latency_ms": round1(avgLatency),
		"records":               *records,
	})
}

// --- Breakeven Benchmark ---

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

	writeOutput(map[string]interface{}{"results": results})
}

// --- Aggregation Benchmark ---

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

	writeOutput(map[string]interface{}{
		"queries": results, "avg_speedup": round1(avgSpeedup),
		"peak_speedup": round1(results[0].Speedup), "peak_query": results[0].Query, "records": *records,
	})
}

// --- Helpers ---

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }
func fmax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
func imin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
