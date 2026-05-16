package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// --- Database Connectivity ---

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

// --- Output ---

func writeOutput(data any) {
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

func generateDocs(n int) []any {
	docs := make([]any, n)
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

// --- Data Loading ---

func loadMongoDB(coll *mongo.Collection, docs []any) time.Duration {
	ctx := context.Background()
	coll.Drop(ctx)
	start := time.Now()
	batch := 10000
	for i := 0; i < len(docs); i += batch {
		end := min(i+batch, len(docs))
		coll.InsertMany(ctx, docs[i:end])
	}
	return time.Since(start)
}

func loadClickHouse(table string, docs []any) time.Duration {
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
		end := min(i+batch, len(docs))
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

// --- Timing ---

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

// avgExcludeFirst computes the average excluding the first (warmup) iteration.
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

// --- Math Helpers ---

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

func fmax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
