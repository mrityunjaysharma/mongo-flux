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
	"flag"
	"log"
	"os"
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
