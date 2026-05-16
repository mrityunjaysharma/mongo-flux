package routing

import (
	"testing"
)

func TestParseMongoURI_full(t *testing.T) {
	uri := "mongodb://user:pass@host1:27017,host2:27018/mydb?replicaSet=rs0&clickhouse=true"
	parsed := ParseMongoURI(uri)

	if parsed.Scheme != "mongodb" {
		t.Errorf("got scheme %s", parsed.Scheme)
	}
	if parsed.Host != "host1" {
		t.Errorf("got host %s", parsed.Host)
	}
	if parsed.Port != "27017" {
		t.Errorf("got port %s", parsed.Port)
	}
	if parsed.Database != "mydb" {
		t.Errorf("got database %s", parsed.Database)
	}
	if parsed.Params["replicaSet"] != "rs0" {
		t.Errorf("got replicaSet %s", parsed.Params["replicaSet"])
	}
	if parsed.Params["clickhouse"] != "true" {
		t.Errorf("got clickhouse %s", parsed.Params["clickhouse"])
	}
}

func TestParseMongoURI_simple(t *testing.T) {
	uri := "mongodb://localhost:27017"
	parsed := ParseMongoURI(uri)

	if parsed.Host != "localhost" {
		t.Errorf("got host %s", parsed.Host)
	}
	if parsed.Port != "27017" {
		t.Errorf("got port %s", parsed.Port)
	}
}

func TestHasClickHouseRouting_true(t *testing.T) {
	tests := []struct {
		params map[string]string
		want   bool
	}{
		{map[string]string{"clickhouse": "true"}, true},
		{map[string]string{"clickhouse": "1"}, true},
		{map[string]string{"clickhouse": "yes"}, true},
		{map[string]string{"clickhouse": "TRUE"}, true},
		{map[string]string{"clickhouse": "false"}, false},
		{map[string]string{"clickhouse": "0"}, false},
		{map[string]string{}, false},
	}

	for _, tt := range tests {
		uri := ParsedURI{Params: tt.params}
		got := HasClickHouseRouting(uri, "clickhouse")
		if got != tt.want {
			t.Errorf("params=%v: got %v, want %v", tt.params, got, tt.want)
		}
	}
}
