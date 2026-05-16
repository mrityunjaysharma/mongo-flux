package schema

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistry_upsertAndGet(t *testing.T) {
	r := NewRegistry()

	mapping := CollectionMapping{
		Collection:         "orders",
		ClickHouseDatabase: "analytics",
		ClickHouseTable:    "orders",
		Fields: []FieldMapping{
			{MongoField: "_id", CHColumn: "id", CHType: "String"},
			{MongoField: "amount", CHColumn: "amount", CHType: "Float64"},
		},
		Engine:  "ReplacingMergeTree",
		OrderBy: []string{"id"},
		Enabled: true,
	}

	created := r.Upsert(mapping)
	if !created {
		t.Error("expected Upsert to return true for new mapping")
	}

	got := r.Get("orders")
	if got == nil {
		t.Fatal("expected to get mapping back")
	}
	if got.ClickHouseTable != "orders" {
		t.Errorf("got table %s, want orders", got.ClickHouseTable)
	}

	// Update
	mapping.ClickHouseTable = "orders_v2"
	created = r.Upsert(mapping)
	if created {
		t.Error("expected Upsert to return false for update")
	}

	got = r.Get("orders")
	if got.ClickHouseTable != "orders_v2" {
		t.Errorf("got table %s, want orders_v2", got.ClickHouseTable)
	}
}

func TestRegistry_remove(t *testing.T) {
	r := NewRegistry()
	r.Upsert(CollectionMapping{Collection: "test", ClickHouseTable: "test", Enabled: true})

	if !r.Remove("test") {
		t.Error("expected Remove to return true")
	}
	if r.Remove("test") {
		t.Error("expected Remove to return false for missing")
	}
	if r.HasMapping("test") {
		t.Error("expected HasMapping to return false after remove")
	}
}

func TestRegistry_generateDDL_standalone(t *testing.T) {
	r := NewRegistry()
	mapping := CollectionMapping{
		Collection:         "orders",
		ClickHouseDatabase: "analytics",
		ClickHouseTable:    "orders",
		Fields: []FieldMapping{
			{MongoField: "_id", CHColumn: "id", CHType: "String"},
			{MongoField: "amount", CHColumn: "amount", CHType: "Float64"},
		},
		Engine:  "ReplacingMergeTree",
		OrderBy: []string{"id"},
	}

	ddl, err := r.GenerateCreateTableSQL(mapping)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS analytics.orders") {
		t.Errorf("DDL missing CREATE TABLE: %s", ddl)
	}
	if !strings.Contains(ddl, "ReplacingMergeTree()") {
		t.Errorf("DDL missing engine: %s", ddl)
	}
	if !strings.Contains(ddl, "ORDER BY (id)") {
		t.Errorf("DDL missing ORDER BY: %s", ddl)
	}
	if strings.Contains(ddl, "ON CLUSTER") {
		t.Error("standalone DDL should not have ON CLUSTER")
	}
}

func TestRegistry_generateDDL_clustered(t *testing.T) {
	r := NewRegistry()
	mapping := CollectionMapping{
		Collection:         "events",
		ClickHouseDatabase: "analytics",
		ClickHouseTable:    "events",
		Fields: []FieldMapping{
			{MongoField: "_id", CHColumn: "id", CHType: "String"},
		},
		Engine:      "ReplacingMergeTree",
		OrderBy:     []string{"id"},
		Cluster:     "prod",
		ShardingKey: "cityHash64(id)",
	}

	ddl, err := r.GenerateCreateTableSQL(mapping)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(ddl, "events_local ON CLUSTER 'prod'") {
		t.Errorf("DDL missing local table with ON CLUSTER: %s", ddl)
	}
	if !strings.Contains(ddl, "ENGINE = Distributed('prod'") {
		t.Errorf("DDL missing Distributed engine: %s", ddl)
	}
	if !strings.Contains(ddl, "cityHash64(id)") {
		t.Errorf("DDL missing sharding key: %s", ddl)
	}
}

func TestRegistry_saveAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mappings.json")

	r := NewRegistry()
	r.Upsert(CollectionMapping{
		Collection:         "orders",
		ClickHouseDatabase: "analytics",
		ClickHouseTable:    "orders",
		Fields:             []FieldMapping{{MongoField: "_id", CHColumn: "id", CHType: "String"}},
		Enabled:            true,
	})

	if err := r.SaveToFile(path); err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}

	// Load into new registry
	r2 := NewRegistry()
	if err := r2.LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	got := r2.Get("orders")
	if got == nil {
		t.Fatal("expected to load mapping")
	}
	if got.ClickHouseTable != "orders" {
		t.Errorf("got table %s, want orders", got.ClickHouseTable)
	}
}

func TestRegistry_loadFromFile_notExists(t *testing.T) {
	r := NewRegistry()
	err := r.LoadFromFile("/nonexistent/path/file.json")
	if err != nil {
		t.Errorf("expected no error for missing file, got: %v", err)
	}
}
