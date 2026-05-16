package translator

import (
	"testing"

	"github.com/mongoflux/mongoflux/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
)

func testRegistry() *schema.Registry {
	r := schema.NewRegistry()
	r.Upsert(schema.CollectionMapping{
		Collection:         "orders",
		ClickHouseDatabase: "analytics",
		ClickHouseTable:    "orders",
		Fields: []schema.FieldMapping{
			{MongoField: "_id", CHColumn: "id", CHType: "String"},
			{MongoField: "amount", CHColumn: "amount", CHType: "Float64"},
			{MongoField: "status", CHColumn: "status", CHType: "LowCardinality(String)"},
			{MongoField: "region", CHColumn: "region", CHType: "LowCardinality(String)"},
			{MongoField: "created_at", CHColumn: "created_at", CHType: "DateTime"},
		},
		Engine:  "ReplacingMergeTree",
		OrderBy: []string{"created_at", "id"},
		Enabled: true,
	})
	return r
}

func TestTranslateFind_simpleFilter(t *testing.T) {
	tr := NewTranslator(testRegistry())

	sql, err := tr.TranslateFind("orders",
		bson.D{{Key: "status", Value: "active"}},
		nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `id`, `amount`, `status`, `region`, `created_at` FROM analytics.`orders` WHERE `status` = 'active'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateFind_withLimitAndSort(t *testing.T) {
	tr := NewTranslator(testRegistry())

	sql, err := tr.TranslateFind("orders",
		nil,
		nil,
		bson.D{{Key: "amount", Value: int32(-1)}},
		10, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `id`, `amount`, `status`, `region`, `created_at` FROM analytics.`orders` ORDER BY `amount` DESC LIMIT 10 OFFSET 5"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateFind_comparisonOperators(t *testing.T) {
	tr := NewTranslator(testRegistry())

	sql, err := tr.TranslateFind("orders",
		bson.D{{Key: "amount", Value: bson.D{{Key: "$gt", Value: int32(100)}, {Key: "$lte", Value: int32(500)}}}},
		nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `id`, `amount`, `status`, `region`, `created_at` FROM analytics.`orders` WHERE (`amount` > 100) AND (`amount` <= 500)"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateFind_inOperator(t *testing.T) {
	tr := NewTranslator(testRegistry())

	sql, err := tr.TranslateFind("orders",
		bson.D{{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{"active", "pending"}}}}},
		nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `id`, `amount`, `status`, `region`, `created_at` FROM analytics.`orders` WHERE `status` IN ('active', 'pending')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateAggregate_groupByWithSum(t *testing.T) {
	tr := NewTranslator(testRegistry())

	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$status"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$amount"}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "total", Value: int32(-1)}}}},
	}

	sql, err := tr.TranslateAggregate("orders", pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `status`, sum(`amount`) AS `total` FROM analytics.`orders` GROUP BY `status` ORDER BY `total` DESC"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateAggregate_matchThenGroup(t *testing.T) {
	tr := NewTranslator(testRegistry())

	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "status", Value: "active"}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$region"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		}}},
	}

	sql, err := tr.TranslateAggregate("orders", pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `region`, count(*) AS `count` FROM analytics.`orders` WHERE `status` = 'active' GROUP BY `region`"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateAggregate_count(t *testing.T) {
	tr := NewTranslator(testRegistry())

	pipeline := []bson.D{
		{{Key: "$count", Value: "total"}},
	}

	sql, err := tr.TranslateAggregate("orders", pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT count(*) AS `total` FROM analytics.`orders`"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateAggregate_compoundGroupKey(t *testing.T) {
	tr := NewTranslator(testRegistry())

	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "s", Value: "$status"},
				{Key: "r", Value: "$region"},
			}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		}}},
	}

	sql, err := tr.TranslateAggregate("orders", pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT `status` AS `s`, `region` AS `r`, count(*) AS `count` FROM analytics.`orders` GROUP BY `status`, `region`"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTranslateFind_noMapping(t *testing.T) {
	tr := NewTranslator(testRegistry())

	_, err := tr.TranslateFind("nonexistent", nil, nil, nil, 0, 0)
	if err == nil {
		t.Fatal("expected error for missing mapping")
	}
}
