package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

// FieldMapping describes how a single MongoDB field maps to a ClickHouse column.
type FieldMapping struct {
	MongoField string `json:"mongo_field" yaml:"mongo_field"`
	CHColumn   string `json:"ch_column" yaml:"ch_column"`
	CHType     string `json:"ch_type" yaml:"ch_type"`
}

// CollectionMapping describes the full mapping from a MongoDB collection to a ClickHouse table.
type CollectionMapping struct {
	Collection         string         `json:"collection"`
	ClickHouseDatabase string         `json:"clickhouse_database"`
	ClickHouseTable    string         `json:"clickhouse_table"`
	Fields             []FieldMapping `json:"fields"`
	Engine             string         `json:"engine"`
	OrderBy            []string       `json:"order_by"`
	Cluster            string         `json:"cluster"`
	ShardingKey        string         `json:"sharding_key"`
	Enabled            bool           `json:"enabled"`
}

// Registry is a thread-safe registry of collection-to-ClickHouse mappings.
type Registry struct {
	mu       sync.RWMutex
	mappings map[string]CollectionMapping
}

// NewRegistry creates a new empty schema registry.
func NewRegistry() *Registry {
	return &Registry{
		mappings: make(map[string]CollectionMapping),
	}
}

// Upsert adds or updates a mapping. Returns true if created, false if updated.
func (r *Registry) Upsert(mapping CollectionMapping) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.mappings[mapping.Collection]
	r.mappings[mapping.Collection] = mapping
	return !exists
}

// Remove deletes a mapping by collection name. Returns true if found and removed.
func (r *Registry) Remove(collection string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.mappings[collection]
	if exists {
		delete(r.mappings, collection)
	}
	return exists
}

// Get returns the mapping for a collection, or nil if not found.
func (r *Registry) Get(collection string) *CollectionMapping {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.mappings[collection]
	if !ok {
		return nil
	}
	return &m
}

// GetAll returns all mappings.
func (r *Registry) GetAll() []CollectionMapping {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]CollectionMapping, 0, len(r.mappings))
	for _, m := range r.mappings {
		result = append(result, m)
	}
	return result
}

// HasMapping checks if a collection has a mapping.
func (r *Registry) HasMapping(collection string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.mappings[collection]
	return ok
}

// GenerateCreateTableSQL generates the CREATE TABLE DDL for a mapping.
func (r *Registry) GenerateCreateTableSQL(mapping CollectionMapping) string {
	var sb strings.Builder

	localTable := mapping.ClickHouseTable
	if mapping.Cluster != "" {
		localTable = mapping.ClickHouseTable + "_local"
	}

	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s",
		mapping.ClickHouseDatabase, localTable))

	if mapping.Cluster != "" {
		sb.WriteString(fmt.Sprintf(" ON CLUSTER '%s'", mapping.Cluster))
	}

	sb.WriteString(" (\n")
	for i, field := range mapping.Fields {
		sb.WriteString(fmt.Sprintf("    %s %s", field.CHColumn, field.CHType))
		if i+1 < len(mapping.Fields) {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	engine := mapping.Engine
	if engine == "" {
		engine = "ReplacingMergeTree"
	}
	sb.WriteString(fmt.Sprintf(") ENGINE = %s()\n", engine))

	if len(mapping.OrderBy) > 0 {
		sb.WriteString(fmt.Sprintf("ORDER BY (%s)\n", strings.Join(mapping.OrderBy, ", ")))
	}

	if mapping.Cluster != "" {
		shardKey := mapping.ShardingKey
		if shardKey == "" {
			shardKey = "rand()"
		}
		sb.WriteString(";\n\n")
		sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER '%s'\n",
			mapping.ClickHouseDatabase, mapping.ClickHouseTable, mapping.Cluster))
		sb.WriteString(fmt.Sprintf("AS %s.%s\n", mapping.ClickHouseDatabase, localTable))
		sb.WriteString(fmt.Sprintf("ENGINE = Distributed('%s', '%s', '%s', %s)\n",
			mapping.Cluster, mapping.ClickHouseDatabase, localTable, shardKey))
	}

	return sb.String()
}

// SaveToFile persists mappings to a JSON file.
func (r *Registry) SaveToFile(path string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	mappings := make([]CollectionMapping, 0, len(r.mappings))
	for _, m := range r.mappings {
		mappings = append(mappings, m)
	}

	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal mappings: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}

// LoadFromFile loads mappings from a JSON file.
func (r *Registry) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet is fine
		}
		return fmt.Errorf("failed to read mapping file '%s': %w", path, err)
	}

	var mappings []CollectionMapping
	if err := json.Unmarshal(data, &mappings); err != nil {
		return fmt.Errorf("failed to parse mapping file '%s': %w", path, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.mappings = make(map[string]CollectionMapping, len(mappings))
	for _, m := range mappings {
		r.mappings[m.Collection] = m
	}
	return nil
}
