#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <optional>
#include <nlohmann/json.hpp>

namespace mg_clickhouse {

/**
 * Describes how a single MongoDB field maps to a ClickHouse column.
 */
struct FieldMapping {
    std::string mongo_field;
    std::string ch_column;
    std::string ch_type; // ClickHouse type: String, Int64, Float64, DateTime, etc.
};

/**
 * Describes the full mapping from a MongoDB collection to a ClickHouse table.
 */
struct CollectionMapping {
    std::string collection;
    std::string clickhouse_database;
    std::string clickhouse_table;
    std::vector<FieldMapping> fields;
    std::string engine = "ReplacingMergeTree";
    std::vector<std::string> order_by;
    bool enabled = true;
};

// JSON serialization
void to_json(nlohmann::json& j, const FieldMapping& f);
void from_json(const nlohmann::json& j, FieldMapping& f);
void to_json(nlohmann::json& j, const CollectionMapping& m);
void from_json(const nlohmann::json& j, CollectionMapping& m);

/**
 * Thread-safe registry of collection-to-ClickHouse mappings.
 * Provides CRUD operations for the management API and lookup for the sync engine.
 */
class SchemaMappingRegistry {
public:
    SchemaMappingRegistry() = default;

    /** Add or update a mapping. Returns true if created, false if updated. */
    bool upsert(const CollectionMapping& mapping);

    /** Remove a mapping by collection name. Returns true if found and removed. */
    bool remove(const std::string& collection);

    /** Get mapping for a collection. Returns nullopt if not found. */
    std::optional<CollectionMapping> get(const std::string& collection) const;

    /** Get all mappings. */
    std::vector<CollectionMapping> get_all() const;

    /** Check if a collection has a mapping. */
    bool has_mapping(const std::string& collection) const;

    /** Generate the CREATE TABLE DDL for a mapping. */
    std::string generate_create_table_sql(const CollectionMapping& mapping) const;

    /** Persist mappings to a JSON file. */
    void save_to_file(const std::string& path) const;

    /** Load mappings from a JSON file. */
    void load_from_file(const std::string& path);

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, CollectionMapping> mappings_;
};

} // namespace mg_clickhouse
