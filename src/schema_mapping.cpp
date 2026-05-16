#include "mongoflux/schema_mapping.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace mongoflux {

void to_json(nlohmann::json& j, const FieldMapping& f) {
    j = nlohmann::json{
        {"mongo_field", f.mongo_field},
        {"ch_column", f.ch_column},
        {"ch_type", f.ch_type}
    };
}

void from_json(const nlohmann::json& j, FieldMapping& f) {
    j.at("mongo_field").get_to(f.mongo_field);
    j.at("ch_column").get_to(f.ch_column);
    j.at("ch_type").get_to(f.ch_type);
}

void to_json(nlohmann::json& j, const CollectionMapping& m) {
    j = nlohmann::json{
        {"collection", m.collection},
        {"clickhouse_database", m.clickhouse_database},
        {"clickhouse_table", m.clickhouse_table},
        {"fields", m.fields},
        {"engine", m.engine},
        {"order_by", m.order_by},
        {"cluster", m.cluster},
        {"sharding_key", m.sharding_key},
        {"enabled", m.enabled}
    };
}

void from_json(const nlohmann::json& j, CollectionMapping& m) {
    j.at("collection").get_to(m.collection);
    j.at("clickhouse_table").get_to(m.clickhouse_table);

    if (j.contains("clickhouse_database"))
        j.at("clickhouse_database").get_to(m.clickhouse_database);

    j.at("fields").get_to(m.fields);

    if (j.contains("engine"))
        j.at("engine").get_to(m.engine);
    if (j.contains("order_by"))
        j.at("order_by").get_to(m.order_by);
    if (j.contains("cluster"))
        j.at("cluster").get_to(m.cluster);
    if (j.contains("sharding_key"))
        j.at("sharding_key").get_to(m.sharding_key);
    if (j.contains("enabled"))
        j.at("enabled").get_to(m.enabled);
}

bool SchemaMappingRegistry::upsert(const CollectionMapping& mapping) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto [it, inserted] = mappings_.insert_or_assign(mapping.collection, mapping);
    return inserted;
}

bool SchemaMappingRegistry::remove(const std::string& collection) {
    std::lock_guard<std::mutex> lock(mutex_);
    return mappings_.erase(collection) > 0;
}

std::optional<CollectionMapping> SchemaMappingRegistry::get(const std::string& collection) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = mappings_.find(collection);
    if (it == mappings_.end()) return std::nullopt;
    return it->second;
}

std::vector<CollectionMapping> SchemaMappingRegistry::get_all() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<CollectionMapping> result;
    result.reserve(mappings_.size());
    for (const auto& [key, val] : mappings_) {
        result.push_back(val);
    }
    return result;
}

bool SchemaMappingRegistry::has_mapping(const std::string& collection) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return mappings_.count(collection) > 0;
}

std::string SchemaMappingRegistry::generate_create_table_sql(const CollectionMapping& mapping) const {
    std::ostringstream sql;

    // Local table (the actual MergeTree table on each shard)
    std::string local_table = mapping.clickhouse_table;
    if (!mapping.cluster.empty()) {
        // For clustered deployments, the local table gets a _local suffix
        // and a Distributed table is created with the user-facing name
        local_table = mapping.clickhouse_table + "_local";
    }

    sql << "CREATE TABLE IF NOT EXISTS "
        << mapping.clickhouse_database << "." << local_table;

    // ON CLUSTER clause for distributed DDL
    if (!mapping.cluster.empty()) {
        sql << " ON CLUSTER '" << mapping.cluster << "'";
    }

    sql << " (\n";

    for (size_t i = 0; i < mapping.fields.size(); ++i) {
        const auto& field = mapping.fields[i];
        sql << "    " << field.ch_column << " " << field.ch_type;
        if (i + 1 < mapping.fields.size()) sql << ",";
        sql << "\n";
    }

    sql << ") ENGINE = " << mapping.engine << "()\n";

    if (!mapping.order_by.empty()) {
        sql << "ORDER BY (";
        for (size_t i = 0; i < mapping.order_by.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << mapping.order_by[i];
        }
        sql << ")\n";
    }

    // For clustered deployments, also generate the Distributed table DDL
    if (!mapping.cluster.empty()) {
        std::string shard_key = mapping.sharding_key.empty() ? "rand()" : mapping.sharding_key;

        sql << ";\n\n";
        sql << "CREATE TABLE IF NOT EXISTS "
            << mapping.clickhouse_database << "." << mapping.clickhouse_table
            << " ON CLUSTER '" << mapping.cluster << "'\n";
        sql << "AS " << mapping.clickhouse_database << "." << local_table << "\n";
        sql << "ENGINE = Distributed('" << mapping.cluster << "', '"
            << mapping.clickhouse_database << "', '" << local_table << "', "
            << shard_key << ")\n";
    }

    return sql.str();
}

void SchemaMappingRegistry::save_to_file(const std::string& path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    nlohmann::json j;
    for (const auto& [key, val] : mappings_) {
        j.push_back(val);
    }

    std::ofstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open mapping file for writing: " + path);
    }
    file << j.dump(2);
}

void SchemaMappingRegistry::load_from_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) return; // No file yet is fine

    nlohmann::json j;
    try {
        file >> j;
    } catch (const nlohmann::json::exception& e) {
        throw std::runtime_error("Failed to parse mapping file '" + path + "': " + e.what());
    }

    std::lock_guard<std::mutex> lock(mutex_);
    mappings_.clear();
    for (const auto& item : j) {
        CollectionMapping mapping = item.get<CollectionMapping>();
        mappings_[mapping.collection] = mapping;
    }
}

} // namespace mongoflux
