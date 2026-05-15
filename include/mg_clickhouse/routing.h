#pragma once

#include <string>
#include <unordered_map>

namespace mg_clickhouse {

/**
 * Parse a MongoDB connection URI and extract query parameters.
 * Used to detect the clickhouse routing flag.
 */
struct ParsedUri {
    std::string scheme;
    std::string host;
    int port = 27017;
    std::string database;
    std::unordered_map<std::string, std::string> params;
};

/**
 * Parse a MongoDB URI string into components.
 * Supports: mongodb://[user:pass@]host[:port][/database][?key=value&...]
 */
ParsedUri parse_mongo_uri(const std::string& uri);

/**
 * Check if a URI has the ClickHouse routing parameter set to a truthy value.
 */
bool has_clickhouse_routing(const ParsedUri& uri, const std::string& param_name);

} // namespace mg_clickhouse
