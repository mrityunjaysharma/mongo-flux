#pragma once

#include <string>
#include <cstdint>

namespace mg_clickhouse {

struct MongoConfig {
    std::string uri = "mongodb://localhost:27017";
    std::string database = "myapp";
};

struct ClickHouseConfig {
    std::string host = "localhost";
    int port = 8123;
    std::string database = "analytics";
    std::string user = "default";
    std::string password;
    std::string cluster;  // Cluster name for distributed DDL (empty = standalone)
};

struct SyncConfig {
    std::string mode = "oplog"; // "oplog" (direct tailing) or "changestream"
    int batch_size = 1000;
    int flush_interval_ms = 500;
    std::string resume_token_path = "/var/lib/mg-clickhouse/resume_tokens";
};

struct ApiConfig {
    int port = 9090;
    std::string bind = "0.0.0.0";
};

struct RoutingConfig {
    std::string clickhouse_param = "clickhouse";
};

struct LoggingConfig {
    std::string level = "info";
    std::string file;
};

struct Config {
    MongoConfig mongo;
    ClickHouseConfig clickhouse;
    SyncConfig sync;
    ApiConfig api;
    RoutingConfig routing;
    LoggingConfig logging;
};

/**
 * Load configuration from a YAML file.
 * Throws std::runtime_error on parse failure.
 */
Config load_config(const std::string& path);

} // namespace mg_clickhouse
