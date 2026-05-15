#include "mg_clickhouse/config.h"

#include <fstream>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

namespace mg_clickhouse {

Config load_config(const std::string& path) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const YAML::Exception& e) {
        throw std::runtime_error("Failed to load config file '" + path + "': " + e.what());
    }

    Config config;

    if (auto mongo = root["mongo"]) {
        if (mongo["uri"]) config.mongo.uri = mongo["uri"].as<std::string>();
        if (mongo["database"]) config.mongo.database = mongo["database"].as<std::string>();
    }

    if (auto ch = root["clickhouse"]) {
        if (ch["host"]) config.clickhouse.host = ch["host"].as<std::string>();
        if (ch["port"]) config.clickhouse.port = ch["port"].as<int>();
        if (ch["database"]) config.clickhouse.database = ch["database"].as<std::string>();
        if (ch["user"]) config.clickhouse.user = ch["user"].as<std::string>();
        if (ch["password"]) config.clickhouse.password = ch["password"].as<std::string>();
        if (ch["cluster"]) config.clickhouse.cluster = ch["cluster"].as<std::string>();
    }

    if (auto sync = root["sync"]) {
        if (sync["mode"]) config.sync.mode = sync["mode"].as<std::string>();
        if (sync["batch_size"]) config.sync.batch_size = sync["batch_size"].as<int>();
        if (sync["flush_interval_ms"]) config.sync.flush_interval_ms = sync["flush_interval_ms"].as<int>();
        if (sync["resume_token_path"]) config.sync.resume_token_path = sync["resume_token_path"].as<std::string>();
    }

    if (auto api = root["api"]) {
        if (api["port"]) config.api.port = api["port"].as<int>();
        if (api["bind"]) config.api.bind = api["bind"].as<std::string>();
    }

    if (auto routing = root["routing"]) {
        if (routing["clickhouse_param"]) config.routing.clickhouse_param = routing["clickhouse_param"].as<std::string>();
    }

    if (auto logging = root["logging"]) {
        if (logging["level"]) config.logging.level = logging["level"].as<std::string>();
        if (logging["file"]) config.logging.file = logging["file"].as<std::string>();
    }

    // Validate required fields
    if (config.mongo.uri.empty()) {
        throw std::runtime_error("Config validation: mongo.uri is required");
    }
    if (config.mongo.database.empty()) {
        throw std::runtime_error("Config validation: mongo.database is required");
    }
    if (config.clickhouse.host.empty()) {
        throw std::runtime_error("Config validation: clickhouse.host is required");
    }
    if (config.clickhouse.database.empty()) {
        throw std::runtime_error("Config validation: clickhouse.database is required");
    }

    // Validate numeric ranges
    if (config.clickhouse.port <= 0 || config.clickhouse.port > 65535) {
        throw std::runtime_error("Config validation: clickhouse.port must be 1-65535");
    }
    if (config.api.port <= 0 || config.api.port > 65535) {
        throw std::runtime_error("Config validation: api.port must be 1-65535");
    }
    if (config.sync.batch_size <= 0 || config.sync.batch_size > 1000000) {
        throw std::runtime_error("Config validation: sync.batch_size must be 1-1000000");
    }
    if (config.sync.flush_interval_ms <= 0 || config.sync.flush_interval_ms > 60000) {
        throw std::runtime_error("Config validation: sync.flush_interval_ms must be 1-60000");
    }

    // Validate sync mode
    if (config.sync.mode != "oplog" && config.sync.mode != "changestream") {
        throw std::runtime_error("Config validation: sync.mode must be 'oplog' or 'changestream'");
    }

    return config;
}

} // namespace mg_clickhouse
