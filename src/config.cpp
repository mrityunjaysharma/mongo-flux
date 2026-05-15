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

    return config;
}

} // namespace mg_clickhouse
