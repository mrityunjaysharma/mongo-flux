#include <iostream>
#include <csignal>
#include <atomic>
#include <memory>

#include <mongocxx/instance.hpp>

#include "mg_clickhouse/config.h"
#include "mg_clickhouse/schema_mapping.h"
#include "mg_clickhouse/clickhouse_client.h"
#include "mg_clickhouse/oplog_sync.h"
#include "mg_clickhouse/change_stream_sync.h"
#include "mg_clickhouse/query_translator.h"
#include "mg_clickhouse/mongo_proxy.h"
#include "mg_clickhouse/management_api.h"

static std::atomic<bool> g_running{true};

static void signal_handler(int signum) {
    (void)signum;
    g_running = false;
}

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [config_path]\n"
              << "  config_path: Path to mg-clickhouse.yaml (default: /etc/mg-clickhouse/mg-clickhouse.yaml)\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "/etc/mg-clickhouse/mg-clickhouse.yaml";
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
        config_path = arg;
    }

    // Load configuration
    mg_clickhouse::Config config;
    try {
        config = mg_clickhouse::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "[mg-clickhouse] Failed to load config: " << e.what() << std::endl;
        return 1;
    }

    // Initialize MongoDB driver (must be done once per process)
    mongocxx::instance mongo_instance{};

    // Initialize components
    auto registry = std::make_shared<mg_clickhouse::SchemaMappingRegistry>();

    // Load persisted mappings
    std::string mappings_file = config.sync.resume_token_path + "/mappings.json";
    try {
        registry->load_from_file(mappings_file);
        std::cout << "[mg-clickhouse] Loaded " << registry->get_all().size()
                  << " schema mappings" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[mg-clickhouse] Warning: " << e.what() << std::endl;
    }

    // ClickHouse client
    auto ch_client = std::make_shared<mg_clickhouse::ClickHouseClient>(config.clickhouse);
    try {
        ch_client->ping();
        std::cout << "[mg-clickhouse] Connected to ClickHouse at "
                  << config.clickhouse.host << ":" << config.clickhouse.port << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[mg-clickhouse] Warning: ClickHouse not reachable: " << e.what() << std::endl;
    }

    // Change stream sync (kept as fallback for sharded/Atlas deployments)
    auto cs_sync = std::make_shared<mg_clickhouse::ChangeStreamSync>(config, registry, ch_client);

    // Oplog sync — direct oplog tailing, same mechanism as MongoDB secondaries
    auto oplog_sync = std::make_shared<mg_clickhouse::OplogSync>(config, registry, ch_client);

    // Query translator
    auto translator = std::make_shared<mg_clickhouse::QueryTranslator>(registry);

    // Mongo proxy
    auto proxy = std::make_shared<mg_clickhouse::MongoProxy>(config, registry, ch_client, translator);

    // Management API (uses change_stream_sync for the restart endpoint)
    auto api = std::make_shared<mg_clickhouse::ManagementApi>(config.api, registry, ch_client, cs_sync, oplog_sync);

    // Set up signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Start sync based on configured mode
    if (config.sync.mode == "oplog") {
        std::cout << "[mg-clickhouse] Starting oplog sync (direct tailing, like a secondary node)..." << std::endl;
        oplog_sync->start();
    } else {
        std::cout << "[mg-clickhouse] Starting change stream sync..." << std::endl;
        cs_sync->start();
    }

    std::cout << "[mg-clickhouse] Starting management API on port " << config.api.port << std::endl;
    api->start();

    std::cout << "[mg-clickhouse] mg-clickhouse is running. Press Ctrl+C to stop." << std::endl;

    // Main loop
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    std::cout << "\n[mg-clickhouse] Shutting down..." << std::endl;

    api->stop();
    oplog_sync->stop();
    cs_sync->stop();

    // Persist mappings
    try {
        registry->save_to_file(mappings_file);
    } catch (const std::exception& e) {
        std::cerr << "[mg-clickhouse] Warning: Failed to save mappings: " << e.what() << std::endl;
    }

    std::cout << "[mg-clickhouse] Shutdown complete." << std::endl;
    return 0;
}
