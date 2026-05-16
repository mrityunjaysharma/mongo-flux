#include <iostream>
#include <csignal>
#include <atomic>
#include <memory>

#include <mongocxx/instance.hpp>

#include "mongoflux/config.h"
#include "mongoflux/schema_mapping.h"
#include "mongoflux/clickhouse_client.h"
#include "mongoflux/oplog_sync.h"
#include "mongoflux/change_stream_sync.h"
#include "mongoflux/query_translator.h"
#include "mongoflux/mongo_proxy.h"
#include "mongoflux/management_api.h"
#include "mongoflux/wire_proxy.h"

static std::atomic<bool> g_running{true};

static void signal_handler(int signum) {
    (void)signum;
    g_running = false;
}

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [config_path]\n"
              << "  config_path: Path to mongoflux.yaml (default: /etc/mongoflux/mongoflux.yaml)\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "/etc/mongoflux/mongoflux.yaml";
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
        config_path = arg;
    }

    // Load configuration
    mongoflux::Config config;
    try {
        config = mongoflux::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "[mongoflux] Failed to load config: " << e.what() << std::endl;
        return 1;
    }

    // Initialize MongoDB driver (must be done once per process)
    mongocxx::instance mongo_instance{};

    // Initialize components
    auto registry = std::make_shared<mongoflux::SchemaMappingRegistry>();

    // Load persisted mappings
    std::string mappings_file = config.sync.resume_token_path + "/mappings.json";
    try {
        registry->load_from_file(mappings_file);
        std::cout << "[mongoflux] Loaded " << registry->get_all().size()
                  << " schema mappings" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[mongoflux] Warning: " << e.what() << std::endl;
    }

    // ClickHouse client
    auto ch_client = std::make_shared<mongoflux::ClickHouseClient>(config.clickhouse);
    try {
        ch_client->ping();
        std::cout << "[mongoflux] Connected to ClickHouse at "
                  << config.clickhouse.host << ":" << config.clickhouse.port << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[mongoflux] Warning: ClickHouse not reachable: " << e.what() << std::endl;
    }

    // Change stream sync (kept as fallback for sharded/Atlas deployments)
    auto cs_sync = std::make_shared<mongoflux::ChangeStreamSync>(config, registry, ch_client);

    // Oplog sync — direct oplog tailing, same mechanism as MongoDB secondaries
    auto oplog_sync = std::make_shared<mongoflux::OplogSync>(config, registry, ch_client);

    // Query translator
    auto translator = std::make_shared<mongoflux::QueryTranslator>(registry);

    // Mongo proxy
    auto proxy = std::make_shared<mongoflux::MongoProxy>(config, registry, ch_client, translator);

    // Management API (uses change_stream_sync for the restart endpoint)
    auto api = std::make_shared<mongoflux::ManagementApi>(config.api, registry, ch_client, cs_sync, oplog_sync);

    // Wire protocol proxy — applications connect here with standard MongoDB drivers
    mongoflux::WireProxy::ProxyConfig wire_config;
    wire_config.listen_port = 27020;
    wire_config.listen_bind = config.api.bind;
    wire_config.clickhouse_routing = true;
    auto wire_proxy = std::make_shared<mongoflux::WireProxy>(config, wire_config, registry, ch_client, translator);

    // Set up signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Start sync based on configured mode
    if (config.sync.mode == "oplog") {
        std::cout << "[mongoflux] Starting oplog sync (direct tailing, like a secondary node)..." << std::endl;
        oplog_sync->start();
    } else {
        std::cout << "[mongoflux] Starting change stream sync..." << std::endl;
        cs_sync->start();
    }

    std::cout << "[mongoflux] Starting management API on port " << config.api.port << std::endl;
    api->start();

    std::cout << "[mongoflux] Starting wire protocol proxy on port " << wire_config.listen_port << std::endl;
    wire_proxy->start();

    std::cout << "[mongoflux] MongoFlux is running. Press Ctrl+C to stop." << std::endl;
    std::cout << "[mongoflux] Connect your app: mongodb://localhost:" << wire_config.listen_port << "/" << config.mongo.database << std::endl;

    // Main loop
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Graceful shutdown
    std::cout << "\n[mongoflux] Shutting down..." << std::endl;

    api->stop();
    wire_proxy->stop();
    oplog_sync->stop();
    cs_sync->stop();

    // Persist mappings
    try {
        registry->save_to_file(mappings_file);
    } catch (const std::exception& e) {
        std::cerr << "[mongoflux] Warning: Failed to save mappings: " << e.what() << std::endl;
    }

    std::cout << "[mongoflux] Shutdown complete." << std::endl;
    return 0;
}
