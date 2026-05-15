#include "mg_clickhouse/management_api.h"

#include <httplib.h>
#include <nlohmann/json.hpp>
#include <iostream>

namespace mg_clickhouse {

ManagementApi::ManagementApi(
    const ApiConfig& config,
    std::shared_ptr<SchemaMappingRegistry> registry,
    std::shared_ptr<ClickHouseClient> ch_client,
    std::shared_ptr<ChangeStreamSync> cs_sync,
    std::shared_ptr<OplogSync> oplog_sync)
    : config_(config)
    , registry_(std::move(registry))
    , ch_client_(std::move(ch_client))
    , cs_sync_(std::move(cs_sync))
    , oplog_sync_(std::move(oplog_sync)) {}

ManagementApi::~ManagementApi() {
    stop();
}

void ManagementApi::start() {
    if (running_.load()) return;
    running_ = true;
    server_thread_ = std::thread(&ManagementApi::run, this);
}

void ManagementApi::stop() {
    running_ = false;
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

void ManagementApi::run() {
    httplib::Server svr;

    // GET /api/v1/mappings - List all mappings
    svr.Get("/api/v1/mappings", [this](const httplib::Request&, httplib::Response& res) {
        auto mappings = registry_->get_all();
        nlohmann::json j = mappings;
        res.set_content(j.dump(2), "application/json");
    });

    // GET /api/v1/mappings/:name - Get mapping by collection name
    svr.Get(R"(/api/v1/mappings/([^/]+))",
        [this](const httplib::Request& req, httplib::Response& res) {
            std::string collection = req.matches[1];
            auto mapping = registry_->get(collection);
            if (!mapping) {
                res.status = 404;
                nlohmann::json err = {{"error", "Mapping not found for collection: " + collection}};
                res.set_content(err.dump(), "application/json");
                return;
            }
            nlohmann::json j = *mapping;
            res.set_content(j.dump(2), "application/json");
        });

    // POST /api/v1/mappings - Create or update a mapping
    svr.Post("/api/v1/mappings", [this](const httplib::Request& req, httplib::Response& res) {
        try {
            auto j = nlohmann::json::parse(req.body);
            CollectionMapping mapping = j.get<CollectionMapping>();

            if (mapping.collection.empty()) {
                res.status = 400;
                nlohmann::json err = {{"error", "collection field is required"}};
                res.set_content(err.dump(), "application/json");
                return;
            }

            if (mapping.fields.empty()) {
                res.status = 400;
                nlohmann::json err = {{"error", "fields array must not be empty"}};
                res.set_content(err.dump(), "application/json");
                return;
            }

            bool created = registry_->upsert(mapping);

            nlohmann::json response = {
                {"status", created ? "created" : "updated"},
                {"collection", mapping.collection},
                {"clickhouse_table", mapping.clickhouse_table}
            };

            res.status = created ? 201 : 200;
            res.set_content(response.dump(2), "application/json");
        } catch (const nlohmann::json::exception& e) {
            res.status = 400;
            nlohmann::json err = {{"error", std::string("Invalid JSON: ") + e.what()}};
            res.set_content(err.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            nlohmann::json err = {{"error", e.what()}};
            res.set_content(err.dump(), "application/json");
        }
    });

    // DELETE /api/v1/mappings/:name - Delete a mapping
    svr.Delete(R"(/api/v1/mappings/([^/]+))",
        [this](const httplib::Request& req, httplib::Response& res) {
            std::string collection = req.matches[1];
            bool removed = registry_->remove(collection);
            if (!removed) {
                res.status = 404;
                nlohmann::json err = {{"error", "Mapping not found for collection: " + collection}};
                res.set_content(err.dump(), "application/json");
                return;
            }
            nlohmann::json response = {{"status", "deleted"}, {"collection", collection}};
            res.set_content(response.dump(2), "application/json");
        });

    // POST /api/v1/mappings/:name/sync - Create ClickHouse table for mapping
    svr.Post(R"(/api/v1/mappings/([^/]+)/sync)",
        [this](const httplib::Request& req, httplib::Response& res) {
            std::string collection = req.matches[1];
            auto mapping = registry_->get(collection);
            if (!mapping) {
                res.status = 404;
                nlohmann::json err = {{"error", "Mapping not found for collection: " + collection}};
                res.set_content(err.dump(), "application/json");
                return;
            }

            try {
                std::string ddl = registry_->generate_create_table_sql(*mapping);
                ch_client_->create_table(ddl);

                // Restart sync for this collection
                sync_->restart_collection(collection);

                nlohmann::json response = {
                    {"status", "synced"},
                    {"collection", collection},
                    {"ddl", ddl}
                };
                res.set_content(response.dump(2), "application/json");
            } catch (const std::exception& e) {
                res.status = 500;
                nlohmann::json err = {{"error", e.what()}};
                res.set_content(err.dump(), "application/json");
            }
        });

    // GET /api/v1/status - System health
    svr.Get("/api/v1/status", [this](const httplib::Request&, httplib::Response& res) {
        nlohmann::json status;
        status["oplog_sync_running"] = oplog_sync_ ? oplog_sync_->is_running() : false;
        status["changestream_sync_running"] = cs_sync_ ? cs_sync_->is_running() : false;
        status["mappings_count"] = registry_->get_all().size();

        try {
            ch_client_->ping();
            status["clickhouse"] = "connected";
        } catch (...) {
            status["clickhouse"] = "disconnected";
        }

        res.set_content(status.dump(2), "application/json");
    });

    // POST /api/v1/sync/restart - Restart all sync threads
    svr.Post("/api/v1/sync/restart", [this](const httplib::Request&, httplib::Response& res) {
        try {
            if (oplog_sync_) {
                oplog_sync_->stop();
                oplog_sync_->start();
            }
            if (cs_sync_) {
                cs_sync_->stop();
                cs_sync_->start();
            }
            nlohmann::json response = {{"status", "restarted"}};
            res.set_content(response.dump(2), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            nlohmann::json err = {{"error", e.what()}};
            res.set_content(err.dump(), "application/json");
        }
    });

    // GET /health - Kubernetes liveness probe
    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content(R"({"status":"ok"})", "application/json");
    });

    // GET /ready - Kubernetes readiness probe
    svr.Get("/ready", [this](const httplib::Request&, httplib::Response& res) {
        try {
            ch_client_->ping();
            res.set_content(R"({"status":"ready"})", "application/json");
        } catch (...) {
            res.status = 503;
            res.set_content(R"({"status":"not_ready","reason":"clickhouse_unavailable"})", "application/json");
        }
    });

    std::cout << "[mg-clickhouse] Management API listening on "
              << config_.bind << ":" << config_.port << std::endl;

    svr.listen(config_.bind, config_.port);
}

} // namespace mg_clickhouse
