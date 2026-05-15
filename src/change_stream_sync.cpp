#include "mg_clickhouse/change_stream_sync.h"
#include "mg_clickhouse/clickhouse_client.h"
#include "mg_clickhouse/bson_utils.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <filesystem>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/change_stream.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

namespace mg_clickhouse {

ChangeStreamSync::ChangeStreamSync(
    const Config& config,
    std::shared_ptr<SchemaMappingRegistry> registry,
    std::shared_ptr<ClickHouseClient> ch_client)
    : config_(config)
    , registry_(std::move(registry))
    , ch_client_(std::move(ch_client)) {}

ChangeStreamSync::~ChangeStreamSync() {
    stop();
}

void ChangeStreamSync::start() {
    if (running_.load()) return;
    running_ = true;

    auto mappings = registry_->get_all();
    for (const auto& mapping : mappings) {
        if (!mapping.enabled) continue;

        // Ensure ClickHouse table exists
        std::string ddl = registry_->generate_create_table_sql(mapping);
        try {
            ch_client_->create_table(ddl);
        } catch (const std::exception& e) {
            std::cerr << "[mg-clickhouse] Failed to create table for "
                      << mapping.collection << ": " << e.what() << std::endl;
        }

        sync_threads_.emplace_back(&ChangeStreamSync::sync_collection, this, mapping.collection);
    }
}

void ChangeStreamSync::stop() {
    running_ = false;
    for (auto& t : sync_threads_) {
        if (t.joinable()) t.join();
    }
    sync_threads_.clear();
}

void ChangeStreamSync::restart_collection(const std::string& collection) {
    // Simple implementation: stop all and restart
    // A production version would track per-collection threads
    stop();
    start();
}

void ChangeStreamSync::sync_collection(const std::string& collection) {
    mongocxx::uri uri(config_.mongo.uri);
    mongocxx::client client(uri);
    auto db = client[config_.mongo.database];
    auto coll = db[collection];

    auto mapping_opt = registry_->get(collection);
    if (!mapping_opt) return;
    const auto& mapping = *mapping_opt;

    // Set up change stream options
    mongocxx::options::change_stream cs_opts;

    // Resume from saved token if available
    std::string resume_token = load_resume_token(collection);
    if (!resume_token.empty()) {
        try {
            auto token_doc = bsoncxx::from_json(resume_token);
            cs_opts.resume_after(token_doc.view());
        } catch (...) {
            // Invalid token, start fresh
        }
    }

    // Watch for insert, update, replace operations
    mongocxx::change_stream stream = coll.watch(cs_opts);

    std::vector<nlohmann::json> batch;
    auto last_flush = std::chrono::steady_clock::now();

    while (running_.load()) {
        // Try to get next change event
        for (auto& event : stream) {
            if (!running_.load()) break;

            std::string op_type;
            auto op_elem = event["operationType"];
            if (op_elem && op_elem.type() == bsoncxx::type::k_string) {
                op_type = std::string(op_elem.get_string().value);
            }

            // We sync inserts, updates, and replaces
            if (op_type == "insert" || op_type == "update" || op_type == "replace") {
                bsoncxx::document::view full_doc;

                if (op_type == "insert" || op_type == "replace") {
                    auto fd = event["fullDocument"];
                    if (fd && fd.type() == bsoncxx::type::k_document) {
                        full_doc = fd.get_document().value;
                    }
                } else if (op_type == "update") {
                    // For updates, we need fullDocument (requires fullDocument: "updateLookup")
                    auto fd = event["fullDocument"];
                    if (fd && fd.type() == bsoncxx::type::k_document) {
                        full_doc = fd.get_document().value;
                    } else {
                        continue; // Can't sync partial updates without full document
                    }
                }

                if (full_doc.empty()) continue;

                // Extract mapped fields using shared utility
                batch.push_back(extract_mapped_fields(full_doc, mapping));
            }

            // Save resume token
            auto token_elem = event["_id"];
            if (token_elem && token_elem.type() == bsoncxx::type::k_document) {
                save_resume_token(collection, bsoncxx::to_json(token_elem.get_document().value));
            }

            // Flush if batch is full
            if (static_cast<int>(batch.size()) >= config_.sync.batch_size) {
                flush_batch(collection, mapping, batch);
                last_flush = std::chrono::steady_clock::now();
            }
        }

        // Flush on interval
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush).count();
        if (!batch.empty() && elapsed >= config_.sync.flush_interval_ms) {
            flush_batch(collection, mapping, batch);
            last_flush = now;
        }

        // Brief sleep to avoid busy-waiting when no events
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Final flush
    if (!batch.empty()) {
        flush_batch(collection, mapping, batch);
    }
}

void ChangeStreamSync::flush_batch(
    const std::string& collection,
    const CollectionMapping& mapping,
    std::vector<nlohmann::json>& batch) {

    if (batch.empty()) return;

    try {
        std::vector<std::string> columns;
        std::vector<std::vector<std::string>> rows;
        prepare_batch_for_insert(batch, mapping, columns, rows);

        ch_client_->insert_batch(mapping.clickhouse_database, mapping.clickhouse_table, columns, rows);
    } catch (const std::exception& e) {
        std::cerr << "[mg-clickhouse/cs] Failed to flush batch for " << collection
                  << " (" << batch.size() << " rows): " << e.what() << std::endl;
    }

    batch.clear();
}

void ChangeStreamSync::save_resume_token(const std::string& collection, const std::string& token) {
    std::string path = config_.sync.resume_token_path + "/" + collection + ".json";
    std::filesystem::create_directories(config_.sync.resume_token_path);
    std::ofstream file(path);
    if (file.is_open()) {
        file << token;
    }
}

std::string ChangeStreamSync::load_resume_token(const std::string& collection) {
    std::string path = config_.sync.resume_token_path + "/" + collection + ".json";
    std::ifstream file(path);
    if (!file.is_open()) return "";
    std::string token((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
    return token;
}

} // namespace mg_clickhouse
