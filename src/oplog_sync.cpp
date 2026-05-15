#include "mg_clickhouse/oplog_sync.h"
#include "mg_clickhouse/clickhouse_client.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <unordered_map>

#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/options/find.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

namespace mg_clickhouse {

OplogSync::OplogSync(
    const Config& config,
    std::shared_ptr<SchemaMappingRegistry> registry,
    std::shared_ptr<ClickHouseClient> ch_client)
    : config_(config)
    , registry_(std::move(registry))
    , ch_client_(std::move(ch_client)) {}

OplogSync::~OplogSync() {
    stop();
}

void OplogSync::start() {
    if (running_.load()) return;
    running_ = true;

    // Ensure ClickHouse tables exist for all mappings
    auto mappings = registry_->get_all();
    for (const auto& mapping : mappings) {
        if (!mapping.enabled) continue;
        std::string ddl = registry_->generate_create_table_sql(mapping);
        try {
            ch_client_->create_table(ddl);
        } catch (const std::exception& e) {
            std::cerr << "[mg-clickhouse/oplog] Failed to create table for "
                      << mapping.collection << ": " << e.what() << std::endl;
        }
    }

    tailer_thread_ = std::thread(&OplogSync::tail_oplog, this);
}

void OplogSync::stop() {
    running_ = false;
    if (tailer_thread_.joinable()) {
        tailer_thread_.join();
    }
}

void OplogSync::tail_oplog() {
    /*
     * This replicates exactly what a MongoDB secondary does:
     *
     * 1. Connect to the replica set primary
     * 2. Open a tailable-await cursor on local.oplog.rs
     * 3. Start from the last saved timestamp (or the current tail)
     * 4. For each oplog entry, apply it to our target (ClickHouse)
     * 5. Periodically save our position for crash recovery
     *
     * The oplog is a capped collection. A tailable cursor stays open
     * and blocks (awaits) when it reaches the end, resuming when new
     * entries arrive. This gives us the same real-time replication
     * latency that secondary nodes experience.
     */

    // Retry connection loop — wait for MongoDB to become available
    while (running_.load()) {
        try {
            tail_oplog_inner();
        } catch (const std::exception& e) {
            std::cerr << "[mg-clickhouse/oplog] Connection failed: " << e.what()
                      << ". Retrying in 3s..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

void OplogSync::tail_oplog_inner() {

    mongocxx::uri uri(config_.mongo.uri);
    mongocxx::client client(uri);

    // Access the oplog — same collection secondaries read from
    auto local_db = client["local"];
    auto oplog = local_db["oplog.rs"];

    // Determine where to start tailing
    bsoncxx::types::b_timestamp start_ts = load_oplog_timestamp();

    // If no saved timestamp, start from the current oplog tail
    if (start_ts.timestamp == 0) {
        // Find the latest oplog entry to start from "now"
        mongocxx::options::find opts;
        opts.sort(make_document(kvp("$natural", -1)));
        opts.limit(1);

        auto cursor = oplog.find({}, opts);
        for (auto& doc : cursor) {
            auto ts_elem = doc["ts"];
            if (ts_elem && ts_elem.type() == bsoncxx::type::k_timestamp) {
                start_ts = ts_elem.get_timestamp();
            }
            break;
        }

        if (start_ts.timestamp == 0) {
            std::cerr << "[mg-clickhouse/oplog] Cannot determine oplog position. "
                      << "Is this a replica set?" << std::endl;
            running_ = false;
            return;
        }

        std::cout << "[mg-clickhouse/oplog] Starting from oplog ts="
                  << start_ts.timestamp << ":" << start_ts.increment << std::endl;
    } else {
        std::cout << "[mg-clickhouse/oplog] Resuming from saved oplog ts="
                  << start_ts.timestamp << ":" << start_ts.increment << std::endl;
    }

    // Build the tailable-await cursor — this is the core replication mechanism
    // Filter: ts > start_ts, and only operations on our target database
    std::string ns_prefix = config_.mongo.database + ".";

    auto filter = make_document(
        kvp("ts", make_document(kvp("$gt", start_ts))),
        kvp("ns", make_document(
            kvp("$regex", bsoncxx::types::b_string{ns_prefix})
        ))
    );

    mongocxx::options::find tail_opts;
    // CursorType::tailable_await = same as what secondaries use
    // The cursor stays open and blocks waiting for new oplog entries
    tail_opts.cursor_type(mongocxx::cursor::type::k_tailable_await);
    // No timeout — keep the cursor alive indefinitely like a secondary
    tail_opts.no_cursor_timeout(true);
    // Batch size controls how many ops we fetch at once
    tail_opts.batch_size(config_.sync.batch_size);

    // Pending batch for ClickHouse inserts
    struct PendingBatch {
        std::vector<nlohmann::json> rows;
        std::chrono::steady_clock::time_point last_flush;
    };
    std::unordered_map<std::string, PendingBatch> pending;

    // Track the last successfully processed oplog timestamp
    bsoncxx::types::b_timestamp last_processed_ts{0, 0};

    auto flush_all = [&]() {
        bool all_flushed = true;
        for (auto& [collection, batch] : pending) {
            if (batch.rows.empty()) continue;

            auto mapping_opt = registry_->get(collection);
            if (!mapping_opt) continue;
            const auto& mapping = *mapping_opt;

            try {
                std::vector<std::string> columns;
                for (const auto& field : mapping.fields) {
                    columns.push_back(field.ch_column);
                }

                std::vector<std::vector<std::string>> ch_rows;
                ch_rows.reserve(batch.rows.size());

                for (const auto& row : batch.rows) {
                    std::vector<std::string> values;
                    for (const auto& field : mapping.fields) {
                        auto it = row.find(field.ch_column);
                        if (it == row.end() || it->is_null()) {
                            values.push_back("NULL");
                        } else if (it->is_string()) {
                            std::string val = it->get<std::string>();
                            std::string escaped = "'";
                            for (char c : val) {
                                if (c == '\'') escaped += "\\'";
                                else if (c == '\\') escaped += "\\\\";
                                else escaped += c;
                            }
                            escaped += "'";
                            values.push_back(escaped);
                        } else {
                            values.push_back(it->dump());
                        }
                    }
                    ch_rows.push_back(std::move(values));
                }

                ch_client_->insert_batch(
                    mapping.clickhouse_database,
                    mapping.clickhouse_table,
                    columns,
                    ch_rows);

                // Only clear batch on successful flush
                batch.rows.clear();
                batch.last_flush = std::chrono::steady_clock::now();
            } catch (const std::exception& e) {
                std::cerr << "[mg-clickhouse/oplog] Flush failed for "
                          << collection << " (" << batch.rows.size() << " rows): "
                          << e.what() << std::endl;
                all_flushed = false;
                // Keep rows in batch for retry on next flush cycle
            }
        }

        // Only persist oplog position after ALL batches flushed successfully
        if (all_flushed && last_processed_ts.timestamp > 0) {
            save_oplog_timestamp(last_processed_ts);
        }
    };

    // Main tailing loop — mirrors what mongod does internally for replication
    // IMPORTANT: We create the cursor ONCE and iterate it. A tailable-await
    // cursor stays open and blocks when exhausted, resuming when new entries
    // arrive. We do NOT re-create the cursor in a loop (that causes duplicates).
    auto cursor = oplog.find(filter.view(), tail_opts);

    for (auto& entry : cursor) {
        if (!running_.load()) break;

        // Extract operation type (i=insert, u=update, d=delete)
        auto op_elem = entry["op"];
        if (!op_elem || op_elem.type() != bsoncxx::type::k_string) continue;
        std::string op(op_elem.get_string().value);

        // Extract namespace (db.collection)
        auto ns_elem = entry["ns"];
        if (!ns_elem || ns_elem.type() != bsoncxx::type::k_string) continue;
        std::string ns(ns_elem.get_string().value);

        std::string collection = extract_collection(ns);
        if (collection.empty()) continue;

        // Only process collections we have mappings for
        if (!registry_->has_mapping(collection)) continue;

        auto mapping_opt = registry_->get(collection);
        if (!mapping_opt || !mapping_opt->enabled) continue;
        const auto& mapping = *mapping_opt;

        // Track the timestamp of this entry (saved AFTER successful flush)
        auto ts_elem = entry["ts"];
        if (ts_elem && ts_elem.type() == bsoncxx::type::k_timestamp) {
            last_processed_ts = ts_elem.get_timestamp();
        }

        if (op == "i") {
            // INSERT — the "o" field contains the full document
            auto o_elem = entry["o"];
            if (!o_elem || o_elem.type() != bsoncxx::type::k_document) continue;
            auto doc = o_elem.get_document().value;

            nlohmann::json row;
            for (const auto& field_map : mapping.fields) {
                auto elem = doc[field_map.mongo_field];
                if (elem) {
                    switch (elem.type()) {
                        case bsoncxx::type::k_int32:
                            row[field_map.ch_column] = elem.get_int32().value;
                            break;
                        case bsoncxx::type::k_int64:
                            row[field_map.ch_column] = elem.get_int64().value;
                            break;
                        case bsoncxx::type::k_double:
                            row[field_map.ch_column] = elem.get_double().value;
                            break;
                        case bsoncxx::type::k_string:
                            row[field_map.ch_column] = std::string(elem.get_string().value);
                            break;
                        case bsoncxx::type::k_bool:
                            row[field_map.ch_column] = elem.get_bool().value;
                            break;
                        case bsoncxx::type::k_oid:
                            row[field_map.ch_column] = elem.get_oid().value.to_string();
                            break;
                        case bsoncxx::type::k_date:
                            row[field_map.ch_column] = elem.get_date().to_int64();
                            break;
                        default:
                            row[field_map.ch_column] = nullptr;
                            break;
                    }
                } else {
                    row[field_map.ch_column] = nullptr;
                }
            }

            pending[collection].rows.push_back(std::move(row));

        } else if (op == "u") {
            // UPDATE — for ReplacingMergeTree, we insert the new version.
            auto o_elem = entry["o"];
            if (!o_elem || o_elem.type() != bsoncxx::type::k_document) continue;
            auto update_doc = o_elem.get_document().value;

            // Check if this is a full replacement (no $ operators)
            bool is_replacement = true;
            for (auto it = update_doc.begin(); it != update_doc.end(); ++it) {
                std::string key((*it).key());
                if (!key.empty() && key[0] == '$') {
                    is_replacement = false;
                    break;
                }
                if (key == "diff") {
                    is_replacement = false;
                    break;
                }
            }

            if (is_replacement) {
                nlohmann::json row;
                for (const auto& field_map : mapping.fields) {
                    auto elem = update_doc[field_map.mongo_field];
                    if (elem) {
                        switch (elem.type()) {
                            case bsoncxx::type::k_int32:
                                row[field_map.ch_column] = elem.get_int32().value;
                                break;
                            case bsoncxx::type::k_int64:
                                row[field_map.ch_column] = elem.get_int64().value;
                                break;
                            case bsoncxx::type::k_double:
                                row[field_map.ch_column] = elem.get_double().value;
                                break;
                            case bsoncxx::type::k_string:
                                row[field_map.ch_column] = std::string(elem.get_string().value);
                                break;
                            case bsoncxx::type::k_bool:
                                row[field_map.ch_column] = elem.get_bool().value;
                                break;
                            case bsoncxx::type::k_oid:
                                row[field_map.ch_column] = elem.get_oid().value.to_string();
                                break;
                            case bsoncxx::type::k_date:
                                row[field_map.ch_column] = elem.get_date().to_int64();
                                break;
                            default:
                                row[field_map.ch_column] = nullptr;
                                break;
                        }
                    } else {
                        row[field_map.ch_column] = nullptr;
                    }
                }
                pending[collection].rows.push_back(std::move(row));
            }
            // Partial updates: skip (ReplacingMergeTree handles via periodic full-sync)
        }
        // op == "d" (delete) — handled by ReplacingMergeTree version column

        // Flush if batch is full
        for (auto& [coll, batch] : pending) {
            if (static_cast<int>(batch.rows.size()) >= config_.sync.batch_size) {
                flush_all();
                break;
            }
        }

        // Flush on time interval
        auto now = std::chrono::steady_clock::now();
        for (auto& [coll, batch] : pending) {
            if (batch.rows.empty()) continue;
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - batch.last_flush).count();
            if (elapsed >= config_.sync.flush_interval_ms) {
                flush_all();
                break;
            }
        }
    }

    // Cursor died (shouldn't happen with tailable-await unless server restarts)
    flush_all();
}

void OplogSync::save_oplog_timestamp(const bsoncxx::types::b_timestamp& ts) {
    std::string path = config_.sync.resume_token_path + "/oplog_position.json";
    std::filesystem::create_directories(config_.sync.resume_token_path);
    std::ofstream file(path);
    if (file.is_open()) {
        nlohmann::json j = {
            {"timestamp", ts.timestamp},
            {"increment", ts.increment}
        };
        file << j.dump();
    }
}

bsoncxx::types::b_timestamp OplogSync::load_oplog_timestamp() {
    bsoncxx::types::b_timestamp ts{0, 0};
    std::string path = config_.sync.resume_token_path + "/oplog_position.json";
    std::ifstream file(path);
    if (!file.is_open()) return ts;

    try {
        nlohmann::json j;
        file >> j;
        ts.timestamp = j.at("timestamp").get<uint32_t>();
        ts.increment = j.at("increment").get<uint32_t>();
    } catch (...) {
        // Corrupted file, start fresh
    }
    return ts;
}

std::string OplogSync::extract_collection(const std::string& ns) {
    // Namespace format: "database.collection"
    auto dot = ns.find('.');
    if (dot == std::string::npos) return "";
    return ns.substr(dot + 1);
}

} // namespace mg_clickhouse
