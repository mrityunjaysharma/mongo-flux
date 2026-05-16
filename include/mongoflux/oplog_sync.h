#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>

#include "config.h"
#include "schema_mapping.h"

namespace mongoflux {

class ClickHouseClient;

/**
 * Oplog-based sync: replicates data to ClickHouse using the same mechanism
 * MongoDB uses to replicate to secondary nodes.
 *
 * MongoDB secondaries work by:
 *   1. Connecting to the primary's `local.oplog.rs` capped collection
 *   2. Tailing it with a tailable-await cursor
 *   3. Applying each operation (insert/update/delete) to their local storage
 *
 * OplogSync does exactly the same thing, but instead of applying ops to a
 * local MongoDB storage engine, it translates them into ClickHouse inserts.
 * This makes ClickHouse behave like a "virtual secondary" that receives the
 * same write stream as real secondaries.
 *
 * Comparison with ChangeStreamSync:
 *   - OplogSync: lower latency, less overhead, requires direct oplog access
 *   - ChangeStreamSync: higher-level API, works with Atlas/sharded clusters,
 *     supports resume tokens natively
 *
 * Use OplogSync when you have direct access to the replica set and want
 * minimal replication lag (same as secondary nodes experience).
 */
class OplogSync {
public:
    OplogSync(const Config& config,
              std::shared_ptr<SchemaMappingRegistry> registry,
              std::shared_ptr<ClickHouseClient> ch_client);
    ~OplogSync();

    /** Start tailing the oplog. Non-blocking. */
    void start();

    /** Stop the oplog tailer. */
    void stop();

    /** Check if sync is running. */
    bool is_running() const { return running_.load(); }

private:
    /**
     * Main oplog tailing loop. Connects to local.oplog.rs with a
     * tailable-await cursor, just like a MongoDB secondary does.
     */
    void tail_oplog();

    /** Inner tailing logic, called with retry from tail_oplog(). */
    void tail_oplog_inner();

    /**
     * Process a single oplog entry. Determines the operation type
     * (insert/update/delete) and routes to the appropriate handler.
     */
    void process_oplog_entry(const bsoncxx::document::view& entry);

    /** Handle an insert operation from the oplog. */
    void handle_insert(const std::string& ns,
                       const bsoncxx::document::view& doc);

    /** Handle an update operation from the oplog. */
    void handle_update(const std::string& ns,
                       const bsoncxx::document::view& doc,
                       const bsoncxx::document::view& filter);

    /**
     * Flush pending rows to ClickHouse.
     * Batches inserts for efficiency, same as how secondaries batch
     * oplog application.
     */
    void flush_pending();

    /** Persist the last processed oplog timestamp for crash recovery. */
    void save_oplog_timestamp(const bsoncxx::types::b_timestamp& ts);

    /** Load the last processed oplog timestamp. */
    bsoncxx::types::b_timestamp load_oplog_timestamp();

    /** Extract collection name from namespace string (db.collection). */
    static std::string extract_collection(const std::string& ns);

    Config config_;
    std::shared_ptr<SchemaMappingRegistry> registry_;
    std::shared_ptr<ClickHouseClient> ch_client_;
    std::atomic<bool> running_{false};
    std::thread tailer_thread_;
};

} // namespace mongoflux
