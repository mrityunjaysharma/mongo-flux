#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "schema_mapping.h"

namespace mg_clickhouse {

class ClickHouseClient;

/**
 * Real-time CDC sync from MongoDB change streams to ClickHouse.
 *
 * For each mapped collection, opens a change stream and batches inserts
 * into ClickHouse according to the configured batch_size and flush_interval.
 */
class ChangeStreamSync {
public:
    ChangeStreamSync(const Config& config,
                     std::shared_ptr<SchemaMappingRegistry> registry,
                     std::shared_ptr<ClickHouseClient> ch_client);
    ~ChangeStreamSync();

    /** Start syncing all mapped collections. Non-blocking. */
    void start();

    /** Stop all sync threads gracefully. */
    void stop();

    /** Check if sync is running. */
    bool is_running() const { return running_.load(); }

    /** Restart sync for a specific collection (e.g., after mapping change). */
    void restart_collection(const std::string& collection);

private:
    void sync_collection(const std::string& collection);
    void flush_batch(const std::string& collection,
                     const CollectionMapping& mapping,
                     std::vector<nlohmann::json>& batch);
    void save_resume_token(const std::string& collection, const std::string& token);
    std::string load_resume_token(const std::string& collection);

    Config config_;
    std::shared_ptr<SchemaMappingRegistry> registry_;
    std::shared_ptr<ClickHouseClient> ch_client_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> sync_threads_;
};

} // namespace mg_clickhouse
