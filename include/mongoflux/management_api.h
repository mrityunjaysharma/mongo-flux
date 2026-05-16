#pragma once

#include <memory>
#include <atomic>
#include <thread>

#include "config.h"
#include "schema_mapping.h"
#include "clickhouse_client.h"
#include "change_stream_sync.h"
#include "oplog_sync.h"

namespace mongoflux {

/**
 * HTTP management API for schema mappings and system status.
 *
 * Endpoints:
 *   GET    /api/v1/mappings          - List all mappings
 *   GET    /api/v1/mappings/:name    - Get mapping by collection name
 *   POST   /api/v1/mappings          - Create/update a mapping
 *   DELETE /api/v1/mappings/:name    - Delete a mapping
 *   POST   /api/v1/mappings/:name/sync - Trigger table creation in ClickHouse
 *   GET    /api/v1/status            - System health and sync status
 *   POST   /api/v1/sync/restart      - Restart all sync threads
 *   GET    /health                   - Liveness probe (returns 200)
 *   GET    /ready                    - Readiness probe (checks dependencies)
 */
class ManagementApi {
public:
    ManagementApi(const ApiConfig& config,
                  std::shared_ptr<SchemaMappingRegistry> registry,
                  std::shared_ptr<ClickHouseClient> ch_client,
                  std::shared_ptr<ChangeStreamSync> cs_sync,
                  std::shared_ptr<OplogSync> oplog_sync);
    ~ManagementApi();

    /** Start the HTTP server. Non-blocking. */
    void start();

    /** Stop the HTTP server. */
    void stop();

private:
    void run();

    ApiConfig config_;
    std::shared_ptr<SchemaMappingRegistry> registry_;
    std::shared_ptr<ClickHouseClient> ch_client_;
    std::shared_ptr<ChangeStreamSync> cs_sync_;
    std::shared_ptr<OplogSync> oplog_sync_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
};

} // namespace mongoflux
