#pragma once

#include <memory>
#include <string>

#include "config.h"
#include "query_translator.h"
#include "clickhouse_client.h"
#include "schema_mapping.h"

namespace mongoflux {

/**
 * MongoDB proxy that intercepts read operations and optionally routes
 * them to ClickHouse based on the connection URI parameters.
 *
 * When the URI contains the configured clickhouse_param (e.g., "clickhouse=true"),
 * find() and aggregate() operations on mapped collections are translated
 * to ClickHouse SQL and executed there. Results are converted back to
 * BSON documents for the client.
 *
 * Writes always go to MongoDB directly.
 */
class MongoProxy {
public:
    MongoProxy(const Config& config,
               std::shared_ptr<SchemaMappingRegistry> registry,
               std::shared_ptr<ClickHouseClient> ch_client,
               std::shared_ptr<QueryTranslator> translator);
    ~MongoProxy();

    /**
     * Determine if a connection URI indicates ClickHouse routing.
     */
    bool should_route_to_clickhouse(const std::string& uri) const;

    /**
     * Execute a find() query, routing to ClickHouse if appropriate.
     * Returns results as JSON array.
     */
    nlohmann::json execute_find(const std::string& uri,
                                const std::string& database,
                                const std::string& collection,
                                const bsoncxx::document::view& filter,
                                const bsoncxx::document::view& projection,
                                const bsoncxx::document::view& sort,
                                int64_t limit = 0,
                                int64_t skip = 0);

    /**
     * Execute an aggregate() pipeline, routing to ClickHouse if appropriate.
     * Returns results as JSON array.
     */
    nlohmann::json execute_aggregate(
        const std::string& uri,
        const std::string& database,
        const std::string& collection,
        const std::vector<bsoncxx::document::view>& pipeline);

private:
    Config config_;
    std::shared_ptr<SchemaMappingRegistry> registry_;
    std::shared_ptr<ClickHouseClient> ch_client_;
    std::shared_ptr<QueryTranslator> translator_;
};

} // namespace mongoflux
