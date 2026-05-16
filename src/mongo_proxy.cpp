#include "mongoflux/mongo_proxy.h"
#include "mongoflux/routing.h"

#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/options/find.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/array/view.hpp>

namespace mongoflux {

MongoProxy::MongoProxy(
    const Config& config,
    std::shared_ptr<SchemaMappingRegistry> registry,
    std::shared_ptr<ClickHouseClient> ch_client,
    std::shared_ptr<QueryTranslator> translator)
    : config_(config)
    , registry_(std::move(registry))
    , ch_client_(std::move(ch_client))
    , translator_(std::move(translator)) {}

MongoProxy::~MongoProxy() = default;

bool MongoProxy::should_route_to_clickhouse(const std::string& uri) const {
    ParsedUri parsed = parse_mongo_uri(uri);
    return has_clickhouse_routing(parsed, config_.routing.clickhouse_param);
}

nlohmann::json MongoProxy::execute_find(
    const std::string& uri,
    const std::string& database,
    const std::string& collection,
    const bsoncxx::document::view& filter,
    const bsoncxx::document::view& projection,
    const bsoncxx::document::view& sort,
    int64_t limit,
    int64_t skip) {

    if (should_route_to_clickhouse(uri) && registry_->has_mapping(collection)) {
        // Route to ClickHouse
        std::string sql = translator_->translate_find(
            collection, filter, projection, sort, limit, skip);

        QueryResult result = ch_client_->query(sql);

        // Convert to JSON array (already in JSON format from ClickHouse)
        nlohmann::json docs = nlohmann::json::array();
        for (auto& row : result.rows) {
            docs.push_back(std::move(row));
        }
        return docs;
    }

    // Route to MongoDB
    mongocxx::client client{mongocxx::uri{config_.mongo.uri}};
    auto db = client[database];
    auto coll = db[collection];

    mongocxx::options::find opts;
    if (!projection.empty()) opts.projection(projection);
    if (!sort.empty()) opts.sort(sort);
    if (limit > 0) opts.limit(limit);
    if (skip > 0) opts.skip(skip);

    auto cursor = coll.find(filter, opts);

    nlohmann::json docs = nlohmann::json::array();
    for (auto& doc : cursor) {
        docs.push_back(nlohmann::json::parse(bsoncxx::to_json(doc)));
    }
    return docs;
}

nlohmann::json MongoProxy::execute_aggregate(
    const std::string& uri,
    const std::string& database,
    const std::string& collection,
    const std::vector<bsoncxx::document::view>& pipeline) {

    if (should_route_to_clickhouse(uri) && registry_->has_mapping(collection)) {
        // Route to ClickHouse
        std::string sql = translator_->translate_aggregate(collection, pipeline);

        QueryResult result = ch_client_->query(sql);

        nlohmann::json docs = nlohmann::json::array();
        for (auto& row : result.rows) {
            docs.push_back(std::move(row));
        }
        return docs;
    }

    // Route to MongoDB
    mongocxx::client client{mongocxx::uri{config_.mongo.uri}};
    auto db = client[database];
    auto coll = db[collection];

    mongocxx::pipeline mongo_pipeline;
    // Note: In a full implementation, each stage would be appended properly.
    // For now, use raw pipeline execution via the aggregate command.

    nlohmann::json docs = nlohmann::json::array();
    // Simplified: would need to build the pipeline from the views
    return docs;
}

} // namespace mongoflux
