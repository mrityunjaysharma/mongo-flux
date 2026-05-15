#pragma once

#include <string>
#include <vector>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/array/view.hpp>
#include <nlohmann/json.hpp>

#include "schema_mapping.h"

namespace mg_clickhouse {

/**
 * Translates MongoDB query operations into ClickHouse SQL.
 *
 * Supports:
 * - find() with filter, projection, sort, limit, skip
 * - aggregate() with $match, $group, $sort, $limit, $project, $unwind, $count
 */
class QueryTranslator {
public:
    explicit QueryTranslator(std::shared_ptr<SchemaMappingRegistry> registry);

    /**
     * Translate a MongoDB find() operation to ClickHouse SQL.
     *
     * @param collection  The MongoDB collection name
     * @param filter      The query filter document (BSON)
     * @param projection  Fields to include/exclude
     * @param sort        Sort specification
     * @param limit       Max documents to return (0 = no limit)
     * @param skip        Documents to skip (0 = none)
     * @return            ClickHouse SQL string
     */
    std::string translate_find(const std::string& collection,
                               const bsoncxx::document::view& filter,
                               const bsoncxx::document::view& projection,
                               const bsoncxx::document::view& sort,
                               int64_t limit = 0,
                               int64_t skip = 0) const;

    /**
     * Translate a MongoDB aggregate() pipeline to ClickHouse SQL.
     *
     * @param collection  The MongoDB collection name
     * @param pipeline    Array of pipeline stage documents
     * @return            ClickHouse SQL string
     */
    std::string translate_aggregate(
        const std::string& collection,
        const std::vector<bsoncxx::document::view>& pipeline) const;

private:
    /** Translate a BSON filter to a SQL WHERE clause. */
    std::string translate_filter(const bsoncxx::document::view& filter,
                                 const CollectionMapping& mapping) const;

    /** Translate a single filter expression. */
    std::string translate_expression(const std::string& field,
                                     const bsoncxx::document::element& value,
                                     const CollectionMapping& mapping) const;

    /** Translate comparison operators ($gt, $lt, $gte, $lte, $eq, $ne). */
    std::string translate_comparison(const std::string& ch_column,
                                     const std::string& op,
                                     const bsoncxx::document::element& value) const;

    /** Translate $in / $nin operators. */
    std::string translate_in(const std::string& ch_column,
                             const bsoncxx::array::view& values,
                             bool negate) const;

    /** Translate $and / $or / $not logical operators. */
    std::string translate_logical(const std::string& op,
                                  const bsoncxx::array::view& conditions,
                                  const CollectionMapping& mapping) const;

    /** Translate a $group stage. */
    std::string translate_group_stage(const bsoncxx::document::view& stage,
                                      const CollectionMapping& mapping) const;

    /** Translate an accumulator ($sum, $avg, $min, $max, $count). */
    std::string translate_accumulator(const std::string& acc_op,
                                      const bsoncxx::document::element& value,
                                      const CollectionMapping& mapping) const;

    /** Map a MongoDB field name to its ClickHouse column name. */
    std::string resolve_column(const std::string& mongo_field,
                               const CollectionMapping& mapping) const;

    /** Quote a ClickHouse identifier. */
    static std::string quote_ident(const std::string& ident);

    /** Escape a string literal for ClickHouse. */
    static std::string quote_literal(const std::string& value);

    /** Convert a BSON value to a SQL literal. */
    std::string bson_to_literal(const bsoncxx::document::element& elem) const;

    std::shared_ptr<SchemaMappingRegistry> registry_;
};

} // namespace mg_clickhouse
