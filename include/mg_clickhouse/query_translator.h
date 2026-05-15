#pragma once

#include <string>
#include <vector>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/array/view.hpp>
#include <nlohmann/json.hpp>

#include "schema_mapping.h"
#include "expr_tree.h"

namespace mg_clickhouse {

/**
 * Translates MongoDB query operations into ClickHouse SQL.
 *
 * Architecture: two-phase translation via an expression tree (AST).
 *
 *   Phase 1 (Parse): BSON → ExprNode / QueryTree
 *   Phase 2 (Emit):  ExprNode / QueryTree → SQL string
 *
 * This separation enables:
 *   - Independent testing of parse and emit phases
 *   - Tree-level optimizations (constant folding, predicate pushdown)
 *   - Clear extension points for new operators
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
     */
    std::string translate_find(const std::string& collection,
                               const bsoncxx::document::view& filter,
                               const bsoncxx::document::view& projection,
                               const bsoncxx::document::view& sort,
                               int64_t limit = 0,
                               int64_t skip = 0) const;

    /**
     * Translate a MongoDB aggregate() pipeline to ClickHouse SQL.
     */
    std::string translate_aggregate(
        const std::string& collection,
        const std::vector<bsoncxx::document::view>& pipeline) const;

    // --- Tree-based API (for testing and advanced usage) ---

    /**
     * Parse a find() into a QueryTree without emitting SQL.
     */
    QueryTree parse_find(const std::string& collection,
                         const bsoncxx::document::view& filter,
                         const bsoncxx::document::view& projection,
                         const bsoncxx::document::view& sort,
                         int64_t limit = 0,
                         int64_t skip = 0) const;

    /**
     * Parse an aggregate() pipeline into a QueryTree without emitting SQL.
     */
    QueryTree parse_aggregate(
        const std::string& collection,
        const std::vector<bsoncxx::document::view>& pipeline) const;

    /**
     * Parse a BSON filter document into an expression tree node.
     */
    ExprNodePtr parse_filter(const bsoncxx::document::view& filter,
                             const CollectionMapping& mapping) const;

private:
    /** Parse a single field expression into an ExprNode. */
    ExprNodePtr parse_expression(const std::string& field,
                                 const bsoncxx::document::element& value,
                                 const CollectionMapping& mapping) const;

    /** Parse $and / $or / $nor into a logical node. */
    ExprNodePtr parse_logical(LogicOp op,
                              const bsoncxx::array::view& conditions,
                              const CollectionMapping& mapping) const;

    /** Parse $in / $nin into an InList node. */
    ExprNodePtr parse_in(const std::string& ch_column,
                         const bsoncxx::array::view& values,
                         bool negate) const;

    /** Parse a $group stage into select expressions and group-by columns. */
    void parse_group_stage(const bsoncxx::document::view& stage,
                           const CollectionMapping& mapping,
                           std::vector<ExprNodePtr>& select_exprs,
                           std::vector<ExprNodePtr>& group_by) const;

    /** Parse an accumulator ($sum, $avg, $min, $max, $count) into a function node. */
    ExprNodePtr parse_accumulator(const std::string& acc_op,
                                  const bsoncxx::document::element& value,
                                  const CollectionMapping& mapping) const;

    /** Parse an aggregation expression (arithmetic, string, date, conditional). */
    ExprNodePtr parse_agg_expression(const bsoncxx::document::element& expr,
                                     const CollectionMapping& mapping) const;

    /** Parse a field reference like "$amount" or a literal value. */
    ExprNodePtr parse_field_or_literal(const bsoncxx::document::element& elem,
                                       const CollectionMapping& mapping) const;

    /** Convert a BSON element to a Literal ExprNode. */
    ExprNodePtr bson_to_literal_node(const bsoncxx::document::element& elem) const;

    /** Convert a BSON array element to a Literal ExprNode. */
    ExprNodePtr bson_array_elem_to_literal(const bsoncxx::array::element& elem) const;

    /** Map a MongoDB field name to its ClickHouse column name. */
    std::string resolve_column(const std::string& mongo_field,
                               const CollectionMapping& mapping) const;

    /** Strip leading '$' from a MongoDB field reference. */
    static std::string strip_dollar(const std::string& field);

    std::shared_ptr<SchemaMappingRegistry> registry_;
};

} // namespace mg_clickhouse
