#include "mg_clickhouse/query_translator.h"

#include <sstream>
#include <stdexcept>
#include <bsoncxx/types.hpp>
#include <bsoncxx/array/view.hpp>
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/json.hpp>

namespace mg_clickhouse {

QueryTranslator::QueryTranslator(std::shared_ptr<SchemaMappingRegistry> registry)
    : registry_(std::move(registry)) {}

// ============================================================
// Public API — translate directly to SQL (convenience wrappers)
// ============================================================

std::string QueryTranslator::translate_find(
    const std::string& collection,
    const bsoncxx::document::view& filter,
    const bsoncxx::document::view& projection,
    const bsoncxx::document::view& sort,
    int64_t limit,
    int64_t skip) const {

    QueryTree tree = parse_find(collection, filter, projection, sort, limit, skip);
    return emit_query(tree);
}

std::string QueryTranslator::translate_aggregate(
    const std::string& collection,
    const std::vector<bsoncxx::document::view>& pipeline) const {

    QueryTree tree = parse_aggregate(collection, pipeline);
    return emit_query(tree);
}

// ============================================================
// Phase 1: Parse BSON → QueryTree
// ============================================================

QueryTree QueryTranslator::parse_find(
    const std::string& collection,
    const bsoncxx::document::view& filter,
    const bsoncxx::document::view& projection,
    const bsoncxx::document::view& sort,
    int64_t limit,
    int64_t skip) const {

    auto mapping_opt = registry_->get(collection);
    if (!mapping_opt) {
        throw std::runtime_error("No schema mapping found for collection: " + collection);
    }
    const auto& mapping = *mapping_opt;

    QueryTree tree;
    tree.from_database = mapping.clickhouse_database;
    tree.from_table = mapping.clickhouse_table;
    tree.limit = limit;
    tree.offset = skip;

    // SELECT columns
    if (projection.empty()) {
        for (const auto& field : mapping.fields) {
            tree.select_exprs.push_back(ExprNode::make_column(field.ch_column));
        }
    } else {
        for (auto it = projection.begin(); it != projection.end(); ++it) {
            auto elem = *it;
            if (elem.type() == bsoncxx::type::k_int32 && elem.get_int32().value == 1) {
                std::string col = resolve_column(std::string(elem.key()), mapping);
                tree.select_exprs.push_back(ExprNode::make_column(col));
            }
        }
    }

    // WHERE clause
    if (!filter.empty()) {
        tree.where_clause = parse_filter(filter, mapping);
    }

    // ORDER BY
    if (!sort.empty()) {
        for (auto it = sort.begin(); it != sort.end(); ++it) {
            auto elem = *it;
            std::string col = resolve_column(std::string(elem.key()), mapping);
            bool desc = (elem.type() == bsoncxx::type::k_int32 && elem.get_int32().value == -1);
            tree.order_by.emplace_back(ExprNode::make_column(col), desc);
        }
    }

    return tree;
}

QueryTree QueryTranslator::parse_aggregate(
    const std::string& collection,
    const std::vector<bsoncxx::document::view>& pipeline) const {

    auto mapping_opt = registry_->get(collection);
    if (!mapping_opt) {
        throw std::runtime_error("No schema mapping found for collection: " + collection);
    }
    const auto& mapping = *mapping_opt;

    QueryTree tree;
    tree.from_database = mapping.clickhouse_database;
    tree.from_table = mapping.clickhouse_table;

    bool has_group = false;

    for (const auto& stage_doc : pipeline) {
        for (auto it = stage_doc.begin(); it != stage_doc.end(); ++it) {
            auto elem = *it;
            std::string stage_name(elem.key());

            if (stage_name == "$match") {
                auto match_doc = elem.get_document().view();
                ExprNodePtr condition = parse_filter(match_doc, mapping);
                if (condition) {
                    if (!has_group) {
                        // Pre-group → WHERE
                        if (!tree.where_clause) {
                            tree.where_clause = condition;
                        } else {
                            tree.where_clause = ExprNode::make_logical(
                                LogicOp::AND, {tree.where_clause, condition});
                        }
                    } else {
                        // Post-group → HAVING
                        if (!tree.having_clause) {
                            tree.having_clause = condition;
                        } else {
                            tree.having_clause = ExprNode::make_logical(
                                LogicOp::AND, {tree.having_clause, condition});
                        }
                    }
                }

            } else if (stage_name == "$group") {
                has_group = true;
                auto group_doc = elem.get_document().view();
                parse_group_stage(group_doc, mapping, tree.select_exprs, tree.group_by);

            } else if (stage_name == "$sort") {
                auto sort_doc = elem.get_document().view();
                tree.order_by.clear();
                for (auto sit = sort_doc.begin(); sit != sort_doc.end(); ++sit) {
                    auto selem = *sit;
                    std::string col = resolve_column(std::string(selem.key()), mapping);
                    bool desc = (selem.type() == bsoncxx::type::k_int32 &&
                                 selem.get_int32().value == -1);
                    tree.order_by.emplace_back(ExprNode::make_column(col), desc);
                }

            } else if (stage_name == "$limit") {
                if (elem.type() == bsoncxx::type::k_int32) {
                    tree.limit = elem.get_int32().value;
                } else if (elem.type() == bsoncxx::type::k_int64) {
                    tree.limit = elem.get_int64().value;
                }

            } else if (stage_name == "$count") {
                std::string count_field(elem.get_string().value);
                auto count_expr = ExprNode::make_function("count", {
                    ExprNode::make_literal(std::string("*"))
                });
                tree.select_exprs.clear();
                tree.select_exprs.push_back(
                    ExprNode::make_select_expr(count_expr, count_field));

            } else if (stage_name == "$project") {
                if (tree.select_exprs.empty()) {
                    auto proj_doc = elem.get_document().view();
                    for (auto pit = proj_doc.begin(); pit != proj_doc.end(); ++pit) {
                        auto pelem = *pit;
                        if (pelem.type() == bsoncxx::type::k_int32 &&
                            pelem.get_int32().value == 1) {
                            std::string col = resolve_column(
                                std::string(pelem.key()), mapping);
                            tree.select_exprs.push_back(ExprNode::make_column(col));
                        }
                    }
                }
            }
        }
    }

    // Default SELECT if nothing was specified
    if (tree.select_exprs.empty()) {
        for (const auto& field : mapping.fields) {
            tree.select_exprs.push_back(ExprNode::make_column(field.ch_column));
        }
    }

    return tree;
}

// ============================================================
// Phase 1: Parse BSON filter → ExprNode tree
// ============================================================

ExprNodePtr QueryTranslator::parse_filter(
    const bsoncxx::document::view& filter,
    const CollectionMapping& mapping) const {

    std::vector<ExprNodePtr> conditions;

    for (auto it = filter.begin(); it != filter.end(); ++it) {
        auto elem = *it;
        std::string key(elem.key());

        if (key == "$and") {
            conditions.push_back(
                parse_logical(LogicOp::AND, elem.get_array().value, mapping));
        } else if (key == "$or") {
            conditions.push_back(
                parse_logical(LogicOp::OR, elem.get_array().value, mapping));
        } else if (key == "$nor") {
            auto or_node = parse_logical(LogicOp::OR, elem.get_array().value, mapping);
            conditions.push_back(
                ExprNode::make_logical(LogicOp::NOT, {or_node}));
        } else {
            conditions.push_back(parse_expression(key, elem, mapping));
        }
    }

    if (conditions.empty()) return nullptr;
    if (conditions.size() == 1) return conditions[0];
    return ExprNode::make_logical(LogicOp::AND, std::move(conditions));
}

ExprNodePtr QueryTranslator::parse_expression(
    const std::string& field,
    const bsoncxx::document::element& value,
    const CollectionMapping& mapping) const {

    std::string ch_col = resolve_column(field, mapping);
    auto col_node = ExprNode::make_column(ch_col);

    if (value.type() == bsoncxx::type::k_document) {
        // Operator expression: {field: {$gt: 5, $lt: 10}}
        auto doc = value.get_document().view();
        std::vector<ExprNodePtr> parts;

        for (auto it = doc.begin(); it != doc.end(); ++it) {
            auto op_elem = *it;
            std::string op(op_elem.key());

            if (op == "$gt") {
                parts.push_back(ExprNode::make_comparison(
                    CompOp::GT, col_node, bson_to_literal_node(op_elem)));
            } else if (op == "$gte") {
                parts.push_back(ExprNode::make_comparison(
                    CompOp::GTE, col_node, bson_to_literal_node(op_elem)));
            } else if (op == "$lt") {
                parts.push_back(ExprNode::make_comparison(
                    CompOp::LT, col_node, bson_to_literal_node(op_elem)));
            } else if (op == "$lte") {
                parts.push_back(ExprNode::make_comparison(
                    CompOp::LTE, col_node, bson_to_literal_node(op_elem)));
            } else if (op == "$eq") {
                parts.push_back(ExprNode::make_comparison(
                    CompOp::EQ, col_node, bson_to_literal_node(op_elem)));
            } else if (op == "$ne") {
                parts.push_back(ExprNode::make_comparison(
                    CompOp::NE, col_node, bson_to_literal_node(op_elem)));
            } else if (op == "$in") {
                parts.push_back(parse_in(ch_col, op_elem.get_array().value, false));
            } else if (op == "$nin") {
                parts.push_back(parse_in(ch_col, op_elem.get_array().value, true));
            } else if (op == "$exists") {
                bool exists = (op_elem.type() == bsoncxx::type::k_bool &&
                               op_elem.get_bool().value);
                parts.push_back(ExprNode::make_is_null(col_node, exists));
            } else if (op == "$regex") {
                std::string pattern(op_elem.get_string().value);
                parts.push_back(ExprNode::make_function("match", {
                    col_node,
                    ExprNode::make_literal(pattern)
                }));
            }
        }

        if (parts.empty()) return ExprNode::make_literal(true);
        if (parts.size() == 1) return parts[0];
        return ExprNode::make_logical(LogicOp::AND, std::move(parts));
    }

    // Simple equality: {field: value}
    return ExprNode::make_comparison(CompOp::EQ, col_node, bson_to_literal_node(value));
}

ExprNodePtr QueryTranslator::parse_logical(
    LogicOp op,
    const bsoncxx::array::view& conditions,
    const CollectionMapping& mapping) const {

    std::vector<ExprNodePtr> children;
    for (auto it = conditions.begin(); it != conditions.end(); ++it) {
        auto elem = *it;
        if (elem.type() == bsoncxx::type::k_document) {
            auto child = parse_filter(elem.get_document().value, mapping);
            if (child) children.push_back(child);
        }
    }

    if (children.empty()) return nullptr;
    if (children.size() == 1 && op != LogicOp::NOT) return children[0];
    return ExprNode::make_logical(op, std::move(children));
}

ExprNodePtr QueryTranslator::parse_in(
    const std::string& ch_column,
    const bsoncxx::array::view& values,
    bool negate) const {

    auto col_node = ExprNode::make_column(ch_column);
    std::vector<ExprNodePtr> value_nodes;

    for (auto it = values.begin(); it != values.end(); ++it) {
        value_nodes.push_back(bson_array_elem_to_literal(*it));
    }

    return ExprNode::make_in(col_node, std::move(value_nodes), negate);
}

void QueryTranslator::parse_group_stage(
    const bsoncxx::document::view& stage,
    const CollectionMapping& mapping,
    std::vector<ExprNodePtr>& select_exprs,
    std::vector<ExprNodePtr>& group_by) const {

    select_exprs.clear();
    group_by.clear();

    for (auto it = stage.begin(); it != stage.end(); ++it) {
        auto elem = *it;
        std::string key(elem.key());

        if (key == "_id") {
            // Group key → GROUP BY + SELECT
            if (elem.type() == bsoncxx::type::k_string) {
                std::string field = strip_dollar(std::string(elem.get_string().value));
                std::string col = resolve_column(field, mapping);
                auto col_node = ExprNode::make_column(col);
                group_by.push_back(col_node);
                select_exprs.push_back(col_node);
            } else if (elem.type() == bsoncxx::type::k_document) {
                // Compound group key: {_id: {year: "$year", month: "$month"}}
                for (auto git = elem.get_document().view().begin();
                     git != elem.get_document().view().end(); ++git) {
                    auto gelem = *git;
                    std::string alias(gelem.key());
                    std::string field = strip_dollar(std::string(gelem.get_string().value));
                    std::string col = resolve_column(field, mapping);
                    auto col_node = ExprNode::make_column(col);
                    group_by.push_back(col_node);
                    select_exprs.push_back(
                        ExprNode::make_select_expr(col_node, alias));
                }
            }
            // _id: null → aggregate entire collection, no GROUP BY
            continue;
        }

        // Accumulator fields: {total: {$sum: "$amount"}}
        if (elem.type() == bsoncxx::type::k_document) {
            auto acc_doc = elem.get_document().view();
            for (auto ait = acc_doc.begin(); ait != acc_doc.end(); ++ait) {
                auto acc_elem = *ait;
                std::string acc_op(acc_elem.key());
                auto func_node = parse_accumulator(acc_op, acc_elem, mapping);
                select_exprs.push_back(
                    ExprNode::make_select_expr(func_node, key));
            }
        }
    }
}

ExprNodePtr QueryTranslator::parse_accumulator(
    const std::string& acc_op,
    const bsoncxx::document::element& value,
    const CollectionMapping& mapping) const {

    if (acc_op == "$sum") {
        if (value.type() == bsoncxx::type::k_int32 && value.get_int32().value == 1) {
            return ExprNode::make_function("count", {
                ExprNode::make_literal(std::string("*"))
            });
        }
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            std::string col = resolve_column(field, mapping);
            return ExprNode::make_function("sum", {ExprNode::make_column(col)});
        }
    } else if (acc_op == "$avg") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            std::string col = resolve_column(field, mapping);
            return ExprNode::make_function("avg", {ExprNode::make_column(col)});
        }
    } else if (acc_op == "$min") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            std::string col = resolve_column(field, mapping);
            return ExprNode::make_function("min", {ExprNode::make_column(col)});
        }
    } else if (acc_op == "$max") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            std::string col = resolve_column(field, mapping);
            return ExprNode::make_function("max", {ExprNode::make_column(col)});
        }
    } else if (acc_op == "$count") {
        return ExprNode::make_function("count", {
            ExprNode::make_literal(std::string("*"))
        });
    }

    throw std::runtime_error("Unsupported accumulator: " + acc_op);
}

// ============================================================
// Helpers
// ============================================================

ExprNodePtr QueryTranslator::bson_to_literal_node(
    const bsoncxx::document::element& elem) const {

    switch (elem.type()) {
        case bsoncxx::type::k_int32:
            return ExprNode::make_literal(elem.get_int32().value);
        case bsoncxx::type::k_int64:
            return ExprNode::make_literal(elem.get_int64().value);
        case bsoncxx::type::k_double:
            return ExprNode::make_literal(elem.get_double().value);
        case bsoncxx::type::k_string:
            return ExprNode::make_literal(std::string(elem.get_string().value));
        case bsoncxx::type::k_bool:
            return ExprNode::make_literal(elem.get_bool().value);
        case bsoncxx::type::k_null:
            return ExprNode::make_null();
        default:
            return ExprNode::make_null();
    }
}

ExprNodePtr QueryTranslator::bson_array_elem_to_literal(
    const bsoncxx::array::element& elem) const {

    switch (elem.type()) {
        case bsoncxx::type::k_int32:
            return ExprNode::make_literal(elem.get_int32().value);
        case bsoncxx::type::k_int64:
            return ExprNode::make_literal(elem.get_int64().value);
        case bsoncxx::type::k_double:
            return ExprNode::make_literal(elem.get_double().value);
        case bsoncxx::type::k_string:
            return ExprNode::make_literal(std::string(elem.get_string().value));
        case bsoncxx::type::k_bool:
            return ExprNode::make_literal(elem.get_bool().value);
        default:
            return ExprNode::make_null();
    }
}

std::string QueryTranslator::resolve_column(
    const std::string& mongo_field,
    const CollectionMapping& mapping) const {

    for (const auto& field : mapping.fields) {
        if (field.mongo_field == mongo_field) {
            return field.ch_column;
        }
    }
    return mongo_field;
}

std::string QueryTranslator::strip_dollar(const std::string& field) {
    if (!field.empty() && field[0] == '$') {
        return field.substr(1);
    }
    return field;
}

} // namespace mg_clickhouse
