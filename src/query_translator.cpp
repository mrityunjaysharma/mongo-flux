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

            } else if (stage_name == "$skip") {
                if (elem.type() == bsoncxx::type::k_int32) {
                    tree.offset = elem.get_int32().value;
                } else if (elem.type() == bsoncxx::type::k_int64) {
                    tree.offset = elem.get_int64().value;
                }

            } else if (stage_name == "$count") {
                std::string count_field(elem.get_string().value);
                auto count_expr = ExprNode::make_function("count", {
                    ExprNode::make_literal(std::string("*"))
                });
                tree.select_exprs.clear();
                tree.select_exprs.push_back(
                    ExprNode::make_select_expr(count_expr, count_field));

            } else if (stage_name == "$project" || stage_name == "$addFields" || stage_name == "$set") {
                auto proj_doc = elem.get_document().view();
                // For $project with only inclusions (1), or $addFields/$set with expressions
                if (tree.select_exprs.empty() || stage_name != "$project") {
                    for (auto pit = proj_doc.begin(); pit != proj_doc.end(); ++pit) {
                        auto pelem = *pit;
                        std::string field_name(pelem.key());

                        if (pelem.type() == bsoncxx::type::k_int32 && pelem.get_int32().value == 1) {
                            // Simple inclusion
                            std::string col = resolve_column(field_name, mapping);
                            tree.select_exprs.push_back(ExprNode::make_column(col));
                        } else if (pelem.type() == bsoncxx::type::k_int32 && pelem.get_int32().value == 0) {
                            // Exclusion — skip
                            continue;
                        } else if (pelem.type() == bsoncxx::type::k_document) {
                            // Expression: {field: {$multiply: ["$price", "$qty"]}}
                            auto expr_node = parse_agg_expression(pelem, mapping);
                            if (expr_node) {
                                tree.select_exprs.push_back(
                                    ExprNode::make_select_expr(expr_node, field_name));
                            }
                        } else if (pelem.type() == bsoncxx::type::k_string) {
                            // Field reference: {newName: "$oldField"}
                            std::string ref = strip_dollar(std::string(pelem.get_string().value));
                            std::string col = resolve_column(ref, mapping);
                            tree.select_exprs.push_back(
                                ExprNode::make_select_expr(ExprNode::make_column(col), field_name));
                        }
                    }
                }

            } else if (stage_name == "$unwind") {
                // $unwind translates to arrayJoin in ClickHouse
                // For simple cases: {$unwind: "$field"} or {$unwind: {path: "$field"}}
                std::string field;
                if (elem.type() == bsoncxx::type::k_string) {
                    field = strip_dollar(std::string(elem.get_string().value));
                } else if (elem.type() == bsoncxx::type::k_document) {
                    auto unwind_doc = elem.get_document().view();
                    auto path_elem = unwind_doc["path"];
                    if (path_elem && path_elem.type() == bsoncxx::type::k_string) {
                        field = strip_dollar(std::string(path_elem.get_string().value));
                    }
                }
                // Note: arrayJoin support requires the column to be Array type in CH
                // This is a best-effort translation

            } else if (stage_name == "$sample") {
                // {$sample: {size: N}} → ORDER BY rand() LIMIT N
                if (elem.type() == bsoncxx::type::k_document) {
                    auto sample_doc = elem.get_document().view();
                    auto size_elem = sample_doc["size"];
                    if (size_elem) {
                        if (size_elem.type() == bsoncxx::type::k_int32)
                            tree.limit = size_elem.get_int32().value;
                        else if (size_elem.type() == bsoncxx::type::k_int64)
                            tree.limit = size_elem.get_int64().value;
                    }
                    tree.order_by.clear();
                    tree.order_by.emplace_back(
                        ExprNode::make_function("rand", {}), false);
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
            return ExprNode::make_function("count", {ExprNode::make_literal(std::string("*"))});
        }
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("sum", {ExprNode::make_column(resolve_column(field, mapping))});
        }
        if (value.type() == bsoncxx::type::k_document) {
            auto expr = parse_agg_expression(value, mapping);
            if (expr) return ExprNode::make_function("sum", {expr});
        }
    } else if (acc_op == "$avg") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("avg", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$min") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("min", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$max") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("max", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$count") {
        return ExprNode::make_function("count", {ExprNode::make_literal(std::string("*"))});
    } else if (acc_op == "$first") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("any", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$last") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("anyLast", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$stdDevPop") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("stddevPop", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$stdDevSamp") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            return ExprNode::make_function("stddevSamp", {ExprNode::make_column(resolve_column(field, mapping))});
        }
    } else if (acc_op == "$push" || acc_op == "$addToSet") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field = strip_dollar(std::string(value.get_string().value));
            std::string func = (acc_op == "$addToSet") ? "groupUniqArray" : "groupArray";
            return ExprNode::make_function(func, {ExprNode::make_column(resolve_column(field, mapping))});
        }
    }

    throw std::runtime_error("Unsupported accumulator: " + acc_op);
}

// ============================================================
// Aggregation Expression Parser
// Handles: arithmetic, string, date, conditional expressions
// ============================================================

ExprNodePtr QueryTranslator::parse_agg_expression(
    const bsoncxx::document::element& expr,
    const CollectionMapping& mapping) const {

    // Field reference: "$fieldName"
    if (expr.type() == bsoncxx::type::k_string) {
        std::string val(expr.get_string().value);
        if (!val.empty() && val[0] == '$') {
            std::string field = val.substr(1);
            return ExprNode::make_column(resolve_column(field, mapping));
        }
        return ExprNode::make_literal(val);
    }

    // Literal values
    if (expr.type() == bsoncxx::type::k_int32) return ExprNode::make_literal(expr.get_int32().value);
    if (expr.type() == bsoncxx::type::k_int64) return ExprNode::make_literal(expr.get_int64().value);
    if (expr.type() == bsoncxx::type::k_double) return ExprNode::make_literal(expr.get_double().value);
    if (expr.type() == bsoncxx::type::k_bool) return ExprNode::make_literal(expr.get_bool().value);
    if (expr.type() == bsoncxx::type::k_null) return ExprNode::make_null();

    // Expression document: {$operator: args}
    if (expr.type() != bsoncxx::type::k_document) return ExprNode::make_null();

    auto doc = expr.get_document().view();
    auto first_it = doc.begin();
    if (first_it == doc.end()) return ExprNode::make_null();

    auto first_elem = *first_it;
    std::string op(first_elem.key());

    // Helper to parse array of expression args
    auto parse_args = [&](const bsoncxx::array::view& arr) -> std::vector<ExprNodePtr> {
        std::vector<ExprNodePtr> args;
        for (auto it = arr.begin(); it != arr.end(); ++it) {
            auto a = *it;
            if (a.type() == bsoncxx::type::k_string) {
                std::string s(a.get_string().value);
                if (!s.empty() && s[0] == '$') {
                    args.push_back(ExprNode::make_column(resolve_column(s.substr(1), mapping)));
                } else {
                    args.push_back(ExprNode::make_literal(s));
                }
            } else if (a.type() == bsoncxx::type::k_int32) {
                args.push_back(ExprNode::make_literal(a.get_int32().value));
            } else if (a.type() == bsoncxx::type::k_int64) {
                args.push_back(ExprNode::make_literal(a.get_int64().value));
            } else if (a.type() == bsoncxx::type::k_double) {
                args.push_back(ExprNode::make_literal(a.get_double().value));
            } else if (a.type() == bsoncxx::type::k_document) {
                // Nested expression
                // Create a temporary element-like wrapper — use the document directly
                args.push_back(ExprNode::make_null()); // placeholder for nested
            } else {
                args.push_back(ExprNode::make_null());
            }
        }
        return args;
    };

    // --- Arithmetic Expressions ---
    if (op == "$multiply" || op == "$add" || op == "$subtract" || op == "$divide" || op == "$mod") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 2) {
                // Map to ClickHouse binary operators via function syntax
                std::string ch_op;
                if (op == "$multiply") ch_op = "multiply";
                else if (op == "$add") ch_op = "plus";
                else if (op == "$subtract") ch_op = "minus";
                else if (op == "$divide") ch_op = "divide";
                else if (op == "$mod") ch_op = "modulo";

                // Chain binary ops: multiply(a, multiply(b, c))
                auto result = args[0];
                for (size_t i = 1; i < args.size(); ++i) {
                    result = ExprNode::make_function(ch_op, {result, args[i]});
                }
                return result;
            }
        }
    }

    // --- Math Expressions ---
    if (op == "$abs") return ExprNode::make_function("abs", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$ceil") return ExprNode::make_function("ceil", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$floor") return ExprNode::make_function("floor", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$round") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 1) return ExprNode::make_function("round", args);
        }
        return ExprNode::make_function("round", {parse_field_or_literal(first_elem, mapping)});
    }
    if (op == "$sqrt") return ExprNode::make_function("sqrt", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$log10") return ExprNode::make_function("log10", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$ln") return ExprNode::make_function("log", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$pow") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 2) return ExprNode::make_function("pow", {args[0], args[1]});
        }
    }

    // --- String Expressions ---
    if (op == "$concat") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            return ExprNode::make_function("concat", args);
        }
    }
    if (op == "$toUpper") return ExprNode::make_function("upper", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$toLower") return ExprNode::make_function("lower", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$trim") return ExprNode::make_function("trimBoth", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$ltrim") return ExprNode::make_function("trimLeft", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$rtrim") return ExprNode::make_function("trimRight", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$strLenBytes" || op == "$strLenCP") return ExprNode::make_function("length", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$substr" || op == "$substrBytes") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 3) {
                // ClickHouse substring(s, offset+1, length) — MongoDB is 0-indexed
                return ExprNode::make_function("substring", {args[0],
                    ExprNode::make_function("plus", {args[1], ExprNode::make_literal(int32_t(1))}),
                    args[2]});
            }
        }
    }
    if (op == "$split") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 2) return ExprNode::make_function("splitByString", {args[1], args[0]});
        }
    }
    if (op == "$regexMatch") {
        if (first_elem.type() == bsoncxx::type::k_document) {
            auto regex_doc = first_elem.get_document().view();
            auto input = regex_doc["input"];
            auto regex = regex_doc["regex"];
            if (input && regex) {
                auto input_node = parse_field_or_literal(input, mapping);
                auto regex_node = ExprNode::make_literal(std::string(regex.get_string().value));
                return ExprNode::make_function("match", {input_node, regex_node});
            }
        }
    }

    // --- Date Expressions ---
    if (op == "$year") return ExprNode::make_function("toYear", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$month") return ExprNode::make_function("toMonth", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$dayOfMonth") return ExprNode::make_function("toDayOfMonth", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$dayOfWeek") return ExprNode::make_function("toDayOfWeek", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$dayOfYear") return ExprNode::make_function("toDayOfYear", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$hour") return ExprNode::make_function("toHour", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$minute") return ExprNode::make_function("toMinute", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$second") return ExprNode::make_function("toSecond", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$week") return ExprNode::make_function("toISOWeek", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$dateToString") {
        if (first_elem.type() == bsoncxx::type::k_document) {
            auto dts_doc = first_elem.get_document().view();
            auto date_elem = dts_doc["date"];
            if (date_elem) {
                return ExprNode::make_function("toString", {parse_field_or_literal(date_elem, mapping)});
            }
        }
    }

    // --- Conditional Expressions ---
    if (op == "$cond") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 3) {
                // if(cond, then, else)
                return ExprNode::make_function("if", {args[0], args[1], args[2]});
            }
        } else if (first_elem.type() == bsoncxx::type::k_document) {
            auto cond_doc = first_elem.get_document().view();
            auto if_elem = cond_doc["if"];
            auto then_elem = cond_doc["then"];
            auto else_elem = cond_doc["else"];
            if (if_elem && then_elem && else_elem) {
                return ExprNode::make_function("if", {
                    parse_field_or_literal(if_elem, mapping),
                    parse_field_or_literal(then_elem, mapping),
                    parse_field_or_literal(else_elem, mapping)
                });
            }
        }
    }
    if (op == "$ifNull") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 2) {
                return ExprNode::make_function("ifNull", {args[0], args[1]});
            }
        }
    }

    // --- Type Expressions ---
    if (op == "$toInt" || op == "$toDouble" || op == "$toString" || op == "$toLong") {
        std::string ch_func;
        if (op == "$toInt") ch_func = "toInt32";
        else if (op == "$toLong") ch_func = "toInt64";
        else if (op == "$toDouble") ch_func = "toFloat64";
        else if (op == "$toString") ch_func = "toString";
        return ExprNode::make_function(ch_func, {parse_field_or_literal(first_elem, mapping)});
    }

    // --- Array Expressions ---
    if (op == "$size") return ExprNode::make_function("length", {parse_field_or_literal(first_elem, mapping)});
    if (op == "$arrayElemAt") {
        if (first_elem.type() == bsoncxx::type::k_array) {
            auto args = parse_args(first_elem.get_array().value);
            if (args.size() >= 2) {
                // ClickHouse arrays are 1-indexed
                return ExprNode::make_function("arrayElement", {args[0],
                    ExprNode::make_function("plus", {args[1], ExprNode::make_literal(int32_t(1))})});
            }
        }
    }

    // Fallback: return NULL for unsupported expressions
    return ExprNode::make_null();
}

ExprNodePtr QueryTranslator::parse_field_or_literal(
    const bsoncxx::document::element& elem,
    const CollectionMapping& mapping) const {

    if (elem.type() == bsoncxx::type::k_string) {
        std::string val(elem.get_string().value);
        if (!val.empty() && val[0] == '$') {
            return ExprNode::make_column(resolve_column(val.substr(1), mapping));
        }
        return ExprNode::make_literal(val);
    }
    if (elem.type() == bsoncxx::type::k_int32) return ExprNode::make_literal(elem.get_int32().value);
    if (elem.type() == bsoncxx::type::k_int64) return ExprNode::make_literal(elem.get_int64().value);
    if (elem.type() == bsoncxx::type::k_double) return ExprNode::make_literal(elem.get_double().value);
    if (elem.type() == bsoncxx::type::k_bool) return ExprNode::make_literal(elem.get_bool().value);
    if (elem.type() == bsoncxx::type::k_document) {
        return parse_agg_expression(elem, mapping);
    }
    return ExprNode::make_null();
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
