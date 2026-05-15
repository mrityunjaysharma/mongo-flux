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

std::string QueryTranslator::translate_find(
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

    std::ostringstream sql;

    // SELECT clause
    sql << "SELECT ";
    if (projection.empty()) {
        // Select all mapped columns
        for (size_t i = 0; i < mapping.fields.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << quote_ident(mapping.fields[i].ch_column);
        }
    } else {
        bool first = true;
        for (auto it = projection.begin(); it != projection.end(); ++it) {
            auto elem = *it;
            // Include fields with value 1 or true
            if (elem.type() == bsoncxx::type::k_int32 && elem.get_int32().value == 1) {
                if (!first) sql << ", ";
                sql << quote_ident(resolve_column(std::string(elem.key()), mapping));
                first = false;
            }
        }
    }

    // FROM clause
    sql << " FROM " << mapping.clickhouse_database << "." << quote_ident(mapping.clickhouse_table);

    // WHERE clause
    if (!filter.empty()) {
        std::string where = translate_filter(filter, mapping);
        if (!where.empty()) {
            sql << " WHERE " << where;
        }
    }

    // ORDER BY clause
    if (!sort.empty()) {
        sql << " ORDER BY ";
        bool first = true;
        for (auto it = sort.begin(); it != sort.end(); ++it) {
            auto elem = *it;
            if (!first) sql << ", ";
            sql << quote_ident(resolve_column(std::string(elem.key()), mapping));
            if (elem.type() == bsoncxx::type::k_int32 && elem.get_int32().value == -1) {
                sql << " DESC";
            }
            first = false;
        }
    }

    // LIMIT / OFFSET
    if (limit > 0) {
        sql << " LIMIT " << limit;
    }
    if (skip > 0) {
        sql << " OFFSET " << skip;
    }

    return sql.str();
}

std::string QueryTranslator::translate_aggregate(
    const std::string& collection,
    const std::vector<bsoncxx::document::view>& pipeline) const {

    auto mapping_opt = registry_->get(collection);
    if (!mapping_opt) {
        throw std::runtime_error("No schema mapping found for collection: " + collection);
    }
    const auto& mapping = *mapping_opt;

    // Build SQL from pipeline stages
    std::string select_clause;
    std::string where_clause;
    std::string group_by_clause;
    std::string having_clause;
    std::string order_by_clause;
    std::string limit_clause;

    for (const auto& stage_doc : pipeline) {
        for (auto it = stage_doc.begin(); it != stage_doc.end(); ++it) {
            auto elem = *it;
            std::string stage_name(elem.key());

            if (stage_name == "$match") {
                auto match_doc = elem.get_document().view();
                std::string condition = translate_filter(match_doc, mapping);
                if (!condition.empty()) {
                    if (group_by_clause.empty()) {
                        // Pre-group $match → WHERE
                        if (where_clause.empty())
                            where_clause = condition;
                        else
                            where_clause += " AND " + condition;
                    } else {
                        // Post-group $match → HAVING
                        if (having_clause.empty())
                            having_clause = condition;
                        else
                            having_clause += " AND " + condition;
                    }
                }
            } else if (stage_name == "$group") {
                auto group_doc = elem.get_document().view();
                select_clause = translate_group_stage(group_doc, mapping);

                // Extract _id for GROUP BY
                auto id_elem = group_doc["_id"];
                if (id_elem) {
                    if (id_elem.type() == bsoncxx::type::k_string) {
                        std::string field(id_elem.get_string().value);
                        if (!field.empty() && field[0] == '$') {
                            field = field.substr(1);
                        }
                        group_by_clause = quote_ident(resolve_column(field, mapping));
                    } else if (id_elem.type() == bsoncxx::type::k_document) {
                        // Compound group key
                        std::ostringstream gb;
                        bool first = true;
                        for (auto git = id_elem.get_document().view().begin();
                             git != id_elem.get_document().view().end(); ++git) {
                            auto gelem = *git;
                            if (!first) gb << ", ";
                            std::string field(gelem.get_string().value);
                            if (!field.empty() && field[0] == '$') field = field.substr(1);
                            gb << quote_ident(resolve_column(field, mapping));
                            first = false;
                        }
                        group_by_clause = gb.str();
                    }
                    // _id: null means no grouping (aggregate entire collection)
                }
            } else if (stage_name == "$sort") {
                auto sort_doc = elem.get_document().view();
                std::ostringstream ob;
                bool first = true;
                for (auto sit = sort_doc.begin(); sit != sort_doc.end(); ++sit) {
                    auto selem = *sit;
                    if (!first) ob << ", ";
                    ob << quote_ident(resolve_column(std::string(selem.key()), mapping));
                    if (selem.type() == bsoncxx::type::k_int32 && selem.get_int32().value == -1) {
                        ob << " DESC";
                    }
                    first = false;
                }
                order_by_clause = ob.str();
            } else if (stage_name == "$limit") {
                if (elem.type() == bsoncxx::type::k_int32) {
                    limit_clause = std::to_string(elem.get_int32().value);
                } else if (elem.type() == bsoncxx::type::k_int64) {
                    limit_clause = std::to_string(elem.get_int64().value);
                }
            } else if (stage_name == "$count") {
                std::string count_field(elem.get_string().value);
                select_clause = "count(*) AS " + quote_ident(count_field);
            } else if (stage_name == "$project") {
                // If no group stage, $project defines the SELECT
                if (select_clause.empty()) {
                    auto proj_doc = elem.get_document().view();
                    std::ostringstream sc;
                    bool first = true;
                    for (auto pit = proj_doc.begin(); pit != proj_doc.end(); ++pit) {
                        auto pelem = *pit;
                        if (pelem.type() == bsoncxx::type::k_int32 && pelem.get_int32().value == 1) {
                            if (!first) sc << ", ";
                            sc << quote_ident(resolve_column(std::string(pelem.key()), mapping));
                            first = false;
                        }
                    }
                    select_clause = sc.str();
                }
            }
        }
    }

    // Build final SQL
    std::ostringstream sql;
    sql << "SELECT ";
    if (select_clause.empty()) {
        // Default: all mapped columns
        for (size_t i = 0; i < mapping.fields.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << quote_ident(mapping.fields[i].ch_column);
        }
    } else {
        sql << select_clause;
    }

    sql << " FROM " << mapping.clickhouse_database << "." << quote_ident(mapping.clickhouse_table);

    if (!where_clause.empty()) sql << " WHERE " << where_clause;
    if (!group_by_clause.empty()) sql << " GROUP BY " << group_by_clause;
    if (!having_clause.empty()) sql << " HAVING " << having_clause;
    if (!order_by_clause.empty()) sql << " ORDER BY " << order_by_clause;
    if (!limit_clause.empty()) sql << " LIMIT " << limit_clause;

    return sql.str();
}

std::string QueryTranslator::translate_filter(
    const bsoncxx::document::view& filter,
    const CollectionMapping& mapping) const {

    std::vector<std::string> conditions;

    for (auto it = filter.begin(); it != filter.end(); ++it) {
        auto elem = *it;
        std::string key(elem.key());

        if (key == "$and") {
            conditions.push_back(translate_logical("AND", elem.get_array().value, mapping));
        } else if (key == "$or") {
            conditions.push_back(translate_logical("OR", elem.get_array().value, mapping));
        } else if (key == "$nor") {
            conditions.push_back("NOT (" + translate_logical("OR", elem.get_array().value, mapping) + ")");
        } else {
            conditions.push_back(translate_expression(key, elem, mapping));
        }
    }

    if (conditions.empty()) return "";
    if (conditions.size() == 1) return conditions[0];

    std::ostringstream result;
    for (size_t i = 0; i < conditions.size(); ++i) {
        if (i > 0) result << " AND ";
        result << "(" << conditions[i] << ")";
    }
    return result.str();
}

std::string QueryTranslator::translate_expression(
    const std::string& field,
    const bsoncxx::document::element& value,
    const CollectionMapping& mapping) const {

    std::string ch_col = quote_ident(resolve_column(field, mapping));

    if (value.type() == bsoncxx::type::k_document) {
        // Operator expression: {field: {$gt: 5, $lt: 10}}
        auto doc = value.get_document().view();
        std::vector<std::string> parts;

        for (auto it = doc.begin(); it != doc.end(); ++it) {
            auto op_elem = *it;
            std::string op(op_elem.key());

            if (op == "$gt") parts.push_back(ch_col + " > " + bson_to_literal(op_elem));
            else if (op == "$gte") parts.push_back(ch_col + " >= " + bson_to_literal(op_elem));
            else if (op == "$lt") parts.push_back(ch_col + " < " + bson_to_literal(op_elem));
            else if (op == "$lte") parts.push_back(ch_col + " <= " + bson_to_literal(op_elem));
            else if (op == "$eq") parts.push_back(ch_col + " = " + bson_to_literal(op_elem));
            else if (op == "$ne") parts.push_back(ch_col + " != " + bson_to_literal(op_elem));
            else if (op == "$in") {
                parts.push_back(translate_in(ch_col, op_elem.get_array().value, false));
            } else if (op == "$nin") {
                parts.push_back(translate_in(ch_col, op_elem.get_array().value, true));
            } else if (op == "$exists") {
                bool exists = (op_elem.type() == bsoncxx::type::k_bool && op_elem.get_bool().value);
                parts.push_back(ch_col + (exists ? " IS NOT NULL" : " IS NULL"));
            } else if (op == "$regex") {
                std::string pattern(op_elem.get_string().value);
                parts.push_back("match(" + ch_col + ", " + quote_literal(pattern) + ")");
            }
        }

        if (parts.size() == 1) return parts[0];
        std::ostringstream combined;
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) combined << " AND ";
            combined << parts[i];
        }
        return combined.str();
    }

    // Simple equality: {field: value}
    return ch_col + " = " + bson_to_literal(value);
}

std::string QueryTranslator::translate_in(
    const std::string& ch_column,
    const bsoncxx::array::view& values,
    bool negate) const {

    std::ostringstream sql;
    sql << ch_column << (negate ? " NOT IN (" : " IN (");
    bool first = true;
    for (auto it = values.begin(); it != values.end(); ++it) {
        if (!first) sql << ", ";
        auto elem = *it;
        // Convert array element to literal
        switch (elem.type()) {
            case bsoncxx::type::k_int32:
                sql << elem.get_int32().value;
                break;
            case bsoncxx::type::k_int64:
                sql << elem.get_int64().value;
                break;
            case bsoncxx::type::k_double:
                sql << elem.get_double().value;
                break;
            case bsoncxx::type::k_string:
                sql << quote_literal(std::string(elem.get_string().value));
                break;
            default:
                sql << "NULL";
                break;
        }
        first = false;
    }
    sql << ")";
    return sql.str();
}

std::string QueryTranslator::translate_logical(
    const std::string& op,
    const bsoncxx::array::view& conditions,
    const CollectionMapping& mapping) const {

    std::vector<std::string> parts;
    for (auto it = conditions.begin(); it != conditions.end(); ++it) {
        auto elem = *it;
        if (elem.type() == bsoncxx::type::k_document) {
            std::string cond = translate_filter(elem.get_document().value, mapping);
            if (!cond.empty()) parts.push_back("(" + cond + ")");
        }
    }

    std::ostringstream result;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) result << " " << op << " ";
        result << parts[i];
    }
    return result.str();
}

std::string QueryTranslator::translate_group_stage(
    const bsoncxx::document::view& stage,
    const CollectionMapping& mapping) const {

    std::ostringstream select;
    bool first = true;

    for (auto it = stage.begin(); it != stage.end(); ++it) {
        auto elem = *it;
        std::string key(elem.key());

        if (key == "_id") {
            // Group key becomes a SELECT column
            if (elem.type() == bsoncxx::type::k_string) {
                std::string field(elem.get_string().value);
                if (!field.empty() && field[0] == '$') field = field.substr(1);
                if (!first) select << ", ";
                select << quote_ident(resolve_column(field, mapping));
                first = false;
            } else if (elem.type() == bsoncxx::type::k_document) {
                for (auto git = elem.get_document().view().begin();
                     git != elem.get_document().view().end(); ++git) {
                    auto gelem = *git;
                    if (!first) select << ", ";
                    std::string field(gelem.get_string().value);
                    if (!field.empty() && field[0] == '$') field = field.substr(1);
                    select << quote_ident(resolve_column(field, mapping))
                           << " AS " << quote_ident(std::string(gelem.key()));
                    first = false;
                }
            }
            // _id: null → no group key in SELECT
            continue;
        }

        // Accumulator fields
        if (!first) select << ", ";
        if (elem.type() == bsoncxx::type::k_document) {
            auto acc_doc = elem.get_document().view();
            for (auto ait = acc_doc.begin(); ait != acc_doc.end(); ++ait) {
                auto acc_elem = *ait;
                std::string acc_op(acc_elem.key());
                select << translate_accumulator(acc_op, acc_elem, mapping);
            }
        }
        select << " AS " << quote_ident(key);
        first = false;
    }

    return select.str();
}

std::string QueryTranslator::translate_accumulator(
    const std::string& acc_op,
    const bsoncxx::document::element& value,
    const CollectionMapping& mapping) const {

    if (acc_op == "$sum") {
        if (value.type() == bsoncxx::type::k_int32 && value.get_int32().value == 1) {
            return "count(*)";
        }
        if (value.type() == bsoncxx::type::k_string) {
            std::string field(value.get_string().value);
            if (!field.empty() && field[0] == '$') field = field.substr(1);
            return "sum(" + quote_ident(resolve_column(field, mapping)) + ")";
        }
    } else if (acc_op == "$avg") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field(value.get_string().value);
            if (!field.empty() && field[0] == '$') field = field.substr(1);
            return "avg(" + quote_ident(resolve_column(field, mapping)) + ")";
        }
    } else if (acc_op == "$min") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field(value.get_string().value);
            if (!field.empty() && field[0] == '$') field = field.substr(1);
            return "min(" + quote_ident(resolve_column(field, mapping)) + ")";
        }
    } else if (acc_op == "$max") {
        if (value.type() == bsoncxx::type::k_string) {
            std::string field(value.get_string().value);
            if (!field.empty() && field[0] == '$') field = field.substr(1);
            return "max(" + quote_ident(resolve_column(field, mapping)) + ")";
        }
    } else if (acc_op == "$count") {
        return "count(*)";
    }

    throw std::runtime_error("Unsupported accumulator: " + acc_op);
}

std::string QueryTranslator::resolve_column(
    const std::string& mongo_field,
    const CollectionMapping& mapping) const {

    for (const auto& field : mapping.fields) {
        if (field.mongo_field == mongo_field) {
            return field.ch_column;
        }
    }
    // Fall back to using the mongo field name directly
    return mongo_field;
}

std::string QueryTranslator::quote_ident(const std::string& ident) {
    // ClickHouse uses backticks or double quotes for identifiers
    std::string escaped;
    escaped.reserve(ident.size() + 2);
    escaped += '`';
    for (char c : ident) {
        if (c == '`') escaped += "``";
        else escaped += c;
    }
    escaped += '`';
    return escaped;
}

std::string QueryTranslator::quote_literal(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped += '\'';
    for (char c : value) {
        if (c == '\'') escaped += "\\'";
        else if (c == '\\') escaped += "\\\\";
        else escaped += c;
    }
    escaped += '\'';
    return escaped;
}

std::string QueryTranslator::bson_to_literal(const bsoncxx::document::element& elem) const {
    switch (elem.type()) {
        case bsoncxx::type::k_int32:
            return std::to_string(elem.get_int32().value);
        case bsoncxx::type::k_int64:
            return std::to_string(elem.get_int64().value);
        case bsoncxx::type::k_double:
            return std::to_string(elem.get_double().value);
        case bsoncxx::type::k_string:
            return quote_literal(std::string(elem.get_string().value));
        case bsoncxx::type::k_bool:
            return elem.get_bool().value ? "1" : "0";
        case bsoncxx::type::k_null:
            return "NULL";
        default:
            return "NULL";
    }
}

} // namespace mg_clickhouse
