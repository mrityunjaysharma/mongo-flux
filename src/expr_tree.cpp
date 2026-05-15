#include "mg_clickhouse/expr_tree.h"

#include <sstream>
#include <stdexcept>

namespace mg_clickhouse {

namespace {

std::string quote_ident(const std::string& ident) {
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

std::string quote_literal(const std::string& value) {
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

std::string comp_op_to_sql(CompOp op) {
    switch (op) {
        case CompOp::EQ:  return "=";
        case CompOp::NE:  return "!=";
        case CompOp::GT:  return ">";
        case CompOp::GTE: return ">=";
        case CompOp::LT:  return "<";
        case CompOp::LTE: return "<=";
    }
    return "=";
}

std::string emit_literal(const LiteralValue& val) {
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return "NULL";
        } else if constexpr (std::is_same_v<T, int32_t>) {
            return std::to_string(arg);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return std::to_string(arg);
        } else if constexpr (std::is_same_v<T, double>) {
            return std::to_string(arg);
        } else if constexpr (std::is_same_v<T, bool>) {
            return arg ? "1" : "0";
        } else if constexpr (std::is_same_v<T, std::string>) {
            return quote_literal(arg);
        }
    }, val);
}

} // anonymous namespace

std::string emit_sql(const ExprNodePtr& node) {
    if (!node) return "";

    switch (node->type) {
        case ExprType::LITERAL:
            return emit_literal(node->literal.value);

        case ExprType::COLUMN_REF:
            return quote_ident(node->column_ref.column_name);

        case ExprType::COMPARISON: {
            std::string left = emit_sql(node->comparison.left);
            std::string right = emit_sql(node->comparison.right);
            return left + " " + comp_op_to_sql(node->comparison.op) + " " + right;
        }

        case ExprType::LOGICAL: {
            const auto& data = node->logical;
            if (data.op == LogicOp::NOT) {
                if (data.children.size() == 1) {
                    return "NOT (" + emit_sql(data.children[0]) + ")";
                }
                return "NOT (1)";
            }

            std::string op_str = (data.op == LogicOp::AND) ? " AND " : " OR ";
            std::ostringstream out;
            for (size_t i = 0; i < data.children.size(); ++i) {
                if (i > 0) out << op_str;
                // Wrap children in parens for clarity
                if (data.children.size() > 1) {
                    out << "(" << emit_sql(data.children[i]) << ")";
                } else {
                    out << emit_sql(data.children[i]);
                }
            }
            return out.str();
        }

        case ExprType::IN_LIST: {
            const auto& data = node->in_list;
            std::ostringstream out;
            out << emit_sql(data.column);
            out << (data.negate ? " NOT IN (" : " IN (");
            for (size_t i = 0; i < data.values.size(); ++i) {
                if (i > 0) out << ", ";
                out << emit_sql(data.values[i]);
            }
            out << ")";
            return out.str();
        }

        case ExprType::FUNCTION_CALL: {
            const auto& data = node->function_call;
            std::ostringstream out;
            out << data.function_name << "(";

            // Special case: count(*) — the star is not a quoted literal
            if (data.function_name == "count" && data.args.size() == 1 &&
                data.args[0]->type == ExprType::LITERAL) {
                auto* lit = &data.args[0]->literal.value;
                if (auto* s = std::get_if<std::string>(lit); s && *s == "*") {
                    out << "*";
                } else {
                    out << emit_sql(data.args[0]);
                }
            } else {
                for (size_t i = 0; i < data.args.size(); ++i) {
                    if (i > 0) out << ", ";
                    out << emit_sql(data.args[i]);
                }
            }

            out << ")";
            return out.str();
        }

        case ExprType::IS_NULL: {
            const auto& data = node->is_null;
            return emit_sql(data.column) + (data.negate ? " IS NOT NULL" : " IS NULL");
        }

        case ExprType::SELECT_EXPR: {
            const auto& data = node->select_expr;
            std::string expr_sql = emit_sql(data.expr);
            if (data.alias.empty()) return expr_sql;
            return expr_sql + " AS " + quote_ident(data.alias);
        }

        case ExprType::BETWEEN:
            // Not yet used, fall through
            return "1";
    }

    return "1";
}

std::string emit_query(const QueryTree& tree) {
    std::ostringstream sql;

    // SELECT
    sql << "SELECT ";
    if (tree.select_exprs.empty()) {
        sql << "*";
    } else {
        for (size_t i = 0; i < tree.select_exprs.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << emit_sql(tree.select_exprs[i]);
        }
    }

    // FROM
    sql << " FROM " << tree.from_database << "." << quote_ident(tree.from_table);

    // WHERE
    if (tree.where_clause) {
        std::string where_sql = emit_sql(tree.where_clause);
        if (!where_sql.empty()) {
            sql << " WHERE " << where_sql;
        }
    }

    // GROUP BY
    if (!tree.group_by.empty()) {
        sql << " GROUP BY ";
        for (size_t i = 0; i < tree.group_by.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << emit_sql(tree.group_by[i]);
        }
    }

    // HAVING
    if (tree.having_clause) {
        std::string having_sql = emit_sql(tree.having_clause);
        if (!having_sql.empty()) {
            sql << " HAVING " << having_sql;
        }
    }

    // ORDER BY
    if (!tree.order_by.empty()) {
        sql << " ORDER BY ";
        for (size_t i = 0; i < tree.order_by.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << emit_sql(tree.order_by[i].first);
            if (tree.order_by[i].second) sql << " DESC";
        }
    }

    // LIMIT / OFFSET
    if (tree.limit > 0) {
        sql << " LIMIT " << tree.limit;
    }
    if (tree.offset > 0) {
        sql << " OFFSET " << tree.offset;
    }

    return sql.str();
}

} // namespace mg_clickhouse
