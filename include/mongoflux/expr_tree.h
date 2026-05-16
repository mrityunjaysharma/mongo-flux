#pragma once

#include <memory>
#include <string>
#include <vector>
#include <variant>
#include <stdexcept>

namespace mongoflux {

/**
 * Expression tree nodes for representing MongoDB queries as an AST.
 *
 * The translation pipeline is:
 *   BSON document → ExprNode tree (parse phase)
 *   ExprNode tree → ClickHouse SQL string (emit phase)
 *
 * This separation allows:
 *   - Independent testing of parse and emit
 *   - Tree transformations (optimization, rewriting)
 *   - Multiple output targets (SQL dialects, explain plans)
 */

// Forward declarations
struct ExprNode;
using ExprNodePtr = std::shared_ptr<ExprNode>;

// Literal value types supported in expressions
using LiteralValue = std::variant<std::monostate, // NULL
                                  int32_t,
                                  int64_t,
                                  double,
                                  bool,
                                  std::string>;

/**
 * Node types in the expression tree.
 */
enum class ExprType {
    LITERAL,       // A constant value (number, string, bool, null)
    COLUMN_REF,    // Reference to a ClickHouse column
    COMPARISON,    // Binary comparison (=, !=, <, >, <=, >=)
    LOGICAL,       // AND, OR, NOT
    IN_LIST,       // column IN (v1, v2, ...) or NOT IN
    FUNCTION_CALL, // match(), count(*), sum(), etc.
    IS_NULL,       // column IS NULL / IS NOT NULL
    BETWEEN,       // column BETWEEN a AND b (optimization)
    SELECT_EXPR,   // A named expression in SELECT (expr AS alias)
};

/**
 * Comparison operators.
 */
enum class CompOp {
    EQ,  // =
    NE,  // !=
    GT,  // >
    GTE, // >=
    LT,  // <
    LTE, // <=
};

/**
 * Logical operators.
 */
enum class LogicOp {
    AND,
    OR,
    NOT,
};

/**
 * The expression tree node. Uses a tagged-union style with ExprType
 * discriminator and type-specific payload structs.
 */
struct ExprNode {
    ExprType type;

    // --- Payloads (only one is active based on `type`) ---

    struct LiteralData {
        LiteralValue value;
    };

    struct ColumnRefData {
        std::string column_name; // Already resolved to ClickHouse column
    };

    struct ComparisonData {
        CompOp op;
        ExprNodePtr left;  // Usually a ColumnRef
        ExprNodePtr right; // Usually a Literal
    };

    struct LogicalData {
        LogicOp op;
        std::vector<ExprNodePtr> children;
    };

    struct InListData {
        ExprNodePtr column;
        std::vector<ExprNodePtr> values;
        bool negate; // true = NOT IN
    };

    struct FunctionCallData {
        std::string function_name; // e.g. "match", "count", "sum"
        std::vector<ExprNodePtr> args;
    };

    struct IsNullData {
        ExprNodePtr column;
        bool negate; // true = IS NOT NULL
    };

    struct SelectExprData {
        ExprNodePtr expr;
        std::string alias;
    };

    // Payload storage
    LiteralData literal;
    ColumnRefData column_ref;
    ComparisonData comparison;
    LogicalData logical;
    InListData in_list;
    FunctionCallData function_call;
    IsNullData is_null;
    SelectExprData select_expr;

    // --- Factory methods for clean construction ---

    static ExprNodePtr make_literal(LiteralValue val) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::LITERAL;
        node->literal.value = std::move(val);
        return node;
    }

    static ExprNodePtr make_null() {
        return make_literal(std::monostate{});
    }

    static ExprNodePtr make_column(const std::string& name) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::COLUMN_REF;
        node->column_ref.column_name = name;
        return node;
    }

    static ExprNodePtr make_comparison(CompOp op, ExprNodePtr left, ExprNodePtr right) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::COMPARISON;
        node->comparison.op = op;
        node->comparison.left = std::move(left);
        node->comparison.right = std::move(right);
        return node;
    }

    static ExprNodePtr make_logical(LogicOp op, std::vector<ExprNodePtr> children) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::LOGICAL;
        node->logical.op = op;
        node->logical.children = std::move(children);
        return node;
    }

    static ExprNodePtr make_in(ExprNodePtr column, std::vector<ExprNodePtr> values, bool negate) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::IN_LIST;
        node->in_list.column = std::move(column);
        node->in_list.values = std::move(values);
        node->in_list.negate = negate;
        return node;
    }

    static ExprNodePtr make_function(const std::string& name, std::vector<ExprNodePtr> args) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::FUNCTION_CALL;
        node->function_call.function_name = name;
        node->function_call.args = std::move(args);
        return node;
    }

    static ExprNodePtr make_is_null(ExprNodePtr column, bool negate) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::IS_NULL;
        node->is_null.column = std::move(column);
        node->is_null.negate = negate;
        return node;
    }

    static ExprNodePtr make_select_expr(ExprNodePtr expr, const std::string& alias) {
        auto node = std::make_shared<ExprNode>();
        node->type = ExprType::SELECT_EXPR;
        node->select_expr.expr = std::move(expr);
        node->select_expr.alias = alias;
        return node;
    }
};

/**
 * Represents a complete SELECT query as a tree structure.
 * This is the top-level AST node produced by the parse phase.
 */
struct QueryTree {
    std::string from_database;
    std::string from_table;
    std::vector<ExprNodePtr> select_exprs;  // SELECT columns/expressions
    ExprNodePtr where_clause;               // WHERE (nullptr = no filter)
    std::vector<ExprNodePtr> group_by;      // GROUP BY columns
    ExprNodePtr having_clause;              // HAVING (nullptr = none)
    std::vector<std::pair<ExprNodePtr, bool>> order_by; // (expr, is_desc)
    int64_t limit = 0;
    int64_t offset = 0;
};

/**
 * Emit a ClickHouse SQL string from an expression node.
 */
std::string emit_sql(const ExprNodePtr& node);

/**
 * Emit a complete SELECT statement from a QueryTree.
 */
std::string emit_query(const QueryTree& tree);

} // namespace mongoflux
