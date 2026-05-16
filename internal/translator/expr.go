package translator

import (
	"fmt"
	"strings"
)

// ExprType represents the type of expression node.
type ExprType int

const (
	ExprLiteral ExprType = iota
	ExprColumnRef
	ExprComparison
	ExprLogical
	ExprInList
	ExprFunctionCall
	ExprIsNull
	ExprSelectExpr
)

// CompOp represents comparison operators.
type CompOp int

const (
	CompEQ CompOp = iota
	CompNE
	CompGT
	CompGTE
	CompLT
	CompLTE
)

func (op CompOp) String() string {
	switch op {
	case CompEQ:
		return "="
	case CompNE:
		return "!="
	case CompGT:
		return ">"
	case CompGTE:
		return ">="
	case CompLT:
		return "<"
	case CompLTE:
		return "<="
	}
	return "="
}

// LogicOp represents logical operators.
type LogicOp int

const (
	LogicAND LogicOp = iota
	LogicOR
	LogicNOT
)

// ExprNode is the expression tree node.
type ExprNode struct {
	Type ExprType

	// Literal
	LiteralVal interface{} // nil, int32, int64, float64, bool, string

	// ColumnRef
	ColumnName string

	// Comparison
	CompOp    CompOp
	CompLeft  *ExprNode
	CompRight *ExprNode

	// Logical
	LogicOp  LogicOp
	Children []*ExprNode

	// InList
	InColumn *ExprNode
	InValues []*ExprNode
	InNegate bool

	// FunctionCall
	FuncName string
	FuncArgs []*ExprNode

	// IsNull
	IsNullCol    *ExprNode
	IsNullNegate bool

	// SelectExpr
	SelectExpr  *ExprNode
	SelectAlias string
}

// Factory functions

func MakeLiteral(val interface{}) *ExprNode {
	return &ExprNode{Type: ExprLiteral, LiteralVal: val}
}

func MakeNull() *ExprNode {
	return &ExprNode{Type: ExprLiteral, LiteralVal: nil}
}

func MakeColumn(name string) *ExprNode {
	return &ExprNode{Type: ExprColumnRef, ColumnName: name}
}

func MakeComparison(op CompOp, left, right *ExprNode) *ExprNode {
	return &ExprNode{Type: ExprComparison, CompOp: op, CompLeft: left, CompRight: right}
}

func MakeLogical(op LogicOp, children []*ExprNode) *ExprNode {
	return &ExprNode{Type: ExprLogical, LogicOp: op, Children: children}
}

func MakeIn(column *ExprNode, values []*ExprNode, negate bool) *ExprNode {
	return &ExprNode{Type: ExprInList, InColumn: column, InValues: values, InNegate: negate}
}

func MakeFunction(name string, args []*ExprNode) *ExprNode {
	return &ExprNode{Type: ExprFunctionCall, FuncName: name, FuncArgs: args}
}

func MakeIsNull(column *ExprNode, negate bool) *ExprNode {
	return &ExprNode{Type: ExprIsNull, IsNullCol: column, IsNullNegate: negate}
}

func MakeSelectExpr(expr *ExprNode, alias string) *ExprNode {
	return &ExprNode{Type: ExprSelectExpr, SelectExpr: expr, SelectAlias: alias}
}

// QueryTree represents a complete SELECT query.
type QueryTree struct {
	FromDatabase string
	FromTable    string
	SelectExprs  []*ExprNode
	WhereClause  *ExprNode
	GroupBy      []*ExprNode
	HavingClause *ExprNode
	OrderBy      []OrderByItem
	Limit        int64
	Offset       int64
}

// OrderByItem is an ORDER BY expression with direction.
type OrderByItem struct {
	Expr *ExprNode
	Desc bool
}

// EmitSQL converts an expression node to a SQL string.
func EmitSQL(node *ExprNode) string {
	if node == nil {
		return ""
	}

	switch node.Type {
	case ExprLiteral:
		return emitLiteral(node.LiteralVal)

	case ExprColumnRef:
		return quoteIdent(node.ColumnName)

	case ExprComparison:
		left := EmitSQL(node.CompLeft)
		right := EmitSQL(node.CompRight)
		return left + " " + node.CompOp.String() + " " + right

	case ExprLogical:
		if node.LogicOp == LogicNOT {
			if len(node.Children) == 1 {
				return "NOT (" + EmitSQL(node.Children[0]) + ")"
			}
			return "NOT (1)"
		}
		opStr := " AND "
		if node.LogicOp == LogicOR {
			opStr = " OR "
		}
		parts := make([]string, 0, len(node.Children))
		for _, child := range node.Children {
			if len(node.Children) > 1 {
				parts = append(parts, "("+EmitSQL(child)+")")
			} else {
				parts = append(parts, EmitSQL(child))
			}
		}
		return strings.Join(parts, opStr)

	case ExprInList:
		var sb strings.Builder
		sb.WriteString(EmitSQL(node.InColumn))
		if node.InNegate {
			sb.WriteString(" NOT IN (")
		} else {
			sb.WriteString(" IN (")
		}
		for i, v := range node.InValues {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(EmitSQL(v))
		}
		sb.WriteString(")")
		return sb.String()

	case ExprFunctionCall:
		var sb strings.Builder
		sb.WriteString(node.FuncName)
		sb.WriteString("(")
		// Special case: count(*)
		if node.FuncName == "count" && len(node.FuncArgs) == 1 &&
			node.FuncArgs[0].Type == ExprLiteral {
			if s, ok := node.FuncArgs[0].LiteralVal.(string); ok && s == "*" {
				sb.WriteString("*")
			} else {
				sb.WriteString(EmitSQL(node.FuncArgs[0]))
			}
		} else {
			for i, arg := range node.FuncArgs {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(EmitSQL(arg))
			}
		}
		sb.WriteString(")")
		return sb.String()

	case ExprIsNull:
		col := EmitSQL(node.IsNullCol)
		if node.IsNullNegate {
			return col + " IS NOT NULL"
		}
		return col + " IS NULL"

	case ExprSelectExpr:
		exprSQL := EmitSQL(node.SelectExpr)
		if node.SelectAlias == "" {
			return exprSQL
		}
		return exprSQL + " AS " + quoteIdent(node.SelectAlias)
	}

	return "1"
}

// EmitQuery builds a complete SELECT statement from a QueryTree.
func EmitQuery(tree *QueryTree) string {
	var sb strings.Builder

	// SELECT
	sb.WriteString("SELECT ")
	if len(tree.SelectExprs) == 0 {
		sb.WriteString("*")
	} else {
		for i, expr := range tree.SelectExprs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(EmitSQL(expr))
		}
	}

	// FROM
	sb.WriteString(fmt.Sprintf(" FROM %s.%s", tree.FromDatabase, quoteIdent(tree.FromTable)))

	// WHERE
	if tree.WhereClause != nil {
		whereSQL := EmitSQL(tree.WhereClause)
		if whereSQL != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(whereSQL)
		}
	}

	// GROUP BY
	if len(tree.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, g := range tree.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(EmitSQL(g))
		}
	}

	// HAVING
	if tree.HavingClause != nil {
		havingSQL := EmitSQL(tree.HavingClause)
		if havingSQL != "" {
			sb.WriteString(" HAVING ")
			sb.WriteString(havingSQL)
		}
	}

	// ORDER BY
	if len(tree.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ob := range tree.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(EmitSQL(ob.Expr))
			if ob.Desc {
				sb.WriteString(" DESC")
			}
		}
	}

	// LIMIT / OFFSET
	if tree.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", tree.Limit))
	}
	if tree.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", tree.Offset))
	}

	return sb.String()
}

func quoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func quoteLiteral(val string) string {
	escaped := strings.ReplaceAll(val, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return "'" + escaped + "'"
}

func emitLiteral(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case string:
		return quoteLiteral(v)
	default:
		return "NULL"
	}
}
