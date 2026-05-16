package translator

import (
	"fmt"
	"strings"

	"github.com/mongoflux/mongoflux/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Translator converts MongoDB queries to ClickHouse SQL via a two-phase AST.
type Translator struct {
	registry *schema.Registry
}

// NewTranslator creates a new query translator.
func NewTranslator(registry *schema.Registry) *Translator {
	return &Translator{registry: registry}
}

// TranslateFind translates a MongoDB find() to ClickHouse SQL.
func (t *Translator) TranslateFind(collection string, filter, projection, sort bson.D, limit, skip int64) (string, error) {
	tree, err := t.ParseFind(collection, filter, projection, sort, limit, skip)
	if err != nil {
		return "", err
	}
	return EmitQuery(tree), nil
}

// TranslateAggregate translates a MongoDB aggregate() pipeline to ClickHouse SQL.
func (t *Translator) TranslateAggregate(collection string, pipeline []bson.D) (string, error) {
	tree, err := t.ParseAggregate(collection, pipeline)
	if err != nil {
		return "", err
	}
	return EmitQuery(tree), nil
}

// ParseFind parses a find() into a QueryTree.
func (t *Translator) ParseFind(collection string, filter, projection, sort bson.D, limit, skip int64) (*QueryTree, error) {
	mapping := t.registry.Get(collection)
	if mapping == nil {
		return nil, fmt.Errorf("no schema mapping found for collection: %s", collection)
	}

	tree := &QueryTree{
		FromDatabase: mapping.ClickHouseDatabase,
		FromTable:    mapping.ClickHouseTable,
		Limit:        limit,
		Offset:       skip,
	}

	// SELECT columns
	if len(projection) == 0 {
		for _, field := range mapping.Fields {
			tree.SelectExprs = append(tree.SelectExprs, MakeColumn(field.CHColumn))
		}
	} else {
		for _, elem := range projection {
			if v, ok := elem.Value.(int32); ok && v == 1 {
				col := resolveColumn(elem.Key, mapping)
				tree.SelectExprs = append(tree.SelectExprs, MakeColumn(col))
			}
		}
	}

	// WHERE
	if len(filter) > 0 {
		tree.WhereClause = t.parseFilter(filter, mapping)
	}

	// ORDER BY
	if len(sort) > 0 {
		for _, elem := range sort {
			col := resolveColumn(elem.Key, mapping)
			desc := false
			if v, ok := elem.Value.(int32); ok && v == -1 {
				desc = true
			}
			tree.OrderBy = append(tree.OrderBy, OrderByItem{Expr: MakeColumn(col), Desc: desc})
		}
	}

	return tree, nil
}

// ParseAggregate parses an aggregate() pipeline into a QueryTree.
func (t *Translator) ParseAggregate(collection string, pipeline []bson.D) (*QueryTree, error) {
	mapping := t.registry.Get(collection)
	if mapping == nil {
		return nil, fmt.Errorf("no schema mapping found for collection: %s", collection)
	}

	tree := &QueryTree{
		FromDatabase: mapping.ClickHouseDatabase,
		FromTable:    mapping.ClickHouseTable,
	}

	hasGroup := false

	for _, stageDoc := range pipeline {
		for _, elem := range stageDoc {
			stageName := elem.Key

			switch stageName {
			case "$match":
				matchDoc, ok := elem.Value.(bson.D)
				if !ok {
					continue
				}
				condition := t.parseFilter(matchDoc, mapping)
				if condition == nil {
					continue
				}
				if !hasGroup {
					if tree.WhereClause == nil {
						tree.WhereClause = condition
					} else {
						tree.WhereClause = MakeLogical(LogicAND, []*ExprNode{tree.WhereClause, condition})
					}
				} else {
					if tree.HavingClause == nil {
						tree.HavingClause = condition
					} else {
						tree.HavingClause = MakeLogical(LogicAND, []*ExprNode{tree.HavingClause, condition})
					}
				}

			case "$group":
				hasGroup = true
				groupDoc, ok := elem.Value.(bson.D)
				if !ok {
					continue
				}
				t.parseGroupStage(groupDoc, mapping, tree)

			case "$sort":
				sortDoc, ok := elem.Value.(bson.D)
				if !ok {
					continue
				}
				tree.OrderBy = nil
				for _, selem := range sortDoc {
					col := resolveColumn(selem.Key, mapping)
					desc := false
					if v, ok := selem.Value.(int32); ok && v == -1 {
						desc = true
					}
					tree.OrderBy = append(tree.OrderBy, OrderByItem{Expr: MakeColumn(col), Desc: desc})
				}

			case "$limit":
				tree.Limit = toInt64(elem.Value)

			case "$skip":
				tree.Offset = toInt64(elem.Value)

			case "$count":
				countField, ok := elem.Value.(string)
				if !ok {
					continue
				}
				countExpr := MakeFunction("count", []*ExprNode{MakeLiteral("*")})
				tree.SelectExprs = []*ExprNode{MakeSelectExpr(countExpr, countField)}

			case "$project", "$addFields", "$set":
				projDoc, ok := elem.Value.(bson.D)
				if !ok {
					continue
				}
				if len(tree.SelectExprs) == 0 || stageName != "$project" {
					for _, pelem := range projDoc {
						fieldName := pelem.Key
						switch v := pelem.Value.(type) {
						case int32:
							if v == 1 {
								col := resolveColumn(fieldName, mapping)
								tree.SelectExprs = append(tree.SelectExprs, MakeColumn(col))
							}
						case string:
							ref := stripDollar(v)
							col := resolveColumn(ref, mapping)
							tree.SelectExprs = append(tree.SelectExprs, MakeSelectExpr(MakeColumn(col), fieldName))
						case bson.D:
							expr := t.parseAggExpression(v, mapping)
							if expr != nil {
								tree.SelectExprs = append(tree.SelectExprs, MakeSelectExpr(expr, fieldName))
							}
						}
					}
				}

			case "$sample":
				sampleDoc, ok := elem.Value.(bson.D)
				if !ok {
					continue
				}
				for _, s := range sampleDoc {
					if s.Key == "size" {
						tree.Limit = toInt64(s.Value)
					}
				}
				tree.OrderBy = []OrderByItem{{Expr: MakeFunction("rand", nil), Desc: false}}
			}
		}
	}

	// Default SELECT if nothing was specified
	if len(tree.SelectExprs) == 0 {
		for _, field := range mapping.Fields {
			tree.SelectExprs = append(tree.SelectExprs, MakeColumn(field.CHColumn))
		}
	}

	return tree, nil
}

func (t *Translator) parseFilter(filter bson.D, mapping *schema.CollectionMapping) *ExprNode {
	var conditions []*ExprNode

	for _, elem := range filter {
		key := elem.Key

		switch key {
		case "$and":
			arr, ok := elem.Value.(bson.A)
			if ok {
				conditions = append(conditions, t.parseLogical(LogicAND, arr, mapping))
			}
		case "$or":
			arr, ok := elem.Value.(bson.A)
			if ok {
				conditions = append(conditions, t.parseLogical(LogicOR, arr, mapping))
			}
		case "$nor":
			arr, ok := elem.Value.(bson.A)
			if ok {
				orNode := t.parseLogical(LogicOR, arr, mapping)
				conditions = append(conditions, MakeLogical(LogicNOT, []*ExprNode{orNode}))
			}
		default:
			conditions = append(conditions, t.parseExpression(key, elem.Value, mapping))
		}
	}

	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return MakeLogical(LogicAND, conditions)
}

func (t *Translator) parseExpression(field string, value interface{}, mapping *schema.CollectionMapping) *ExprNode {
	chCol := resolveColumn(field, mapping)
	colNode := MakeColumn(chCol)

	doc, ok := value.(bson.D)
	if ok {
		var parts []*ExprNode
		for _, opElem := range doc {
			op := opElem.Key
			switch op {
			case "$gt":
				parts = append(parts, MakeComparison(CompGT, colNode, toLiteralNode(opElem.Value)))
			case "$gte":
				parts = append(parts, MakeComparison(CompGTE, colNode, toLiteralNode(opElem.Value)))
			case "$lt":
				parts = append(parts, MakeComparison(CompLT, colNode, toLiteralNode(opElem.Value)))
			case "$lte":
				parts = append(parts, MakeComparison(CompLTE, colNode, toLiteralNode(opElem.Value)))
			case "$eq":
				parts = append(parts, MakeComparison(CompEQ, colNode, toLiteralNode(opElem.Value)))
			case "$ne":
				parts = append(parts, MakeComparison(CompNE, colNode, toLiteralNode(opElem.Value)))
			case "$in":
				parts = append(parts, t.parseIn(chCol, opElem.Value, false))
			case "$nin":
				parts = append(parts, t.parseIn(chCol, opElem.Value, true))
			case "$exists":
				exists := toBool(opElem.Value)
				parts = append(parts, MakeIsNull(colNode, exists))
			case "$regex":
				pattern := toString(opElem.Value)
				parts = append(parts, MakeFunction("match", []*ExprNode{colNode, MakeLiteral(pattern)}))
			}
		}
		if len(parts) == 0 {
			return MakeLiteral(true)
		}
		if len(parts) == 1 {
			return parts[0]
		}
		return MakeLogical(LogicAND, parts)
	}

	// Simple equality
	return MakeComparison(CompEQ, colNode, toLiteralNode(value))
}

func (t *Translator) parseLogical(op LogicOp, conditions bson.A, mapping *schema.CollectionMapping) *ExprNode {
	var children []*ExprNode
	for _, item := range conditions {
		doc, ok := item.(bson.D)
		if ok {
			child := t.parseFilter(doc, mapping)
			if child != nil {
				children = append(children, child)
			}
		}
	}
	if len(children) == 0 {
		return nil
	}
	if len(children) == 1 && op != LogicNOT {
		return children[0]
	}
	return MakeLogical(op, children)
}

func (t *Translator) parseIn(chColumn string, value interface{}, negate bool) *ExprNode {
	colNode := MakeColumn(chColumn)
	var valueNodes []*ExprNode

	arr, ok := value.(bson.A)
	if ok {
		for _, v := range arr {
			valueNodes = append(valueNodes, toLiteralNode(v))
		}
	}

	return MakeIn(colNode, valueNodes, negate)
}

func (t *Translator) parseGroupStage(stage bson.D, mapping *schema.CollectionMapping, tree *QueryTree) {
	tree.SelectExprs = nil
	tree.GroupBy = nil

	for _, elem := range stage {
		key := elem.Key

		if key == "_id" {
			switch v := elem.Value.(type) {
			case string:
				field := stripDollar(v)
				col := resolveColumn(field, mapping)
				colNode := MakeColumn(col)
				tree.GroupBy = append(tree.GroupBy, colNode)
				tree.SelectExprs = append(tree.SelectExprs, colNode)
			case bson.D:
				for _, gelem := range v {
					alias := gelem.Key
					field := stripDollar(toString(gelem.Value))
					col := resolveColumn(field, mapping)
					colNode := MakeColumn(col)
					tree.GroupBy = append(tree.GroupBy, colNode)
					tree.SelectExprs = append(tree.SelectExprs, MakeSelectExpr(colNode, alias))
				}
			}
			// _id: null → aggregate entire collection, no GROUP BY
			continue
		}

		// Accumulator fields
		accDoc, ok := elem.Value.(bson.D)
		if !ok {
			continue
		}
		for _, accElem := range accDoc {
			accOp := accElem.Key
			funcNode := t.parseAccumulator(accOp, accElem.Value, mapping)
			tree.SelectExprs = append(tree.SelectExprs, MakeSelectExpr(funcNode, key))
		}
	}
}

func (t *Translator) parseAccumulator(accOp string, value interface{}, mapping *schema.CollectionMapping) *ExprNode {
	switch accOp {
	case "$sum":
		if v, ok := value.(int32); ok && v == 1 {
			return MakeFunction("count", []*ExprNode{MakeLiteral("*")})
		}
		if s, ok := value.(string); ok {
			field := stripDollar(s)
			return MakeFunction("sum", []*ExprNode{MakeColumn(resolveColumn(field, mapping))})
		}
		if doc, ok := value.(bson.D); ok {
			expr := t.parseAggExpression(doc, mapping)
			if expr != nil {
				return MakeFunction("sum", []*ExprNode{expr})
			}
		}
	case "$avg":
		if s, ok := value.(string); ok {
			return MakeFunction("avg", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$min":
		if s, ok := value.(string); ok {
			return MakeFunction("min", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$max":
		if s, ok := value.(string); ok {
			return MakeFunction("max", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$count":
		return MakeFunction("count", []*ExprNode{MakeLiteral("*")})
	case "$first":
		if s, ok := value.(string); ok {
			return MakeFunction("any", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$last":
		if s, ok := value.(string); ok {
			return MakeFunction("anyLast", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$stdDevPop":
		if s, ok := value.(string); ok {
			return MakeFunction("stddevPop", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$stdDevSamp":
		if s, ok := value.(string); ok {
			return MakeFunction("stddevSamp", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$push":
		if s, ok := value.(string); ok {
			return MakeFunction("groupArray", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	case "$addToSet":
		if s, ok := value.(string); ok {
			return MakeFunction("groupUniqArray", []*ExprNode{MakeColumn(resolveColumn(stripDollar(s), mapping))})
		}
	}
	return MakeFunction("count", []*ExprNode{MakeLiteral("*")})
}

func (t *Translator) parseAggExpression(doc bson.D, mapping *schema.CollectionMapping) *ExprNode {
	if len(doc) == 0 {
		return MakeNull()
	}

	op := doc[0].Key
	val := doc[0].Value

	// Arithmetic
	switch op {
	case "$multiply", "$add", "$subtract", "$divide", "$mod":
		arr, ok := val.(bson.A)
		if ok && len(arr) >= 2 {
			chOp := map[string]string{
				"$multiply": "multiply", "$add": "plus", "$subtract": "minus",
				"$divide": "divide", "$mod": "modulo",
			}[op]
			args := t.parseExprArgs(arr, mapping)
			result := args[0]
			for i := 1; i < len(args); i++ {
				result = MakeFunction(chOp, []*ExprNode{result, args[i]})
			}
			return result
		}
	case "$abs":
		return MakeFunction("abs", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$ceil":
		return MakeFunction("ceil", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$floor":
		return MakeFunction("floor", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$sqrt":
		return MakeFunction("sqrt", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$round":
		if arr, ok := val.(bson.A); ok {
			args := t.parseExprArgs(arr, mapping)
			return MakeFunction("round", args)
		}
		return MakeFunction("round", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$pow":
		if arr, ok := val.(bson.A); ok && len(arr) >= 2 {
			args := t.parseExprArgs(arr, mapping)
			return MakeFunction("pow", []*ExprNode{args[0], args[1]})
		}
	}

	// String
	switch op {
	case "$concat":
		if arr, ok := val.(bson.A); ok {
			return MakeFunction("concat", t.parseExprArgs(arr, mapping))
		}
	case "$toUpper":
		return MakeFunction("upper", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$toLower":
		return MakeFunction("lower", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$trim":
		return MakeFunction("trimBoth", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	}

	// Date
	switch op {
	case "$year":
		return MakeFunction("toYear", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$month":
		return MakeFunction("toMonth", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$dayOfMonth":
		return MakeFunction("toDayOfMonth", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$dayOfWeek":
		return MakeFunction("toDayOfWeek", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	case "$dayOfYear":
		return MakeFunction("toDayOfYear", []*ExprNode{t.parseFieldOrLiteral(val, mapping)})
	}

	// Conditional
	switch op {
	case "$cond":
		if arr, ok := val.(bson.A); ok && len(arr) >= 3 {
			args := t.parseExprArgs(arr, mapping)
			return MakeFunction("if", []*ExprNode{args[0], args[1], args[2]})
		}
		if doc, ok := val.(bson.D); ok {
			var ifNode, thenNode, elseNode *ExprNode
			for _, e := range doc {
				switch e.Key {
				case "if":
					ifNode = t.parseFieldOrLiteral(e.Value, mapping)
				case "then":
					thenNode = t.parseFieldOrLiteral(e.Value, mapping)
				case "else":
					elseNode = t.parseFieldOrLiteral(e.Value, mapping)
				}
			}
			if ifNode != nil && thenNode != nil && elseNode != nil {
				return MakeFunction("if", []*ExprNode{ifNode, thenNode, elseNode})
			}
		}
	}

	return MakeNull()
}

func (t *Translator) parseExprArgs(arr bson.A, mapping *schema.CollectionMapping) []*ExprNode {
	var args []*ExprNode
	for _, a := range arr {
		args = append(args, t.parseFieldOrLiteral(a, mapping))
	}
	return args
}

func (t *Translator) parseFieldOrLiteral(val interface{}, mapping *schema.CollectionMapping) *ExprNode {
	switch v := val.(type) {
	case string:
		if strings.HasPrefix(v, "$") {
			return MakeColumn(resolveColumn(v[1:], mapping))
		}
		return MakeLiteral(v)
	case int32:
		return MakeLiteral(v)
	case int64:
		return MakeLiteral(v)
	case float64:
		return MakeLiteral(v)
	case bool:
		return MakeLiteral(v)
	case bson.D:
		return t.parseAggExpression(v, mapping)
	case nil:
		return MakeNull()
	}
	return MakeNull()
}

// Helpers

func resolveColumn(mongoField string, mapping *schema.CollectionMapping) string {
	for _, f := range mapping.Fields {
		if f.MongoField == mongoField {
			return f.CHColumn
		}
	}
	return mongoField
}

func stripDollar(field string) string {
	if strings.HasPrefix(field, "$") {
		return field[1:]
	}
	return field
}

func toLiteralNode(val interface{}) *ExprNode {
	switch v := val.(type) {
	case int32:
		return MakeLiteral(v)
	case int64:
		return MakeLiteral(v)
	case float64:
		return MakeLiteral(v)
	case string:
		return MakeLiteral(v)
	case bool:
		return MakeLiteral(v)
	case primitive.ObjectID:
		return MakeLiteral(v.Hex())
	case nil:
		return MakeNull()
	}
	return MakeNull()
}

func toInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return 0
}

func toBool(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case int32:
		return v != 0
	}
	return false
}

func toString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
