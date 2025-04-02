package sql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/convert"

	"github.com/oarkflow/etl/pkg/utils"
)

type EvalContext struct {
	OuterRow         utils.Record
	CurrentResultSet []utils.Record
	Errors           []string
}

func NewEvalContext() *EvalContext {
	return &EvalContext{
		OuterRow: make(map[string]any),
		Errors:   []string{},
	}
}

func (ctx *EvalContext) evalExpression(expr Expression, row utils.Record) any {
	switch e := expr.(type) {
	case *Identifier:
		return ctx.evalIdentifier(e, row)
	case *Literal:
		return e.Value
	case *AliasExpression:
		return ctx.evalExpression(e.Expr, row)
	case *BinaryExpression:
		return ctx.evalBinaryExpression(e, row)
	case *InExpression:
		return ctx.evalInExpression(e, row)
	case *LikeExpression:
		return ctx.evalLikeExpression(e, row)
	case *FunctionCall:
		return ctx.evalFunctionCall(e, row)
	case *WindowFunction:
		return ctx.evalWindowFunction(e, row)
	case *CaseExpression:
		return ctx.evalCaseExpression(e, row)
	case *Star:

		return nil
	case *Subquery:
		return ctx.evalSubquery(e, row)
	default:
		ctx.logError(fmt.Sprintf("Unsupported expression type: %T", expr))
		return nil
	}
}

func (ctx *EvalContext) evalIdentifier(id *Identifier, row utils.Record) any {
	// Support for alias fields: check full key first then fallback.
	if strings.Contains(id.Value, ".") {
		// Try full key (e.g. "t1.work_item_id")
		if val, ok := row[id.Value]; ok {
			return val
		}
		// Split alias and field; then try field-only lookup.
		parts := strings.SplitN(id.Value, ".", 2)
		if val, ok := row[parts[1]]; ok {
			return val
		}
		return nil
	}
	upperVal := strings.ToUpper(id.Value)
	if upperVal == "CURRENT_DATE" {
		return time.Now().Format("2006-01-02")
	}

	if v, ok := row[id.Value]; ok {
		return v
	}

	if ctx.OuterRow != nil {
		if v, ok := ctx.OuterRow[id.Value]; ok {
			return v
		}
	}
	return nil
}

func (ctx *EvalContext) evalInExpression(e *InExpression, row utils.Record) any {
	leftVal := ctx.evalExpression(e.Left, row)
	found := false
	for _, exp := range e.List {
		if utils.CompareValues(ctx.evalExpression(exp, row), leftVal) == 0 {
			found = true
			break
		}
	}
	if e.Not {
		return !found
	}
	return found
}

func (ctx *EvalContext) evalLikeExpression(e *LikeExpression, row utils.Record) any {
	leftVal := ctx.evalExpression(e.Left, row)
	pattern := ctx.evalExpression(e.Pattern, row)
	s, ok1 := leftVal.(string)
	pat, ok2 := pattern.(string)
	if ok1 && ok2 {

		match := sqlLikeMatch(s, pat)
		if e.Not {
			return !match
		}
		return match
	}
	return false
}

func (ctx *EvalContext) evalFunctionCall(fc *FunctionCall, row utils.Record) any {
	name := strings.ToUpper(fc.FunctionName)
	switch name {
	case "COALESCE", "CONCAT", "IF", "SUBSTR", "LENGTH", "UPPER", "LOWER", "TO_DATE", "TO_NUMBER":
		return ctx.evalScalarFunction(fc, row)
	default:
		ctx.logError("Unsupported function: " + name)
		return nil
	}
}

func (ctx *EvalContext) evalWindowFunction(e *WindowFunction, row utils.Record) any {
	fnName := strings.ToUpper(e.Func.TokenLiteral())
	switch fnName {
	case "COUNT":
		key := ctx.getPartitionKey(row, e.PartitionBy)
		count := 0
		for _, r := range ctx.CurrentResultSet {
			if ctx.getPartitionKey(r, e.PartitionBy) == key {
				count++
			}
		}
		return count
	case "ROW_NUMBER":
		key := ctx.getPartitionKey(row, e.PartitionBy)
		var partitionRows []utils.Record
		for _, r := range ctx.CurrentResultSet {
			if ctx.getPartitionKey(r, e.PartitionBy) == key {
				partitionRows = append(partitionRows, r)
			}
		}

		if e.OrderBy != nil && len(e.OrderBy.Fields) > 0 {
			sort.SliceStable(partitionRows, func(i, j int) bool {
				vi := ctx.evalExpression(e.OrderBy.Fields[0], partitionRows[i])
				vj := ctx.evalExpression(e.OrderBy.Fields[0], partitionRows[j])
				return utils.CompareValues(vi, vj) < 0
			})
		}

		for i, r := range partitionRows {
			if utils.DeepEqual(r, row) {
				return i + 1
			}
		}
		return nil
	default:

		return ctx.evalExpression(e.Func, row)
	}
}

func (ctx *EvalContext) evalCaseExpression(e *CaseExpression, row utils.Record) any {
	for _, wc := range e.WhenClauses {
		cond := ctx.evalExpression(wc.Condition, row)
		if b, ok := cond.(bool); ok && b {
			return ctx.evalExpression(wc.Result, row)
		}
	}
	if e.Else != nil {
		return ctx.evalExpression(e.Else, row)
	}
	return nil
}

func (ctx *EvalContext) evalSubquery(e *Subquery, row utils.Record) any {

	oldOuter := ctx.OuterRow
	ctx.OuterRow = row
	subRows, err := e.Query.executeQuery(loadDataForSubquery())

	ctx.OuterRow = oldOuter
	if err != nil || len(subRows) == 0 {
		ctx.logError(fmt.Sprintf("Subquery execution error: %v", err))
		return nil
	}

	for _, v := range subRows[0] {
		return v
	}
	return nil
}

func (ctx *EvalContext) evalBinaryExpression(e *BinaryExpression, row utils.Record) any {
	left := ctx.evalExpression(e.Left, row)
	right := ctx.evalExpression(e.Right, row)

	if isArithmeticOperator(e.Operator) {
		lnum, okA := convert.ToFloat64(left)
		rnum, okB := convert.ToFloat64(right)
		if okA && okB {
			switch e.Operator {
			case PLUS:
				return lnum + rnum
			case MINUS:
				return lnum - rnum
			case ASTERISK:
				return lnum * rnum
			case SLASH:
				if rnum == 0 {
					ctx.logError("Division by zero")
					return nil
				}
				return lnum / rnum
			}
		} else if e.Operator == MINUS {

			leftStr, okL := left.(string)
			rightStr, okR := right.(string)
			if okL && okR {
				t1, err1 := time.Parse("2006-01-02", leftStr)
				t2, err2 := time.Parse("2006-01-02", rightStr)
				if err1 == nil && err2 == nil {
					diff := t1.Sub(t2)
					return int(diff.Hours() / 24)
				}
			}
		}
	}

	switch e.Operator {
	case "=":
		return utils.CompareValues(left, right) == 0
	case "!=":
		return utils.CompareValues(left, right) != 0
	case "<":
		return utils.CompareValues(left, right) < 0
	case ">":
		return utils.CompareValues(left, right) > 0
	case "<=":
		return utils.CompareValues(left, right) <= 0
	case ">=":
		return utils.CompareValues(left, right) >= 0
	case "IS NULL":
		return left == nil
	case "IS NOT NULL":
		return left != nil
	case "AND":
		leftVal, _ := convert.ToBool(left)
		rightVal, _ := convert.ToBool(right)
		return leftVal && rightVal
	default:
		ctx.logError("Unsupported binary operator: " + e.Operator)
		return nil
	}
}

func (ctx *EvalContext) evalScalarFunction(fc *FunctionCall, row utils.Record) any {
	name := strings.ToUpper(fc.FunctionName)
	switch name {
	case "COALESCE":
		for _, arg := range fc.Args {
			val := ctx.evalExpression(arg, row)
			if !isNilOrEmpty(val) {
				return val
			}
		}
		return nil
	case "CONCAT":
		var parts []string
		for _, arg := range fc.Args {
			val := ctx.evalExpression(arg, row)
			parts = append(parts, fmt.Sprintf("%v", val))
		}
		return strings.Join(parts, "")
	case "IF":
		if len(fc.Args) < 3 {
			ctx.logError("IF requires at least 3 arguments")
			return nil
		}
		cond := ctx.evalExpression(fc.Args[0], row)
		if b, ok := cond.(bool); ok && b {
			return ctx.evalExpression(fc.Args[1], row)
		}
		return ctx.evalExpression(fc.Args[2], row)
	case "SUBSTR":
		if len(fc.Args) < 2 {
			ctx.logError("SUBSTR requires at least 2 arguments")
			return nil
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[0], row))
		startVal, ok := convert.ToFloat64(ctx.evalExpression(fc.Args[1], row))
		if !ok {
			return nil
		}
		start := int(startVal) - 1
		if start < 0 || start >= len(s) {
			return ""
		}
		if len(fc.Args) == 3 {
			lenVal, ok := convert.ToFloat64(ctx.evalExpression(fc.Args[2], row))
			if !ok {
				return nil
			}
			length := int(lenVal)
			end := start + length
			if end > len(s) {
				end = len(s)
			}
			return s[start:end]
		}
		return s[start:]
	case "LENGTH":
		s := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[0], row))
		return len(s)
	case "UPPER":
		s := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[0], row))
		return strings.ToUpper(s)
	case "LOWER":
		s := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[0], row))
		return strings.ToLower(s)
	case "TO_DATE":
		if len(fc.Args) < 2 {
			ctx.logError("TO_DATE requires 2 arguments")
			return nil
		}
		dateStr := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[0], row))
		formatStr := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[1], row))
		layout := sqlDateFormatToGoLayout(formatStr)
		t, err := time.Parse(layout, dateStr)
		if err != nil {
			ctx.logError("TO_DATE parse error: " + err.Error())
			return nil
		}

		return t.Format("2006-01-02")
	case "TO_NUMBER":
		if len(fc.Args) < 1 {
			return nil
		}
		numStr := fmt.Sprintf("%v", ctx.evalExpression(fc.Args[0], row))
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			ctx.logError("TO_NUMBER parse error: " + err.Error())
			return nil
		}
		return f
	default:
		ctx.logError("Unsupported scalar function: " + name)
		return nil
	}
}

func (ctx *EvalContext) getPartitionKey(row utils.Record, exprs []Expression) string {
	if len(exprs) == 0 {
		return "all"
	}
	var parts []string
	for _, expr := range exprs {
		val := ctx.evalExpression(expr, row)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "|")
}

func (ctx *EvalContext) logError(msg string) {
	ctx.Errors = append(ctx.Errors, msg)
}

func isNilOrEmpty(val any) bool {
	if val == nil {
		return true
	}
	if str, ok := val.(string); ok && str == "" {
		return true
	}
	return false
}

func isArithmeticOperator(op string) bool {
	return op == PLUS || op == MINUS || op == ASTERISK || op == SLASH
}
