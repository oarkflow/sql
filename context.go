package sql

import (
	"context"
	"fmt"
	"math" // <-- added import for math functions
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/convert"

	"github.com/oarkflow/sql/pkg/utils"
)

type EvalContext struct {
	OuterRow         utils.Record
	CurrentResultSet []utils.Record
	Errors           []string
	CurrentAlias     string
}

type quantifiedValues struct {
	Mode   string
	Values []any
}

func NewEvalContext() *EvalContext {
	return &EvalContext{
		OuterRow: make(map[string]any),
		Errors:   []string{},
	}
}

func (ctx *EvalContext) evalExpression(c context.Context, expr Expression, row utils.Record) any {
	switch e := expr.(type) {
	case *Identifier:
		return ctx.evalIdentifier(c, e, row)
	case *Literal:
		return e.Value
	case *ArrayLiteral:
		out := make([]any, 0, len(e.Elements))
		for _, el := range e.Elements {
			out = append(out, ctx.evalExpression(c, el, row))
		}
		return out
	case *AliasExpression:
		return ctx.evalExpression(c, e.Expr, row)
	case *BinaryExpression:
		return ctx.evalBinaryExpression(c, e, row)
	case *InExpression:
		return ctx.evalInExpression(c, e, row)
	case *LikeExpression:
		return ctx.evalLikeExpression(c, e, row)
	case *FunctionCall:
		return ctx.evalFunctionCall(c, e, row)
	case *WindowFunction:
		return ctx.evalWindowFunction(c, e, row)
	case *CaseExpression:
		return ctx.evalCaseExpression(c, e, row)
	case *Star:
		return nil
	case *Subquery:
		return ctx.evalSubquery(c, e, row)
	case *ExistsExpression:
		return ctx.evalExistsExpression(c, e, row)
	case *PrefixExpression:
		return ctx.evalPrefixExpression(c, e, row)
	default:
		ctx.logError(fmt.Sprintf("Unsupported expression type: %T", expr))
		return nil
	}
}

func (ctx *EvalContext) evalIdentifier(_ context.Context, id *Identifier, row utils.Record) any {
	upperVal := strings.ToUpper(id.Value)
	if upperVal == "CURRENT_DATE" {
		return time.Now().Format("2006-01-02")
	}

	path := normalizeIdentifierPath(id.Value)
	if val, ok := resolvePathFromRecord(row, path); ok {
		return val
	}
	if ctx.CurrentAlias != "" && !strings.Contains(path, ".") {
		if val, ok := resolvePathFromRecord(row, ctx.CurrentAlias+"."+path); ok {
			return val
		}
	}
	if ctx.CurrentAlias != "" {
		aliasedPath := ctx.CurrentAlias + "." + path
		if val, ok := resolvePathFromRecord(row, aliasedPath); ok {
			return val
		}
	}
	if ctx.OuterRow != nil {
		if v, ok := resolvePathFromRecord(ctx.OuterRow, path); ok {
			return v
		}
	}
	return nil
}

func (ctx *EvalContext) evalInExpression(c context.Context, e *InExpression, row utils.Record) any {
	leftVal := ctx.evalExpression(c, e.Left, row)
	found := false
	var candidateVals []any

	if e.Subquery != nil {
		// Handle subquery IN
		subRows, err := e.Subquery.Query.executeQuery(c, nil)
		if err != nil {
			return false
		}
		for _, subRow := range subRows {
			// For subquery, we assume it returns a single column
			for _, val := range subRow {
				candidateVals = append(candidateVals, val)
			}
		}
	} else {
		// Handle expression list IN
		for _, exp := range e.List {
			candidateVals = append(candidateVals, ctx.evalExpression(c, exp, row))
		}
	}
	found = valueInList(leftVal, candidateVals)

	if e.Not {
		return !found
	}
	return found
}

func (ctx *EvalContext) evalLikeExpression(c context.Context, e *LikeExpression, row utils.Record) any {
	leftVal := ctx.evalExpression(c, e.Left, row)
	pattern := ctx.evalExpression(c, e.Pattern, row)
	s, ok1 := leftVal.(string)
	pat, ok2 := pattern.(string)
	if ok1 && ok2 {
		// New: enable case-insensitive matching when pattern starts with "i:"
		if strings.HasPrefix(pat, "i:") {
			pat = pat[2:]
			if e.Not {
				return !strings.Contains(strings.ToLower(s), strings.ToLower(pat))
			}
			return strings.Contains(strings.ToLower(s), strings.ToLower(pat))
		}
		match := sqlLikeMatch(s, pat)
		if e.Not {
			return !match
		}
		return match
	}
	return false
}

func (ctx *EvalContext) evalPrefixExpression(c context.Context, e *PrefixExpression, row utils.Record) any {
	val := ctx.evalExpression(c, e.Right, row)
	switch strings.ToUpper(e.Operator) {
	case "NOT":
		b, _ := convert.ToBool(val)
		return !b
	case "-":
		if num, ok := convert.ToFloat64(val); ok {
			return -num
		}
		return nil
	default:
		ctx.logError("Unsupported prefix operator: " + e.Operator)
		return nil
	}
}

func (ctx *EvalContext) evalFunctionCall(c context.Context, fc *FunctionCall, row utils.Record) any {
	name := strings.ToUpper(fc.FunctionName)
	switch name {
	case "COALESCE", "CONCAT", "IF", "SUBSTR", "LENGTH", "UPPER", "LOWER", "TO_DATE", "TO_NUMBER", "NOW",
		"CURRENT_TIMESTAMP", "ROUND", "DATEDIFF", "ANY", "ALL", "CAST", "JSON_EXTRACT", "JSON_EXISTS", "JSON_VALUE":
		return ctx.evalScalarFunction(c, fc, row)
	default:
		ctx.logError("Unsupported function: " + name)
		return nil
	}
}

func (ctx *EvalContext) evalWindowFunction(c context.Context, e *WindowFunction, row utils.Record) any {
	fnName := strings.ToUpper(e.Func.TokenLiteral())
	switch fnName {
	case "COUNT":
		key := ctx.getPartitionKey(c, row, e.PartitionBy)
		count := 0
		for _, r := range ctx.CurrentResultSet {
			if ctx.getPartitionKey(c, r, e.PartitionBy) == key {
				count++
			}
		}
		return count
	case "ROW_NUMBER":
		key := ctx.getPartitionKey(c, row, e.PartitionBy)
		var partitionRows []utils.Record
		for _, r := range ctx.CurrentResultSet {
			if ctx.getPartitionKey(c, r, e.PartitionBy) == key {
				partitionRows = append(partitionRows, r)
			}
		}

		if e.OrderBy != nil && len(e.OrderBy.Fields) > 0 {
			sort.SliceStable(partitionRows, func(i, j int) bool {
				vi := ctx.evalExpression(c, e.OrderBy.Fields[0], partitionRows[i])
				vj := ctx.evalExpression(c, e.OrderBy.Fields[0], partitionRows[j])
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
		return ctx.evalExpression(c, e.Func, row)
	}
}

func (ctx *EvalContext) evalCaseExpression(c context.Context, e *CaseExpression, row utils.Record) any {
	for _, wc := range e.WhenClauses {
		cond := ctx.evalExpression(c, wc.Condition, row)
		if b, ok := cond.(bool); ok && b {
			return ctx.evalExpression(c, wc.Result, row)
		}
	}
	if e.Else != nil {
		return ctx.evalExpression(c, e.Else, row)
	}
	return nil
}

func (ctx *EvalContext) evalSubquery(c context.Context, e *Subquery, row utils.Record) any {
	oldOuter := ctx.OuterRow
	ctx.OuterRow = row
	subRows, err := e.Query.executeQuery(c, loadDataForSubquery())

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

func (ctx *EvalContext) evalExistsExpression(c context.Context, e *ExistsExpression, _ utils.Record) any {
	subRows, err := e.Subquery.Query.executeQuery(c, nil)
	if err != nil || len(subRows) == 0 {
		return false
	}
	return true
}

func (ctx *EvalContext) evalBinaryExpression(c context.Context, e *BinaryExpression, row utils.Record) any {
	left := ctx.evalExpression(c, e.Left, row)
	right := ctx.evalExpression(c, e.Right, row)

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
	case "||":
		leftStr := fmt.Sprintf("%v", left)
		rightStr := fmt.Sprintf("%v", right)
		return leftStr + rightStr
	case "=":
		if result, ok := compareWithQuantified("=", left, right); ok {
			return result
		}
		if result, ok := compareWithCollection("=", left, right); ok {
			return result
		}
		return utils.CompareValues(left, right) == 0
	case "!=":
		if result, ok := compareWithQuantified("!=", left, right); ok {
			return result
		}
		if result, ok := compareWithCollection("!=", left, right); ok {
			return result
		}
		return utils.CompareValues(left, right) != 0
	case "<":
		if result, ok := compareWithQuantified("<", left, right); ok {
			return result
		}
		if result, ok := compareWithCollection("<", left, right); ok {
			return result
		}
		return utils.CompareValues(left, right) < 0
	case ">":
		if result, ok := compareWithQuantified(">", left, right); ok {
			return result
		}
		if result, ok := compareWithCollection(">", left, right); ok {
			return result
		}
		return utils.CompareValues(left, right) > 0
	case "<=":
		if result, ok := compareWithQuantified("<=", left, right); ok {
			return result
		}
		if result, ok := compareWithCollection("<=", left, right); ok {
			return result
		}
		return utils.CompareValues(left, right) <= 0
	case ">=":
		if result, ok := compareWithQuantified(">=", left, right); ok {
			return result
		}
		if result, ok := compareWithCollection(">=", left, right); ok {
			return result
		}
		return utils.CompareValues(left, right) >= 0
	case "CONTAINS":
		return containsOperator(left, right)
	case "OVERLAPS":
		return overlapsOperator(left, right)
	case "IS NULL":
		return left == nil
	case "IS NOT NULL":
		return left != nil
	case "AND":
		leftVal, _ := convert.ToBool(left)
		rightVal, _ := convert.ToBool(right)
		return leftVal && rightVal
	case "OR":
		leftVal, _ := convert.ToBool(left)
		rightVal, _ := convert.ToBool(right)
		return leftVal || rightVal
	default:
		ctx.logError("Unsupported binary operator: " + e.Operator)
		return nil
	}
}

func (ctx *EvalContext) evalScalarFunction(c context.Context, fc *FunctionCall, row utils.Record) any {
	name := strings.ToUpper(fc.FunctionName)
	switch name {
	case "NOW", "CURRENT_TIMESTAMP":
		return time.Now().Format("2006-01-02 15:04:05")
	case "COALESCE":
		for _, arg := range fc.Args {
			val := ctx.evalExpression(c, arg, row)
			if !isNilOrEmpty(val) {
				return val
			}
		}
		return nil
	case "CONCAT":
		var parts []string
		for _, arg := range fc.Args {
			val := ctx.evalExpression(c, arg, row)
			parts = append(parts, fmt.Sprintf("%v", val))
		}
		return strings.Join(parts, "")
	case "IF":
		if len(fc.Args) < 3 {
			ctx.logError("IF requires at least 3 arguments")
			return nil
		}
		cond := ctx.evalExpression(c, fc.Args[0], row)
		if b, ok := cond.(bool); ok && b {
			return ctx.evalExpression(c, fc.Args[1], row)
		}
		return ctx.evalExpression(c, fc.Args[2], row)
	case "SUBSTR":
		if len(fc.Args) < 2 {
			ctx.logError("SUBSTR requires at least 2 arguments")
			return nil
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		startVal, ok := convert.ToFloat64(ctx.evalExpression(c, fc.Args[1], row))
		if !ok {
			return nil
		}
		start := int(startVal) - 1
		if start < 0 || start >= len(s) {
			return ""
		}
		if len(fc.Args) == 3 {
			lenVal, ok := convert.ToFloat64(ctx.evalExpression(c, fc.Args[2], row))
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
		s := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		return len(s)
	case "UPPER":
		s := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		return strings.ToUpper(s)
	case "LOWER":
		s := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		return strings.ToLower(s)
	case "TO_DATE":
		if len(fc.Args) < 2 {
			ctx.logError("TO_DATE requires 2 arguments")
			return nil
		}
		dateStr := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		formatStr := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[1], row))
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
		numStr := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			ctx.logError("TO_NUMBER parse error: " + err.Error())
			return nil
		}
		return f
	case "ROUND":
		if len(fc.Args) < 1 {
			ctx.logError("ROUND requires at least 1 argument")
			return nil
		}
		val := ctx.evalExpression(c, fc.Args[0], row)
		num, ok := convert.ToFloat64(val)
		if !ok {
			ctx.logError("ROUND: unable to convert argument to number")
			return nil
		}
		precision := 0.0
		if len(fc.Args) >= 2 {
			pVal := ctx.evalExpression(c, fc.Args[1], row)
			if p, ok := convert.ToFloat64(pVal); ok {
				precision = p
			}
		}
		factor := math.Pow(10, precision)
		return math.Round(num*factor) / factor
	case "ANY", "ALL":
		if len(fc.Args) < 1 {
			ctx.logError(strings.ToUpper(fc.FunctionName) + " requires one argument")
			return quantifiedValues{Mode: strings.ToUpper(fc.FunctionName)}
		}
		val := ctx.evalExpression(c, fc.Args[0], row)
		values := toComparableSlice(val)
		return quantifiedValues{
			Mode:   strings.ToUpper(fc.FunctionName),
			Values: values,
		}
	case "CAST":
		if len(fc.Args) < 2 {
			ctx.logError("CAST requires 2 arguments: CAST(expr AS type)")
			return nil
		}
		typeName := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[1], row))
		value := ctx.evalExpression(c, fc.Args[0], row)
		return castValue(typeName, value)
	case "DATEDIFF":
		if len(fc.Args) < 2 {
			ctx.logError("DATEDIFF requires 2 arguments")
			return nil
		}
		date1Str := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[0], row))
		date2Str := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[1], row))
		t1, err1 := time.Parse("2006-01-02", date1Str)
		t2, err2 := time.Parse("2006-01-02", date2Str)
		if err1 != nil || err2 != nil {
			ctx.logError("DATEDIFF parse error: invalid date format")
			return nil
		}
		diff := t1.Sub(t2)
		return int(diff.Hours() / 24)
	case "JSON_EXTRACT":
		if len(fc.Args) < 2 {
			ctx.logError("JSON_EXTRACT requires 2 arguments")
			return nil
		}
		payload := ctx.evalExpression(c, fc.Args[0], row)
		path := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[1], row))
		val, ok := resolvePathFromValue(payload, normalizeJSONPath(path))
		if !ok {
			return nil
		}
		return val
	case "JSON_EXISTS":
		if len(fc.Args) < 2 {
			ctx.logError("JSON_EXISTS requires 2 arguments")
			return false
		}
		payload := ctx.evalExpression(c, fc.Args[0], row)
		path := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[1], row))
		_, ok := resolvePathFromValue(payload, normalizeJSONPath(path))
		return ok
	case "JSON_VALUE":
		if len(fc.Args) < 2 {
			ctx.logError("JSON_VALUE requires 2 arguments")
			return nil
		}
		payload := ctx.evalExpression(c, fc.Args[0], row)
		path := fmt.Sprintf("%v", ctx.evalExpression(c, fc.Args[1], row))
		val, ok := resolvePathFromValue(payload, normalizeJSONPath(path))
		if !ok {
			return nil
		}
		if arr, ok := toAnySlice(val); ok {
			for _, item := range arr {
				if item != nil {
					return item
				}
			}
			return nil
		}
		return val
	default:
		ctx.logError("Unsupported scalar function: " + name)
		return nil
	}
}

func (ctx *EvalContext) getPartitionKey(c context.Context, row utils.Record, exprs []Expression) string {
	if len(exprs) == 0 {
		return "all"
	}
	var parts []string
	for _, expr := range exprs {
		val := ctx.evalExpression(c, expr, row)
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

func normalizeIdentifierPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.ReplaceAll(path, "->", ".")
	path = strings.Trim(path, ".")
	return path
}

type pathSegment struct {
	Key     string
	Indexes []int
}

func parsePath(path string) []pathSegment {
	path = normalizeIdentifierPath(path)
	if path == "" {
		return nil
	}
	rawSegs := strings.Split(path, ".")
	segments := make([]pathSegment, 0, len(rawSegs))
	for _, raw := range rawSegs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		seg := pathSegment{}
		for i := 0; i < len(raw); {
			switch raw[i] {
			case '[':
				end := strings.IndexByte(raw[i:], ']')
				if end <= 1 {
					return nil
				}
				end += i
				idxVal, err := strconv.Atoi(strings.TrimSpace(raw[i+1 : end]))
				if err != nil {
					return nil
				}
				seg.Indexes = append(seg.Indexes, idxVal)
				i = end + 1
			default:
				j := i
				for j < len(raw) && raw[j] != '[' {
					j++
				}
				if seg.Key == "" {
					seg.Key = strings.TrimSpace(raw[i:j])
				} else {
					seg.Key += strings.TrimSpace(raw[i:j])
				}
				i = j
			}
		}
		segments = append(segments, seg)
	}
	return segments
}

func resolvePathFromRecord(row utils.Record, path string) (any, bool) {
	segs := parsePath(path)
	if len(segs) == 0 {
		return nil, false
	}

	// Direct object traversal: row[first] -> nested keys/indexes.
	if first, ok := row[segs[0].Key]; ok {
		val, ok := resolveSegments(first, segs, 0, true)
		if ok {
			return val, true
		}
	}

	// Flattened key traversal: support alias-prefixed keys like "u.education".
	for i := len(segs); i >= 1; i-- {
		var keyParts []string
		valid := true
		for _, seg := range segs[:i] {
			if seg.Key == "" {
				valid = false
				break
			}
			keyParts = append(keyParts, seg.Key)
		}
		if !valid {
			continue
		}
		key := strings.Join(keyParts, ".")
		val, ok := row[key]
		if !ok {
			continue
		}
		current := val
		if len(segs[i-1].Indexes) > 0 {
			next, ok := applyIndexes(current, segs[i-1].Indexes)
			if !ok {
				continue
			}
			current = next
		}
		if i == len(segs) {
			return current, true
		}
		return resolveSegments(current, segs, i, false)
	}
	return nil, false
}

func resolvePathFromValue(value any, path string) (any, bool) {
	segs := parsePath(path)
	if len(segs) == 0 {
		return nil, false
	}
	return resolveSegments(value, segs, 0, false)
}

func resolveSegments(current any, segs []pathSegment, start int, firstFromRecord bool) (any, bool) {
	val := current
	for i := start; i < len(segs); i++ {
		seg := segs[i]
		if i == start && firstFromRecord {
			// key already resolved by caller
		} else if seg.Key != "" {
			switch v := val.(type) {
			case map[string]any:
				next, ok := v[seg.Key]
				if !ok {
					return nil, false
				}
				val = next
			default:
				items, ok := toAnySlice(val)
				if !ok {
					return nil, false
				}
				var out []any
				for _, item := range items {
					resolved, ok := resolveSegments(item, segs, i, false)
					if !ok {
						continue
					}
					if nested, isSlice := toAnySlice(resolved); isSlice {
						out = append(out, nested...)
					} else {
						out = append(out, resolved)
					}
				}
				if len(out) == 0 {
					return nil, false
				}
				return out, true
			}
		}
		if len(seg.Indexes) > 0 {
			next, ok := applyIndexes(val, seg.Indexes)
			if !ok {
				return nil, false
			}
			val = next
		}
	}
	return val, true
}

func applyIndexes(value any, indexes []int) (any, bool) {
	current := value
	for _, idx := range indexes {
		items, ok := toAnySlice(current)
		if !ok {
			return nil, false
		}
		if idx < 0 || idx >= len(items) {
			return nil, false
		}
		current = items[idx]
	}
	return current, true
}

func toAnySlice(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	switch v := value.(type) {
	case []any:
		return v, true
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, false
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

func toComparableSlice(value any) []any {
	if value == nil {
		return nil
	}
	if arr, ok := toAnySlice(value); ok {
		return arr
	}
	return []any{value}
}

func valueInList(left any, candidates []any) bool {
	if leftSlice, ok := toAnySlice(left); ok {
		for _, lv := range leftSlice {
			if valueInList(lv, candidates) {
				return true
			}
		}
		return false
	}
	for _, candidate := range candidates {
		if rightSlice, ok := toAnySlice(candidate); ok {
			if valueInList(left, rightSlice) {
				return true
			}
			continue
		}
		if utils.CompareValues(left, candidate) == 0 {
			return true
		}
	}
	return false
}

func compareWithCollection(op string, left, right any) (bool, bool) {
	if leftSlice, ok := toAnySlice(left); ok {
		for _, lv := range leftSlice {
			if result, done := compareWithCollection(op, lv, right); done && result {
				return true, true
			}
		}
		return false, true
	}
	if rightSlice, ok := toAnySlice(right); ok {
		for _, rv := range rightSlice {
			if result, done := compareWithCollection(op, left, rv); done && result {
				return true, true
			}
		}
		return false, true
	}
	switch op {
	case "=":
		return utils.CompareValues(left, right) == 0, true
	case "!=":
		return utils.CompareValues(left, right) != 0, true
	case "<":
		return utils.CompareValues(left, right) < 0, true
	case ">":
		return utils.CompareValues(left, right) > 0, true
	case "<=":
		return utils.CompareValues(left, right) <= 0, true
	case ">=":
		return utils.CompareValues(left, right) >= 0, true
	default:
		return false, false
	}
}

func compareWithQuantified(op string, left, right any) (bool, bool) {
	if q, ok := right.(quantifiedValues); ok {
		return compareWithQuantifiedValues(op, left, q), true
	}
	if q, ok := left.(quantifiedValues); ok {
		return compareWithQuantifiedValues(op, right, q), true
	}
	return false, false
}

func compareWithQuantifiedValues(op string, value any, q quantifiedValues) bool {
	switch q.Mode {
	case "ANY":
		for _, candidate := range q.Values {
			if matchSimpleComparison(op, value, candidate) {
				return true
			}
		}
		return false
	case "ALL":
		if len(q.Values) == 0 {
			return true
		}
		for _, candidate := range q.Values {
			if !matchSimpleComparison(op, value, candidate) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func matchSimpleComparison(op string, left, right any) bool {
	switch op {
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
	default:
		return false
	}
}

func containsOperator(left, right any) bool {
	leftSlice := toComparableSlice(left)
	if len(leftSlice) == 0 {
		return false
	}
	rightSlice := toComparableSlice(right)
	if len(rightSlice) == 0 {
		return false
	}
	for _, rv := range rightSlice {
		found := false
		for _, lv := range leftSlice {
			if utils.CompareValues(lv, rv) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func overlapsOperator(left, right any) bool {
	leftSlice := toComparableSlice(left)
	rightSlice := toComparableSlice(right)
	for _, lv := range leftSlice {
		for _, rv := range rightSlice {
			if utils.CompareValues(lv, rv) == 0 {
				return true
			}
		}
	}
	return false
}

func normalizeJSONPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "'")
	path = strings.Trim(path, "\"")
	if strings.HasPrefix(path, "$.") {
		return path[2:]
	}
	if path == "$" {
		return ""
	}
	if strings.HasPrefix(path, "$") {
		return strings.TrimPrefix(path, "$")
	}
	return path
}

func castValue(typeName string, value any) any {
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "int", "integer", "int32":
		v, ok := convert.ToInt(value)
		if !ok {
			return nil
		}
		return v
	case "int64", "bigint":
		v, ok := convert.ToInt64(value)
		if !ok {
			return nil
		}
		return v
	case "float", "float64", "double", "decimal", "numeric":
		v, ok := convert.ToFloat64(value)
		if !ok {
			return nil
		}
		return v
	case "bool", "boolean":
		v, ok := convert.ToBool(value)
		if !ok {
			return nil
		}
		return v
	case "string", "text", "varchar":
		return fmt.Sprintf("%v", value)
	default:
		return nil
	}
}
