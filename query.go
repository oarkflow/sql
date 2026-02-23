package sql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/convert"
	"github.com/oarkflow/log"

	"github.com/oarkflow/sql/pkg/utils"
)

const (
	maxTableCacheSize     = 100
	maxLikeRegexCacheSize = 100
)

type tableCacheEntry struct {
	rows    []utils.Record
	modTime time.Time
}

var tableCache = make(map[string]tableCacheEntry)
var tableCacheMu sync.Mutex

var likeRegexCache = make(map[string]*regexp.Regexp)
var likeRegexCacheMu sync.RWMutex

func compileLikeRegex(pattern string) (*regexp.Regexp, error) {
	likeRegexCacheMu.RLock()
	re, ok := likeRegexCache[pattern]
	likeRegexCacheMu.RUnlock()
	if ok {
		return re, nil
	}
	var sb strings.Builder
	sb.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			if strings.ContainsRune(`\.+*?()|[]{}^$`, r) {
				sb.WriteRune('\\')
			}
			sb.WriteRune(r)
		}
	}
	sb.WriteString("$")
	compiled, err := regexp.Compile(sb.String())
	if err != nil {
		return nil, err
	}
	likeRegexCacheMu.Lock()
	if len(likeRegexCache) >= maxLikeRegexCacheSize {
		for key := range likeRegexCache {
			delete(likeRegexCache, key)
			break
		}
	}
	likeRegexCache[pattern] = compiled
	likeRegexCacheMu.Unlock()
	return compiled, nil
}

func sqlLikeMatch(s, pattern string) bool {
	re, err := compileLikeRegex(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(s)
}

func Query(ctx context.Context, query string) ([]utils.Record, error) {
	start := time.Now()
	lexer := NewLexer(query)
	parser := NewParser(lexer)
	program := parser.ParseQueryStatement()
	if len(parser.errors) != 0 {
		return nil, errors.New(strings.Join(parser.errors, "\t"))
	}
	records, err := program.parseAndExecute(ctx)
	if err != nil {
		return nil, err
	}
	latency := time.Since(start)
	userID := ctx.Value("user_id")
	if userID == nil {
		return records, nil
	}
	log.Info().Any("user_id", userID).Str("latency", fmt.Sprintf("%s", latency)).Msg("Executed query")
	return records, nil
}

type QueryStatement struct {
	With     *WithClause
	Query    *SQL
	Compound *CompoundQuery
}

func (qs *QueryStatement) parseAndExecute(ctx context.Context) ([]utils.Record, error) {
	if qs.Compound != nil {
		// For compound queries, execute left and right separately
		leftRows, err := qs.Compound.Left.executeFullQuery(ctx)
		if err != nil {
			return nil, err
		}
		rightRows, err := qs.Compound.Right.executeFullQuery(ctx)
		if err != nil {
			return nil, err
		}
		switch qs.Compound.Operator {
		case UNION_ALL:
			return UnionAll(leftRows, rightRows), nil
		case UNION:
			return utils.Union(leftRows, rightRows), nil
		case INTERSECT:
			return utils.Intersect(leftRows, rightRows), nil
		case EXCEPT:
			return utils.Except(leftRows, rightRows), nil
		}
	}

	// For simple queries
	return qs.Query.executeFullQuery(ctx)
}

// New helper for UNION ALL compound query
func UnionAll(a, b []utils.Record) []utils.Record {
	// UNION ALL: simply return concatenated records
	return append(a, b...)
}

func (q *SQL) executeFullQuery(ctx context.Context) ([]utils.Record, error) {
	mainRows, err := q.From.loadData(ctx)
	if err != nil {
		return nil, err
	}
	if q.From.Alias != "" {
		alias := q.From.Alias
		for i, row := range mainRows {
			mainRows[i] = utils.ApplyAliasToRecord(row, alias)
		}
	}
	if len(q.Joins) > 0 {
		mainRows, err = q.executeJoins(ctx, mainRows)
		if err != nil {
			return nil, err
		}
	}
	result, err := q.executeQuery(ctx, mainRows)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (qs *QueryStatement) statementNode()       {}
func (qs *QueryStatement) TokenLiteral() string { return qs.Query.TokenLiteral() }
func (qs *QueryStatement) String() string {
	if qs.Compound != nil {
		return qs.Compound.String()
	}
	if qs.With != nil {
		var ctes []string
		for _, cte := range qs.With.CTEs {
			ctes = append(ctes, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.String()))
		}
		return fmt.Sprintf("WITH %s %s", strings.Join(ctes, ", "), qs.Query.String())
	}
	return qs.Query.String()
}

type SQL struct {
	With     *WithClause
	Select   *SelectClause
	From     *TableReference
	Joins    []*JoinClause
	Where    Expression
	GroupBy  *GroupByClause
	Having   Expression
	OrderBy  *OrderByClause
	Limit    *LimitClause
	Distinct bool
}

func (query *SQL) TokenLiteral() string { return query.Select.TokenLiteral() }
func (query *SQL) String() string {
	var parts []string
	if query.With != nil {
		var ctes []string
		for _, cte := range query.With.CTEs {
			ctes = append(ctes, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.String()))
		}
		parts = append(parts, "WITH "+strings.Join(ctes, ", "))
	}
	sel := "SELECT "
	if query.Distinct {
		sel += "DISTINCT "
	}
	sel += query.Select.String()
	parts = append(parts, sel)
	parts = append(parts, "FROM "+query.From.String())
	for _, join := range query.Joins {
		parts = append(parts, join.String())
	}
	if query.Where != nil {
		parts = append(parts, "WHERE "+query.Where.String())
	}
	if query.GroupBy != nil {
		parts = append(parts, "GROUP BY "+query.GroupBy.String())
	}
	if query.Having != nil {
		parts = append(parts, "HAVING "+query.Having.String())
	}
	if query.OrderBy != nil {
		parts = append(parts, query.OrderBy.String())
	}
	if query.Limit != nil {
		parts = append(parts, query.Limit.String())
	}
	return strings.Join(parts, " ")
}

type Condition struct {
	Field    string
	Operator string
	Value    string
}

func extractConditions(expr Expression) []Condition {
	var conds []Condition
	switch e := expr.(type) {
	case *BinaryExpression:
		if e.Operator == "AND" {
			conds = append(conds, extractConditions(e.Left)...)
			conds = append(conds, extractConditions(e.Right)...)
		} else if e.Operator == "=" || e.Operator == "!=" || e.Operator == ">" ||
			e.Operator == "<" || e.Operator == ">=" || e.Operator == "<=" ||
			strings.ToUpper(e.Operator) == "LIKE" {
			if ident, ok := e.Left.(*Identifier); ok {
				if lit, ok2 := e.Right.(*Literal); ok2 {
					// Keep fast-path filtering for simple flat fields only.
					if strings.Contains(ident.Value, ".") || strings.Contains(ident.Value, "->") ||
						strings.Contains(ident.Value, "[") || strings.Contains(ident.Value, "]") {
						break
					}
					conds = append(conds, Condition{
						Field:    ident.Value,
						Operator: e.Operator,
						Value:    fmt.Sprintf("%v", lit.Value),
					})
				}
			}
		}
	}
	return conds
}

func evaluateCondition(keyStr, op, condVal string) bool {
	k, ok1 := convert.ToFloat64(keyStr)
	c, ok2 := convert.ToFloat64(condVal)
	if ok1 && ok2 {
		switch op {
		case "=":
			return k == c
		case "!=":
			return k != c
		case ">":
			return k > c
		case "<":
			return k < c
		case ">=":
			return k >= c
		case "<=":
			return k <= c
		case "LIKE":
			return sqlLikeMatch(keyStr, condVal)
		}
	} else {
		switch op {
		case "=":
			return keyStr == condVal
		case "!=":
			return keyStr != condVal
		case ">":
			return keyStr > condVal
		case "<":
			return keyStr < condVal
		case ">=":
			return keyStr >= condVal
		case "<=":
			return keyStr <= condVal
		case "LIKE":
			return sqlLikeMatch(keyStr, condVal)
		}
	}
	return false
}

func intersectRecords(a, b []utils.Record) []utils.Record {
	var result []utils.Record
	keys := make(map[string]bool)
	for _, r := range a {
		key := fmt.Sprintf("%v", r)
		keys[key] = true
	}
	for _, r := range b {
		key := fmt.Sprintf("%v", r)
		if keys[key] {
			result = append(result, r)
		}
	}
	return result
}

func (query *SQL) executeQuery(c context.Context, rows []utils.Record) ([]utils.Record, error) {
	if len(rows) == 0 {
		if query.From != nil {
			var err error
			rows, err = query.From.loadData(c)
			if err != nil {
				return nil, err
			}
		} else {
			rows = []utils.Record{}
		}
	}
	ctx := NewEvalContext()
	if query.From != nil {
		ctx.CurrentAlias = query.From.Alias
	}
	var filteredRows []utils.Record
	conds := []Condition{}
	if query.Where != nil {
		conds = extractConditions(query.Where)
	}
	if len(conds) > 0 {
		var sets [][]utils.Record
		for _, cond := range conds {
			var set []utils.Record
			for _, row := range rows {
				// Use evalExpression to apply alias rules
				val := ctx.evalExpression(c, &Identifier{Value: cond.Field}, row)
				if val != nil {
					keyStr := fmt.Sprintf("%v", val)
					if evaluateCondition(keyStr, cond.Operator, cond.Value) {
						set = append(set, row)
					}
				}
			}
			sets = append(sets, set)
		}
		if len(sets) > 0 {
			filteredRows = sets[0]
			for i := 1; i < len(sets); i++ {
				filteredRows = intersectRecords(filteredRows, sets[i])
			}
		}
	} else {
		for _, row := range rows {
			if query.Where != nil {
				result := ctx.evalExpression(c, query.Where, row)
				if b, ok := result.(bool); !ok || !b {
					continue
				}
			}
			filteredRows = append(filteredRows, row)
		}
	}
	hasAggregate := false
	for _, expr := range query.Select.Fields {
		_, underlying := unwrapAlias(expr)
		if fc, ok := underlying.(*FunctionCall); ok {
			switch strings.ToUpper(fc.FunctionName) {
			case "COUNT", "AVG", "SUM", "MIN", "MAX", "DIFF":
				hasAggregate = true
			}
		}
	}
	simpleQuery := (query.GroupBy == nil && query.Having == nil && query.OrderBy == nil && !hasAggregate)
	targetCount := -1
	if simpleQuery && query.Limit != nil && query.Limit.Limit > 0 {
		// Only apply early-stop optimization when LIMIT is a positive number.
		targetCount = query.Limit.Offset + query.Limit.Limit
		if len(conds) == 0 {
			var temp []utils.Record
			for _, row := range rows {
				if query.Where != nil {
					result := ctx.evalExpression(c, query.Where, row)
					if b, ok := result.(bool); !ok || !b {
						continue
					}
				}
				temp = append(temp, row)
				if len(temp) >= targetCount {
					break
				}
			}
			filteredRows = temp
		}
	}
	ctx.CurrentResultSet = filteredRows
	var resultRows []utils.Record
	if query.GroupBy != nil {
		groups := make(map[string][]utils.Record)
		for _, row := range filteredRows {
			var keyParts []string
			// Support multiple grouping fields.
			for _, expr := range query.GroupBy.Fields {
				val := ctx.evalExpression(c, expr, row)
				keyParts = append(keyParts, fmt.Sprintf("%v", val))
			}
			key := strings.Join(keyParts, "||")
			groups[key] = append(groups[key], row)
		}
		for _, groupRows := range groups {
			resultRow := make(utils.Record)
			for i, expr := range query.Select.Fields {
				colName := getFieldName(expr, i)
				_, underlying := unwrapAlias(expr)
				if fc, ok := underlying.(*FunctionCall); ok {
					switch strings.ToUpper(fc.FunctionName) {
					case "COUNT":
						resultRow[colName] = len(groupRows)
					case "AVG":
						sum := 0.0
						count := 0.0
						for _, r := range groupRows {
							val := ctx.evalExpression(c, fc.Args[0], r)
							num, ok := convert.ToFloat64(val)
							if ok {
								sum += num
								count++
							}
						}
						if count > 0 {
							resultRow[colName] = sum / count
						} else {
							resultRow[colName] = nil
						}
					case "SUM":
						sum := 0.0
						for _, r := range groupRows {
							val := ctx.evalExpression(c, fc.Args[0], r)
							num, ok := convert.ToFloat64(val)
							if ok {
								sum += num
							}
						}
						resultRow[colName] = sum
					case "MIN":
						var minVal float64
						first := true
						for _, r := range groupRows {
							val := ctx.evalExpression(c, fc.Args[0], r)
							num, ok := convert.ToFloat64(val)
							if ok {
								if first {
									minVal = num
									first = false
								} else if num < minVal {
									minVal = num
								}
							}
						}
						if first {
							resultRow[colName] = nil
						} else {
							resultRow[colName] = minVal
						}
					case "MAX":
						var maxVal float64
						first := true
						for _, r := range groupRows {
							val := ctx.evalExpression(c, fc.Args[0], r)
							num, ok := convert.ToFloat64(val)
							if ok {
								if first {
									maxVal = num
									first = false
								} else if num > maxVal {
									maxVal = num
								}
							}
						}
						if first {
							resultRow[colName] = nil
						} else {
							resultRow[colName] = maxVal
						}
					case "DIFF":
						var minVal, maxVal float64
						first := true
						for _, r := range groupRows {
							val := ctx.evalExpression(c, fc.Args[0], r)
							num, ok := convert.ToFloat64(val)
							if ok {
								if first {
									minVal = num
									maxVal = num
									first = false
								} else {
									if num < minVal {
										minVal = num
									}
									if num > maxVal {
										maxVal = num
									}
								}
							}
						}
						if first {
							resultRow[colName] = nil
						} else {
							resultRow[colName] = maxVal - minVal
						}
					default:
						resultRow[colName] = nil
					}
				} else {
					resultRow[colName] = ctx.evalExpression(c, underlying, groupRows[0])
				}
			}
			if query.Having != nil {
				hv := ctx.evalExpression(c, query.Having, resultRow)
				if b, ok := hv.(bool); !ok || !b {
					continue
				}
			}
			resultRows = append(resultRows, resultRow)
		}
	} else if hasAggregate {
		resultRow := make(utils.Record)
		for i, expr := range query.Select.Fields {
			colName := getFieldName(expr, i)
			_, underlying := unwrapAlias(expr)
			if fc, ok := underlying.(*FunctionCall); ok {
				switch strings.ToUpper(fc.FunctionName) {
				case "COUNT":
					resultRow[colName] = len(filteredRows)
				case "AVG":
					sum := 0.0
					count := 0.0
					for _, r := range filteredRows {
						val := ctx.evalExpression(c, fc.Args[0], r)
						num, ok := convert.ToFloat64(val)
						if ok {
							sum += num
							count++
						}
					}
					if count > 0 {
						resultRow[colName] = sum / count
					} else {
						resultRow[colName] = nil
					}
				case "SUM":
					sum := 0.0
					for _, r := range filteredRows {
						val := ctx.evalExpression(c, fc.Args[0], r)
						num, ok := convert.ToFloat64(val)
						if ok {
							sum += num
						}
					}
					resultRow[colName] = sum
				case "MIN":
					var minVal float64
					first := true
					for _, r := range filteredRows {
						val := ctx.evalExpression(c, fc.Args[0], r)
						num, ok := convert.ToFloat64(val)
						if ok {
							if first {
								minVal = num
								first = false
							} else if num < minVal {
								minVal = num
							}
						}
					}
					if first {
						resultRow[colName] = nil
					} else {
						resultRow[colName] = minVal
					}
				case "MAX":
					var maxVal float64
					first := true
					for _, r := range filteredRows {
						val := ctx.evalExpression(c, fc.Args[0], r)
						num, ok := convert.ToFloat64(val)
						if ok {
							if first {
								maxVal = num
								first = false
							} else if num > maxVal {
								maxVal = num
							}
						}
					}
					if first {
						resultRow[colName] = nil
					} else {
						resultRow[colName] = maxVal
					}
				case "DIFF":
					var minVal, maxVal float64
					first := true
					for _, r := range filteredRows {
						val := ctx.evalExpression(c, fc.Args[0], r)
						num, ok := convert.ToFloat64(val)
						if ok {
							if first {
								minVal = num
								maxVal = num
								first = false
							} else {
								if num < minVal {
									minVal = num
								}
								if num > maxVal {
									maxVal = num
								}
							}
						}
					}
					if first {
						resultRow[colName] = nil
					} else {
						resultRow[colName] = maxVal - minVal
					}
				default:
					resultRow[colName] = nil
				}
			} else {
				if len(filteredRows) > 0 {
					resultRow[colName] = ctx.evalExpression(c, underlying, filteredRows[0])
				} else {
					resultRow[colName] = nil
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	} else {
		for _, row := range filteredRows {
			newRow := make(utils.Record)
			for i, expr := range query.Select.Fields {
				colName := getFieldName(expr, i)
				_, underlying := unwrapAlias(expr)
				switch underlying := underlying.(type) {
				case *Star:
					for k, v := range row {
						newRow[k] = v
					}
				case *QualifiedStar:
					prefix := underlying.Alias + "."
					for k, v := range row {
						if strings.HasPrefix(k, prefix) {
							// Remove alias prefix from the final column name.
							newKey := strings.TrimPrefix(k, prefix)
							newRow[newKey] = v
						}
					}
				default:
					newRow[colName] = ctx.evalExpression(c, underlying, row)
				}
			}
			resultRows = append(resultRows, newRow)
			if targetCount > 0 && len(resultRows) >= query.Limit.Limit {
				break
			}
		}
	}
	if query.Distinct {
		var unique []utils.Record
		seen := make(map[string]bool)
		for _, row := range resultRows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		resultRows = unique
	}
	if query.OrderBy != nil {
		sort.Slice(resultRows, func(i, j int) bool {
			for index, expr := range query.OrderBy.Fields {
				dir := "ASC"
				if index < len(query.OrderBy.Directions) {
					dir = query.OrderBy.Directions[index]
				}
				vi := ctx.evalExpression(c, expr, resultRows[i])
				vj := ctx.evalExpression(c, expr, resultRows[j])
				cmp := utils.CompareValues(vi, vj)
				if cmp == 0 {
					continue
				}
				if dir == "ASC" {
					return cmp < 0
				}
				return cmp > 0
			}
			return false
		})
	}
	if query.Limit != nil {
		start := query.Limit.Offset
		// LIMIT 0 or LIMIT ALL means "no limit", i.e., return all rows after OFFSET.
		end := len(resultRows)
		if query.Limit.Limit > 0 {
			end = start + query.Limit.Limit
		}
		if start > len(resultRows) {
			resultRows = []utils.Record{}
		} else {
			if end > len(resultRows) {
				end = len(resultRows)
			}
			resultRows = resultRows[start:end]
		}
	}
	return resultRows, nil
}

func executeCrossJoin(leftRows, rightRows []utils.Record, alias string) []utils.Record {
	var newResult []utils.Record
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			newResult = append(newResult, merged)
		}
	}
	return newResult
}

func executeLeftJoin(c context.Context, leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(c, joinOn, merged)
			if b, ok := cond.(bool); ok && b {
				newResult = append(newResult, merged)
				matched = true
			}
		}
		if !matched {
			merged := utils.MergeRows(leftRow, nil, alias)
			newResult = append(newResult, merged)
		}
	}
	return newResult
}

func executeRightJoin(c context.Context, leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(c, joinOn, merged)
			if b, ok := cond.(bool); ok && b {
				newResult = append(newResult, merged)
				matched = true
			}
		}
		if !matched {
			merged := utils.MergeRows(nil, rightRow, alias)
			newResult = append(newResult, merged)
		}
	}
	return newResult
}

func executeFullJoin(c context.Context, leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	leftMatched := make(map[string]bool)
	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(c, joinOn, merged)
			if b, ok := cond.(bool); ok && b {
				newResult = append(newResult, merged)
				matched = true
				key := utils.RecordKey(leftRow, rightRow)
				leftMatched[key] = true
			}
		}
		if !matched {
			merged := utils.MergeRows(leftRow, nil, alias)
			newResult = append(newResult, merged)
		}
	}
	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(c, joinOn, merged)
			if b, ok := cond.(bool); ok && b {
				key := utils.RecordKey(leftRow, rightRow)
				if leftMatched[key] {
					matched = true
					break
				}
			}
		}
		if !matched {
			merged := utils.MergeRows(nil, rightRow, alias)
			newResult = append(newResult, merged)
		}
	}
	return newResult
}

func executeNaturalJoin(leftRows, rightRows []utils.Record, alias string) []utils.Record {
	var newResult []utils.Record
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			// Determine common keys between leftRow and rightRow.
			common := make([]string, 0)
			for k := range leftRow {
				if _, ok := rightRow[k]; ok {
					common = append(common, k)
				}
			}
			// If no common keys, skip natural join.
			if len(common) == 0 {
				continue
			}
			// Check all common columns are equal.
			match := true
			for _, k := range common {
				if utils.CompareValues(leftRow[k], rightRow[k]) != 0 {
					match = false
					break
				}
			}
			if match {
				// Merge rows: include all keys from left; for right add keys not already in left.
				merged := make(utils.Record)
				// Copy left row.
				for k, v := range leftRow {
					merged[k] = v
				}
				// Merge right row without duplicate keys.
				for k, v := range rightRow {
					if _, exists := merged[k]; !exists {
						merged[k] = v
					}
				}
				// Optionally, add alias handling if needed.
				newResult = append(newResult, merged)
			}
		}
	}
	return newResult
}

func executeDefaultJoin(c context.Context, leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(c, joinOn, merged)
			if b, ok := cond.(bool); ok && b {
				newResult = append(newResult, merged)
			}
		}
	}
	return newResult
}

func (query *SQL) executeJoins(ctx context.Context, rows []utils.Record) ([]utils.Record, error) {
	currentRows := rows
	for _, join := range query.Joins {
		if join.Table != nil && join.Table.Source == "unnest" && join.Table.UnnestExpr != nil {
			currentRows = executeUnnestJoin(ctx, currentRows, join)
			continue
		}
		joinRows, err := join.Table.loadData(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load join table %s: %s", join.Table.Name, err)
		}
		alias := join.Table.Alias
		if alias == "" {
			alias = join.Table.Name
		}
		joinType := strings.ToUpper(join.JoinType)
		var newResult []utils.Record
		switch joinType {
		case "CROSS", "CROSS JOIN":
			newResult = executeCrossJoin(currentRows, joinRows, alias)
		case "LEFT", "LEFT JOIN", "LEFT OUTER", "LEFT OUTER JOIN":
			newResult = executeLeftJoin(ctx, currentRows, joinRows, alias, join.On)
		case "RIGHT", "RIGHT JOIN", "RIGHT OUTER", "RIGHT OUTER JOIN":
			newResult = executeRightJoin(ctx, currentRows, joinRows, alias, join.On)
		case "FULL", "FULL JOIN", "FULL OUTER", "FULL OUTER JOIN", "OUTER", "OUTER JOIN":
			newResult = executeFullJoin(ctx, currentRows, joinRows, alias, join.On)
		case "NATURAL":
			newResult = executeNaturalJoin(currentRows, joinRows, alias)
		default:
			newResult = executeDefaultJoin(ctx, currentRows, joinRows, alias, join.On)
		}
		currentRows = newResult
	}
	return currentRows, nil
}

func executeUnnestJoin(c context.Context, leftRows []utils.Record, join *JoinClause) []utils.Record {
	var newResult []utils.Record
	joinType := strings.ToUpper(join.JoinType)
	alias := join.Table.Alias
	if alias == "" {
		alias = "unnest"
	}
	for _, leftRow := range leftRows {
		ev := NewEvalContext()
		items := ev.evalExpression(c, join.Table.UnnestExpr, leftRow)
		values, ok := toAnySlice(items)
		if !ok || len(values) == 0 {
			if joinType == "LEFT" || joinType == "LEFT JOIN" || joinType == "LEFT OUTER" || joinType == "LEFT OUTER JOIN" {
				newResult = append(newResult, utils.MergeRows(leftRow, nil, alias))
			}
			continue
		}
		matched := false
		for _, item := range values {
			rightRow := make(utils.Record)
			switch v := item.(type) {
			case map[string]any:
				rightRow = v
			default:
				rightRow["value"] = v
			}
			merged := utils.MergeRows(leftRow, rightRow, alias)
			if join.On != nil {
				cond := ev.evalExpression(c, join.On, merged)
				if b, ok := cond.(bool); !ok || !b {
					continue
				}
			}
			matched = true
			newResult = append(newResult, merged)
		}
		if !matched && (joinType == "LEFT" || joinType == "LEFT JOIN" || joinType == "LEFT OUTER" || joinType == "LEFT OUTER JOIN") {
			newResult = append(newResult, utils.MergeRows(leftRow, nil, alias))
		}
	}
	return newResult
}
