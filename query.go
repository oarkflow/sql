package sql

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/convert"

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

func Query(query string) ([]utils.Record, error) {
	lexer := NewLexer(query)
	parser := NewParser(lexer)
	program := parser.ParseQueryStatement()
	if len(parser.errors) != 0 {
		return nil, errors.New(strings.Join(parser.errors, "\t"))
	}
	return program.parseAndExecute()
}

type QueryStatement struct {
	With     *WithClause
	Query    *SQL
	Compound *CompoundQuery
}

func (qs *QueryStatement) parseAndExecute() ([]utils.Record, error) {
	mainRows, err := qs.Query.From.loadData()
	if err != nil {
		return nil, err
	}
	if qs.Query.From.Alias != "" {
		alias := qs.Query.From.Alias
		for i, row := range mainRows {
			mainRows[i] = utils.ApplyAliasToRecord(row, alias)
		}
	}
	if len(qs.Query.Joins) > 0 {
		mainRows, err = qs.Query.executeJoins(mainRows)
		if err != nil {
			return nil, err
		}
	}
	result, err := qs.Query.executeQuery(mainRows)
	if err != nil {
		return nil, err
	}
	if qs.Compound != nil {
		leftRes, _ := qs.Compound.Left.executeQuery(mainRows)
		rightRes, _ := qs.Compound.Right.executeQuery(mainRows)
		switch qs.Compound.Operator {
		case UNION:
			result = utils.Union(leftRes, rightRes)
		case INTERSECT:
			result = utils.Intersect(leftRes, rightRes)
		case EXCEPT:
			result = utils.Except(leftRes, rightRes)
		}
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

func (query *SQL) executeQuery(rows []utils.Record) ([]utils.Record, error) {
	if len(rows) == 0 {
		if query.From != nil {
			var err error
			rows, err = query.From.loadData()
			if err != nil {
				return nil, err
			}
		} else {
			rows = []utils.Record{}
		}
	}
	ctx := NewEvalContext()
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
				val := ctx.evalExpression(&Identifier{Value: cond.Field}, row)
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
				result := ctx.evalExpression(query.Where, row)
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
	if simpleQuery && query.Limit != nil {
		targetCount = query.Limit.Offset + query.Limit.Limit
		if len(conds) == 0 {
			var temp []utils.Record
			for _, row := range rows {
				if query.Where != nil {
					result := ctx.evalExpression(query.Where, row)
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
			keyVal := ctx.evalExpression(query.GroupBy.Fields[0], row)
			key := fmt.Sprintf("%v", keyVal)
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
							val := ctx.evalExpression(fc.Args[0], r)
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
							val := ctx.evalExpression(fc.Args[0], r)
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
							val := ctx.evalExpression(fc.Args[0], r)
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
							val := ctx.evalExpression(fc.Args[0], r)
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
							val := ctx.evalExpression(fc.Args[0], r)
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
					resultRow[colName] = ctx.evalExpression(underlying, groupRows[0])
				}
			}
			if query.Having != nil {
				hv := ctx.evalExpression(query.Having, resultRow)
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
						val := ctx.evalExpression(fc.Args[0], r)
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
						val := ctx.evalExpression(fc.Args[0], r)
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
						val := ctx.evalExpression(fc.Args[0], r)
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
						val := ctx.evalExpression(fc.Args[0], r)
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
						val := ctx.evalExpression(fc.Args[0], r)
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
					resultRow[colName] = ctx.evalExpression(underlying, filteredRows[0])
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
					newRow[colName] = ctx.evalExpression(underlying, row)
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
				vi := ctx.evalExpression(expr, resultRows[i])
				vj := ctx.evalExpression(expr, resultRows[j])
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
		end := start + query.Limit.Limit
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

func executeLeftJoin(leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(joinOn, merged)
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

func executeRightJoin(leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(joinOn, merged)
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

func executeFullJoin(leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	leftMatched := make(map[string]bool)
	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(joinOn, merged)
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
			cond := ctx.evalExpression(joinOn, merged)
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

func executeDefaultJoin(leftRows, rightRows []utils.Record, alias string, joinOn Expression) []utils.Record {
	var newResult []utils.Record
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			merged := utils.MergeRows(leftRow, rightRow, alias)
			ctx := NewEvalContext()
			cond := ctx.evalExpression(joinOn, merged)
			if b, ok := cond.(bool); ok && b {
				newResult = append(newResult, merged)
			}
		}
	}
	return newResult
}

func (query *SQL) executeJoins(rows []utils.Record) ([]utils.Record, error) {
	currentRows := rows
	for _, join := range query.Joins {
		joinRows, err := join.Table.loadData()
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
			newResult = executeLeftJoin(currentRows, joinRows, alias, join.On)
		case "RIGHT", "RIGHT JOIN", "RIGHT OUTER", "RIGHT OUTER JOIN":
			newResult = executeRightJoin(currentRows, joinRows, alias, join.On)
		case "FULL", "FULL JOIN", "FULL OUTER", "FULL OUTER JOIN", "OUTER", "OUTER JOIN":
			newResult = executeFullJoin(currentRows, joinRows, alias, join.On)
		default:
			newResult = executeDefaultJoin(currentRows, joinRows, alias, join.On)
		}
		currentRows = newResult
	}
	return currentRows, nil
}
