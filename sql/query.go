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

	"github.com/oarkflow/etl/pkg/utils"
)

type tableCacheEntry struct {
	rows    []utils.Record
	modTime time.Time
}

var tableCache = make(map[string]tableCacheEntry)

// --- Regex Caching for LIKE Operator ---

var likeRegexCache = make(map[string]*regexp.Regexp)
var likeRegexCacheMu sync.RWMutex

// compileLikeRegex manually converts an SQL LIKE pattern to a regex and caches it.
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
			// Escape regex special characters.
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
	likeRegexCache[pattern] = compiled
	likeRegexCacheMu.Unlock()
	return compiled, nil
}

// sqlLikeMatch returns true if s matches the SQL LIKE pattern.
func sqlLikeMatch(s, pattern string) bool {
	re, err := compileLikeRegex(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(s)
}

// --- Query Execution Entry Point ---

func Query(query string) ([]utils.Record, error) {
	lexer := NewLexer(query)
	parser := NewParser(lexer)
	program := parser.ParseQueryStatement()
	if len(parser.errors) != 0 {
		return nil, errors.New(strings.Join(parser.errors, "\t"))
	}
	return program.parseAndExecute()
}

// --- QueryStatement and SQL ---

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

func (query *SQL) executeQuery(rows []utils.Record) ([]utils.Record, error) {
	if rows == nil || len(rows) == 0 {
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

	indexApplied := false
	if query.Where != nil {
		if be, ok := query.Where.(*BinaryExpression); ok && be.Operator == "=" {
			if ident, ok := be.Left.(*Identifier); ok {
				if lit, ok := be.Right.(*Literal); ok {
					indexApplied = true
					keyName := ident.Value
					filterValue := fmt.Sprintf("%v", lit.Value)
					index := make(map[string][]utils.Record)
					for _, row := range rows {
						if val, exists := row[keyName]; exists {
							keyStr := fmt.Sprintf("%v", val)
							index[keyStr] = append(index[keyStr], row)
						}
					}
					if recs, found := index[filterValue]; found {
						filteredRows = recs
					} else {
						filteredRows = []utils.Record{}
					}
				}
			}
		}
	}
	if !indexApplied {
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
	ctx.CurrentResultSet = filteredRows

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
				if _, ok := underlying.(*Star); ok {
					for k, v := range row {
						newRow[k] = v
					}
				} else {
					newRow[colName] = ctx.evalExpression(underlying, row)
				}
			}
			resultRows = append(resultRows, newRow)
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
		if joinType == "CROSS" || joinType == "CROSS JOIN" {
			for _, leftRow := range currentRows {
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					newResult = append(newResult, merged)
				}
			}
		} else if joinType == "LEFT" || joinType == "LEFT JOIN" || joinType == "LEFT OUTER" || joinType == "LEFT OUTER JOIN" {
			for _, leftRow := range currentRows {
				matched := false
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
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
		} else if joinType == "RIGHT" || joinType == "RIGHT JOIN" || joinType == "RIGHT OUTER" || joinType == "RIGHT OUTER JOIN" {
			for _, rightRow := range joinRows {
				matched := false
				for _, leftRow := range currentRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
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
		} else if joinType == "FULL" || joinType == "FULL JOIN" || joinType == "FULL OUTER" || joinType == "FULL OUTER JOIN" || joinType == "OUTER" || joinType == "OUTER JOIN" {
			leftMatched := make(map[string]bool)
			for _, leftRow := range currentRows {
				matched := false
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
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
			for _, rightRow := range joinRows {
				matched := false
				for _, leftRow := range currentRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
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
		} else {
			for _, leftRow := range currentRows {
				for _, rightRow := range joinRows {
					merged := utils.MergeRows(leftRow, rightRow, alias)
					ctx := NewEvalContext()
					cond := ctx.evalExpression(join.On, merged)
					if b, ok := cond.(bool); ok && b {
						newResult = append(newResult, merged)
					}
				}
			}
		}
		currentRows = newResult
	}
	return currentRows, nil
}
