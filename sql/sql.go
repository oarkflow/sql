package sql

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/utils"
	"github.com/oarkflow/etl/pkg/utils/fileutil"
)

type Integration struct {
	Type       string
	DataConfig *config.DataConfig // used for SQL (mysql, postgres, etc.)
	Endpoint   string             // used for REST integration
	Method     string
}

var integrationRegistry = map[string]Integration{
	"test_db": {
		Type: "mysql",
		DataConfig: &config.DataConfig{
			Driver:   "postgres",
			Host:     "127.0.0.1",
			Port:     5432,
			Username: "postgres",
			Password: "postgres",
			Database: "clear_dev",
		},
	},
	"test_rest": {
		Type:     "rest",
		Endpoint: "https://jsonplaceholder.typicode.com/posts",
	},
}

func readService(identifier string) ([]utils.Record, error) {
	parts := strings.SplitN(identifier, ".", 2)
	integrationKey := parts[0]
	var source string
	if len(parts) > 1 {
		source = parts[1]
	}
	integration, exists := integrationRegistry[integrationKey]
	if !exists {
		return nil, fmt.Errorf("integration not found: %s", integrationKey)
	}

	switch strings.ToLower(integration.Type) {
	case "mysql", "postgres", "sqlite", "sqlite3":
		if integration.DataConfig == nil {
			return nil, fmt.Errorf("no data config provided for SQL integration")
		}
		if source == "" {
			return nil, fmt.Errorf("no table name provided for SQL integration")
		}
		db, err := config.OpenDB(*integration.DataConfig)
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = db.Close()
		}()
		src := adapters.NewSQLAdapterAsSource(db, source, "")
		ctx := context.Background()
		err = src.Setup(ctx)
		if err != nil {
			return nil, err
		}
		return src.LoadData()
	case "rest":
		if integration.Method == "" {
			integration.Method = "GET"
		}
		data, err := utils.Request[[]utils.Record](integration.Endpoint, integration.Method, nil)
		if err == nil {
			return data, nil
		}
		singleData, err := utils.Request[utils.Record](integration.Endpoint, integration.Method, nil)
		if err != nil {
			return nil, err
		}
		return []utils.Record{singleData}, nil
	default:
		return nil, fmt.Errorf("unsupported integration type: %s", integration.Type)
	}
}

func (tr *TableReference) loadData() ([]utils.Record, error) {
	if tr.Subquery != nil {
		return tr.Subquery.executeQuery(loadDataForSubquery())
	}
	tableKey := strings.ToLower(tr.Source) + ":" + tr.Name
	switch strings.ToLower(tr.Source) {
	case "read_file":
		fi, err := os.Stat(tr.Name)
		if err == nil {
			modTime := fi.ModTime()
			if entry, exists := tableCache[tableKey]; exists {
				if entry.modTime.Equal(modTime) {
					return entry.rows, nil
				}
			}
			rows, err := fileutil.ProcessFile(tr.Name)
			if err != nil {
				return nil, err
			}
			tableCache[tableKey] = tableCacheEntry{
				rows:    rows,
				modTime: modTime,
			}
			return rows, nil
		}
		return nil, nil
	case "read_service":
		return readService(tr.Name)
	default:
		return nil, fmt.Errorf("unsupported data source: %s", tr.Source)
	}
}

type Node interface {
	TokenLiteral() string
	String() string
}

type Statement interface {
	Node
	statementNode()
}

type Expression interface {
	Node
	ExpressionNode()
}

type Star struct{}

func (s *Star) ExpressionNode()      {}
func (s *Star) TokenLiteral() string { return "*" }
func (s *Star) String() string       { return "*" }

type AliasExpression struct {
	Expr  Expression
	Alias string
}

func (ae *AliasExpression) ExpressionNode()      {}
func (ae *AliasExpression) TokenLiteral() string { return ae.Expr.TokenLiteral() }
func (ae *AliasExpression) String() string {
	return fmt.Sprintf("%s AS %s", ae.Expr.String(), ae.Alias)
}

type FunctionCall struct {
	FunctionName string
	Args         []Expression
}

func (fc *FunctionCall) ExpressionNode()      {}
func (fc *FunctionCall) TokenLiteral() string { return fc.FunctionName }
func (fc *FunctionCall) String() string {
	var args []string
	for _, a := range fc.Args {
		args = append(args, a.String())
	}
	return fmt.Sprintf("%s(%s)", fc.FunctionName, strings.Join(args, ", "))
}

type WindowFunction struct {
	Func        Expression
	PartitionBy []Expression
	OrderBy     *OrderByClause
}

func (wf *WindowFunction) ExpressionNode()      {}
func (wf *WindowFunction) TokenLiteral() string { return wf.Func.TokenLiteral() + " OVER(...)" }
func (wf *WindowFunction) String() string {
	var parts []string
	parts = append(parts, wf.Func.String())
	parts = append(parts, "OVER (")
	if len(wf.PartitionBy) > 0 {
		var partExprs []string
		for _, e := range wf.PartitionBy {
			partExprs = append(partExprs, e.String())
		}
		parts = append(parts, "PARTITION BY "+strings.Join(partExprs, ", "))
	}
	if wf.OrderBy != nil {
		parts = append(parts, wf.OrderBy.String())
	}
	parts = append(parts, ")")
	return strings.Join(parts, " ")
}

type CaseExpression struct {
	WhenClauses []*WhenClause
	Else        Expression
}

func (ce *CaseExpression) ExpressionNode()      {}
func (ce *CaseExpression) TokenLiteral() string { return "CASE" }
func (ce *CaseExpression) String() string {
	var parts []string
	parts = append(parts, "CASE")
	for _, wc := range ce.WhenClauses {
		parts = append(parts, wc.String())
	}
	if ce.Else != nil {
		parts = append(parts, fmt.Sprintf("ELSE %s", ce.Else.String()))
	}
	parts = append(parts, "END")
	return strings.Join(parts, " ")
}

type WhenClause struct {
	Condition Expression
	Result    Expression
}

func (wc *WhenClause) ExpressionNode()      {}
func (wc *WhenClause) TokenLiteral() string { return "WHEN" }
func (wc *WhenClause) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", wc.Condition.String(), wc.Result.String())
}

type Identifier struct {
	Value string
}

func (i *Identifier) ExpressionNode()      {}
func (i *Identifier) TokenLiteral() string { return i.Value }
func (i *Identifier) String() string       { return i.Value }

type Literal struct {
	Value any
}

func (l *Literal) ExpressionNode()      {}
func (l *Literal) TokenLiteral() string { return fmt.Sprintf("%v", l.Value) }
func (l *Literal) String() string {
	switch v := l.Value.(type) {
	case string:
		return "'" + v + "'"
	default:
		return fmt.Sprintf("%v", v)
	}
}

type BinaryExpression struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (be *BinaryExpression) ExpressionNode()      {}
func (be *BinaryExpression) TokenLiteral() string { return be.Operator }
func (be *BinaryExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", be.Left.String(), be.Operator, be.Right.String())
}

type InExpression struct {
	Left Expression
	Not  bool
	List []Expression
}

func (ie *InExpression) ExpressionNode() {}
func (ie *InExpression) TokenLiteral() string {
	if ie.Not {
		return "NOT IN"
	}
	return "IN"
}
func (ie *InExpression) String() string {
	var list []string
	for _, expr := range ie.List {
		list = append(list, expr.String())
	}
	notStr := ""
	if ie.Not {
		notStr = "NOT "
	}
	return fmt.Sprintf("(%s %sIN (%s))", ie.Left.String(), notStr, strings.Join(list, ", "))
}

type LikeExpression struct {
	Left    Expression
	Not     bool
	Pattern Expression
}

func (le *LikeExpression) ExpressionNode() {}
func (le *LikeExpression) TokenLiteral() string {
	if le.Not {
		return "NOT LIKE"
	}
	return "LIKE"
}
func (le *LikeExpression) String() string {
	notStr := ""
	if le.Not {
		notStr = "NOT "
	}
	return fmt.Sprintf("(%s %sLIKE %s)", le.Left.String(), notStr, le.Pattern.String())
}

type Subquery struct {
	Query *SQL
}

func (sq *Subquery) ExpressionNode()      {}
func (sq *Subquery) TokenLiteral() string { return "(" + sq.Query.TokenLiteral() + ")" }
func (sq *Subquery) String() string       { return "(" + sq.Query.String() + ")" }

type TableReference struct {
	Source   string
	Name     string
	Alias    string
	Subquery *SQL
}

func (tr *TableReference) TokenLiteral() string { return tr.Source }
func (tr *TableReference) String() string {
	if tr.Subquery != nil {
		if tr.Alias != "" {
			return fmt.Sprintf("(%s) AS %s", tr.Subquery.String(), tr.Alias)
		}
		return fmt.Sprintf("(%s)", tr.Subquery.String())
	}
	return fmt.Sprintf("%s('%s')", tr.Source, tr.Name)
}

type JoinClause struct {
	JoinType string
	Table    *TableReference
	On       Expression
}

func (jc *JoinClause) TokenLiteral() string { return jc.JoinType + " " + jc.Table.TokenLiteral() }
func (jc *JoinClause) String() string {
	if jc.On != nil {
		return fmt.Sprintf("%s JOIN %s ON %s", jc.JoinType, jc.Table.String(), jc.On.String())
	}
	return fmt.Sprintf("%s JOIN %s", jc.JoinType, jc.Table.String())
}

type OrderByClause struct {
	Fields     []Expression
	Directions []string
}

func (ob *OrderByClause) TokenLiteral() string {
	if len(ob.Fields) > 0 {
		return ob.Fields[0].TokenLiteral()
	}
	return ""
}
func (ob *OrderByClause) String() string {
	var parts []string
	for i, field := range ob.Fields {
		dir := "ASC"
		if i < len(ob.Directions) {
			dir = ob.Directions[i]
		}
		parts = append(parts, fmt.Sprintf("%s %s", field.String(), dir))
	}
	return "ORDER BY " + strings.Join(parts, ", ")
}

type LimitClause struct {
	Limit  int
	Offset int
}

func (l *LimitClause) TokenLiteral() string { return "LIMIT" }
func (l *LimitClause) String() string {
	if l.Offset > 0 {
		return fmt.Sprintf("LIMIT %d OFFSET %d", l.Limit, l.Offset)
	}
	return fmt.Sprintf("LIMIT %d", l.Limit)
}

type CompoundQuery struct {
	Left     *SQL
	Operator TokenType
	Right    *SQL
}

func (cq *CompoundQuery) TokenLiteral() string { return string(cq.Operator) }
func (cq *CompoundQuery) String() string {
	return fmt.Sprintf("%s %s %s", cq.Left.String(), cq.Operator, cq.Right.String())
}

type SelectClause struct {
	Fields []Expression
}

func (sc *SelectClause) TokenLiteral() string {
	if len(sc.Fields) > 0 {
		return sc.Fields[0].TokenLiteral()
	}
	return ""
}
func (sc *SelectClause) String() string {
	var fields []string
	for _, f := range sc.Fields {
		fields = append(fields, f.String())
	}
	return strings.Join(fields, ", ")
}

type GroupByClause struct {
	Fields []Expression
}

func (gb *GroupByClause) TokenLiteral() string {
	if len(gb.Fields) > 0 {
		return gb.Fields[0].TokenLiteral()
	}
	return ""
}
func (gb *GroupByClause) String() string {
	var fields []string
	for _, f := range gb.Fields {
		fields = append(fields, f.String())
	}
	return strings.Join(fields, ", ")
}

type WithClause struct {
	CTEs []CTE
}

type CTE struct {
	Name  string
	Query *SQL
}
