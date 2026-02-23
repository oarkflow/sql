package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type Parser struct {
	l           *Lexer
	curToken    Token
	peekToken   Token
	errors      []string
	currentWith *WithClause
	exprDepth   int
	runtimeCfg  RuntimeConfig
}

func NewParser(l *Lexer) *Parser {
	return NewParserWithRuntimeConfig(l, GetRuntimeConfig())
}

func NewParserWithRuntimeConfig(l *Lexer, cfg RuntimeConfig) *Parser {
	p := &Parser{
		l:          l,
		errors:     []string{},
		runtimeCfg: cfg,
	}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekToken.Type == t {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) ParseQueryStatement() *QueryStatement {
	return p.ParseQueryStatementWithCTE(nil)
}

func (p *Parser) ParseQueryStatementWithCTE(withClause *WithClause) *QueryStatement {
	stmt := &QueryStatement{}
	if p.curToken.Type == WITH && withClause == nil {
		stmt.With = p.parseWithClause()
		p.currentWith = stmt.With
		p.nextToken()
	} else if withClause != nil {
		stmt.With = withClause
		p.currentWith = withClause
	}
	if p.curToken.Type != SELECT {
		p.errors = append(p.errors, fmt.Sprintf("SQL must begin with SELECT (at line %d, col %d)", p.curToken.Line, p.curToken.Column))
		return nil
	}
	query := p.parseSelectQuery()
	stmt.Query = query
	if stmt.With != nil {
		if stmt.Query.From != nil && stmt.Query.From.Source == "" {
			for _, cte := range stmt.With.CTEs {
				if strings.EqualFold(cte.Name, stmt.Query.From.Name) {
					stmt.Query.From.Subquery = cte.Query
					break
				}
			}
		}
		for _, join := range stmt.Query.Joins {
			if join.Table != nil && join.Table.Source == "" {
				for _, cte := range stmt.With.CTEs {
					if strings.EqualFold(cte.Name, join.Table.Name) {
						join.Table.Subquery = cte.Query
						break
					}
				}
			}
		}
	}
	if p.peekToken.Type == UNION || p.peekToken.Type == INTERSECT || p.peekToken.Type == EXCEPT {
		// Advance to the compound operator
		p.nextToken()
		compOp := p.curToken.Type
		// Handle UNION ALL
		if compOp == UNION && p.peekToken.Type == ALL {
			p.nextToken() // consume ALL
			compOp = UNION_ALL
		}
		// Advance to start parsing the right-hand query
		p.nextToken()
		rightStmt := p.ParseQueryStatementWithCTE(stmt.With)
		if rightStmt == nil || rightStmt.Query == nil {
			p.errors = append(p.errors, fmt.Sprintf("Invalid compound query near token %s (line %d, col %d)", p.peekToken.Type, p.peekToken.Line, p.peekToken.Column))
			return nil
		}
		stmt.Compound = &CompoundQuery{
			Left:     stmt.Query,
			Operator: compOp,
			Right:    rightStmt.Query,
		}
		// Resolve CTEs for the right side of compound query
		if stmt.With != nil {
			resolveCTEsForQuery(stmt.With, rightStmt.Query)
		}
	}
	return stmt
}

func (p *Parser) parseWithClause() *WithClause {
	withClause := &WithClause{}
	p.nextToken()
	for {
		if p.curToken.Type != IDENT {
			p.errors = append(p.errors, "Expected CTE name, got "+string(p.curToken.Type))
			return nil
		}
		cteName := p.curToken.Literal
		if !p.expectPeek(AS) {
			return nil
		}
		if !p.expectPeek(LPAREN) {
			return nil
		}
		p.nextToken()
		cteStmt := p.ParseQueryStatementWithCTE(nil)
		if cteStmt == nil || cteStmt.Query == nil {
			p.errors = append(p.errors, "Invalid CTE query for "+cteName)
			return nil
		}
		if !p.expectPeek(RPAREN) {
			p.errors = append(p.errors, "Expected closing parenthesis for CTE "+cteName+", got "+string(p.peekToken.Type)+" instead")
			return nil
		}
		withClause.CTEs = append(withClause.CTEs, CTE{Name: cteName, Query: cteStmt.Query})
		if p.peekToken.Type != COMMA {
			break
		}
		p.nextToken()
		p.nextToken()
	}
	return withClause
}

func resolveCTEsForQuery(with *WithClause, query *SQL) {
	if with == nil || query == nil {
		return
	}
	if query.From != nil && query.From.Source == "" {
		for _, cte := range with.CTEs {
			if strings.EqualFold(cte.Name, query.From.Name) {
				query.From.Subquery = cte.Query
				break
			}
		}
	}
	for _, join := range query.Joins {
		if join.Table != nil && join.Table.Source == "" {
			for _, cte := range with.CTEs {
				if strings.EqualFold(cte.Name, join.Table.Name) {
					join.Table.Subquery = cte.Query
					break
				}
			}
		}
	}
}

func (p *Parser) parseSelectQuery() *SQL {
	query := &SQL{}
	if p.curToken.Type != SELECT {
		p.errors = append(p.errors, "SQL must begin with SELECT")
		return nil
	}
	if p.peekToken.Type == DISTINCT {
		p.nextToken()
		query.Distinct = true
	}
	query.Select = p.parseSelectClause()
	if p.peekToken.Type == FROM {
		p.nextToken() // consume FROM
	} else if p.curToken.Type != FROM {
		p.errors = append(p.errors, fmt.Sprintf("expected FROM token, got %s instead", p.peekToken.Type))
		return nil
	}
	p.nextToken()
	query.From = p.parseTableReference()
	for p.peekTokenIsOneOf([]TokenType{INNER, LEFT, RIGHT, FULL, CROSS, JOIN}) {
		p.nextToken()
		if join := p.parseJoinClause(); join != nil {
			query.Joins = append(query.Joins, join)
		}
	}
	if p.peekToken.Type == WHERE {
		p.nextToken() // consume WHERE
		p.nextToken() // advance to the expression
		query.Where = p.parseExpression(0)
	}
	if p.peekToken.Type == GROUP {
		p.nextToken()
		if !p.expectPeek(BY) {
			return nil
		}
		query.GroupBy = p.parseGroupByClause()
	}
	if p.peekToken.Type == HAVING {
		p.nextToken() // consume HAVING
		p.nextToken() // advance to the expression
		query.Having = p.parseExpression(0)
	}
	if p.peekToken.Type == ORDER {
		query.OrderBy = p.parseOrderByClause()
	}
	// Support LIMIT or OFFSET in either order (Postgres/MySQL variants)
	if p.peekToken.Type == LIMIT || p.peekToken.Type == OFFSET {
		query.Limit = p.parseLimitClause()
	}
	if p.peekToken.Type == SEMICOLON {
		p.nextToken()
	}
	// Don't consume UNION/INTERSECT/EXCEPT tokens here - let compound query detection handle them
	return query
}

func (p *Parser) peekTokenIsOneOf(types []TokenType) bool {
	for _, t := range types {
		if p.peekToken.Type == t {
			return true
		}
	}
	return false
}

func (p *Parser) parseSelectClause() *SelectClause {
	sc := &SelectClause{}
	p.nextToken()
	var fields []Expression
	fields = append(fields, p.parseSelectExpression())
	for p.peekToken.Type == COMMA {
		p.nextToken()
		p.nextToken()
		fields = append(fields, p.parseSelectExpression())
	}
	sc.Fields = fields
	return sc
}

func (p *Parser) parseSelectExpression() Expression {
	if p.curToken.Type == IDENT && strings.HasSuffix(p.curToken.Literal, ".*") {
		alias := strings.TrimSuffix(p.curToken.Literal, ".*")
		return &QualifiedStar{Alias: alias}
	}
	if p.curToken.Type == ASTERISK {
		return &Star{}
	}
	expr := p.parseExpression(0)
	switch p.peekToken.Type {
	case AS:
		p.nextToken()
		p.nextToken()
		if p.curToken.Type == IDENT {
			expr = &AliasExpression{Expr: expr, Alias: p.curToken.Literal}
		}
	case IDENT:
		alias := p.peekToken.Literal
		if !isReservedAlias(alias) {
			p.nextToken()
			expr = &AliasExpression{Expr: expr, Alias: p.curToken.Literal}
		}
	}
	return expr
}

func (p *Parser) parseTableReference() *TableReference {
	lateral := false
	if p.curToken.Type == LATERAL {
		lateral = true
		p.nextToken()
	}
	if p.curToken.Type == IDENT || p.curToken.Type == UNNEST {
		if strings.EqualFold(p.curToken.Literal, "UNNEST") && p.peekToken.Type == LPAREN {
			tr := &TableReference{Source: "unnest", Lateral: lateral}
			if !p.expectPeek(LPAREN) {
				return nil
			}
			p.nextToken()
			expr := p.parseExpression(0)
			if expr == nil {
				p.errors = append(p.errors, "UNNEST requires an array expression, e.g. UNNEST(user.education)")
				return nil
			}
			tr.UnnestExpr = expr
			if !p.expectPeek(RPAREN) {
				p.errors = append(p.errors, "UNNEST is missing closing ')'")
				return nil
			}
			p.parseOptionalTableAlias(tr)
			return tr
		}
		if p.peekToken.Type == LPAREN {
			sourceFunc := strings.ToLower(p.curToken.Literal)
			if strings.HasPrefix(sourceFunc, "read_") {
				tr := &TableReference{Source: sourceFunc, Lateral: lateral}
				if !p.expectPeek(LPAREN) {
					return nil
				}
				p.nextToken()
				if p.curToken.Type != STRING {
					p.errors = append(p.errors, "Data source function expects a string literal argument")
					return nil
				}
				tr.Name = p.curToken.Literal
				if !p.expectPeek(RPAREN) {
					return nil
				}
				p.parseOptionalTableAlias(tr)
				return tr
			}
			return &TableReference{Name: p.curToken.Literal, Lateral: lateral}
		}
		return &TableReference{Name: p.curToken.Literal, Lateral: lateral}
	}
	if p.curToken.Type == LPAREN {
		if p.peekToken.Type == SELECT {
			p.nextToken()
			subStmt := p.ParseQueryStatementWithCTE(p.currentWith)
			if !p.expectPeek(RPAREN) {
				p.errors = append(p.errors, "Expected closing parenthesis for subquery")
				return nil
			}
			// Handle compound queries (UNION, INTERSECT, EXCEPT) in subqueries
			var tr *TableReference
			if subStmt.Compound != nil {
				tr = &TableReference{CompoundSubquery: subStmt}
			} else {
				tr = &TableReference{Subquery: subStmt.Query}
			}
			tr.Lateral = lateral
			p.parseOptionalTableAlias(tr)
			return tr
		} else {
			p.errors = append(p.errors, "Expected SELECT after '(' in table reference")
			return nil
		}
	}
	p.errors = append(p.errors, "Table must be specified using a data source function (e.g. read_file, read_db, read_api) or as a CTE reference")
	return nil
}

func (p *Parser) parseOptionalTableAlias(tr *TableReference) {
	switch p.peekToken.Type {
	case AS:
		p.nextToken()
		p.nextToken()
		if p.curToken.Type == IDENT {
			tr.Alias = p.curToken.Literal
		}
	case IDENT:
		alias := p.peekToken.Literal
		if !isReservedAlias(alias) {
			p.nextToken()
			tr.Alias = p.curToken.Literal
		}
	}
}

func (p *Parser) parseJoinClause() *JoinClause {
	jc := &JoinClause{}
	joinType := ""
	switch p.curToken.Type {
	case NATURAL:
		joinType = "NATURAL"
		p.nextToken()
		if p.curToken.Type != JOIN {
			p.errors = append(p.errors, "expected JOIN after NATURAL")
			return nil
		}
	case INNER, LEFT, RIGHT, FULL, CROSS:
		joinType = p.curToken.Literal
		if p.peekToken.Type == OUTER {
			p.nextToken()
			joinType += " " + p.curToken.Literal
		}
		if !p.expectPeek(JOIN) {
			return nil
		}
	case JOIN:
		joinType = "INNER"
	}
	jc.JoinType = joinType
	p.nextToken()
	jc.Table = p.parseTableReference()
	if jc.Table == nil {
		p.errors = append(p.errors, "JOIN table must be specified using a valid data source function or subquery")
		return nil
	}
	if jc.Table.Source == "unnest" && jc.Table.Lateral {
		if p.peekToken.Type == ON {
			p.nextToken()
			p.nextToken()
			jc.On = p.parseExpression(0)
		}
		return jc
	}
	if joinType != "CROSS" && joinType != "CROSS JOIN" && joinType != "NATURAL" {
		if !p.expectPeek(ON) {
			return nil
		}
		p.nextToken()
		jc.On = p.parseExpression(0)
	}
	return jc
}

func (p *Parser) parseGroupByClause() *GroupByClause {
	gb := &GroupByClause{}
	p.nextToken()
	var fields []Expression
	fields = append(fields, p.parseExpression(0))
	for p.peekToken.Type == COMMA {
		p.nextToken()
		p.nextToken()
		fields = append(fields, p.parseExpression(0))
	}
	gb.Fields = fields
	return gb
}

func (p *Parser) parseCaseExpression() Expression {
	ce := &CaseExpression{}
	p.nextToken()
	for p.curToken.Type == WHEN {
		wc := &WhenClause{}
		p.nextToken()
		wc.Condition = p.parseExpression(0)
		if !p.expectPeek(THEN) {
			return nil
		}
		p.nextToken()
		wc.Result = p.parseExpression(0)
		ce.WhenClauses = append(ce.WhenClauses, wc)
		// Check for more WHEN clauses
		if p.peekToken.Type != WHEN {
			break
		}
		p.nextToken() // consume WHEN for next iteration
	}
	if p.peekToken.Type == ELSE {
		p.nextToken() // consume ELSE
		p.nextToken() // move to ELSE expression
		ce.Else = p.parseExpression(0)
	}
	if !p.expectPeek(END) {
		return nil
	}
	return ce
}

func (p *Parser) parseOrderByClause() *OrderByClause {
	ob := &OrderByClause{}
	p.nextToken()
	if !p.expectPeek(BY) {
		return nil
	}
	p.nextToken()
	var fields []Expression
	var directions []string
	expr := p.parseExpression(0)
	fields = append(fields, expr)
	if p.peekToken.Type == ASC || p.peekToken.Type == DESC {
		p.nextToken()
		directions = append(directions, strings.ToUpper(p.curToken.Literal))
	} else {
		directions = append(directions, "ASC")
	}
	for p.peekToken.Type == COMMA {
		p.nextToken()
		p.nextToken()
		expr := p.parseExpression(0)
		fields = append(fields, expr)
		if p.peekToken.Type == ASC || p.peekToken.Type == DESC {
			p.nextToken()
			directions = append(directions, strings.ToUpper(p.curToken.Literal))
		} else {
			directions = append(directions, "ASC")
		}
	}
	ob.Fields = fields
	ob.Directions = directions
	return ob
}

func (p *Parser) parseLimitClause() *LimitClause {
	lc := &LimitClause{}
	p.nextToken()
	if p.curToken.Type == LIMIT {
		// Parse LIMIT clause
		p.nextToken()
		if p.curToken.Type == ALL {
			// LIMIT ALL => unlimited; keep Limit=0 and handle in executor as unlimited
			lc.Limit = 0
		} else if p.curToken.Type == INT {
			firstVal, err := strconv.Atoi(p.curToken.Literal)
			if err != nil {
				p.errors = append(p.errors, "Invalid LIMIT value")
				return nil
			}
			// MySQL syntax: LIMIT offset, count
			if p.peekToken.Type == COMMA {
				p.nextToken() // consume comma
				p.nextToken() // move to count
				if p.curToken.Type != INT {
					p.errors = append(p.errors, "LIMIT requires an integer count after comma")
					return nil
				}
				countVal, err := strconv.Atoi(p.curToken.Literal)
				if err != nil {
					p.errors = append(p.errors, "Invalid LIMIT count value")
					return nil
				}
				lc.Offset = firstVal
				lc.Limit = countVal
			} else {
				lc.Limit = firstVal
			}
		} else {
			p.errors = append(p.errors, "LIMIT requires ALL or integer")
			return nil
		}
		// Optional OFFSET after LIMIT
		if p.peekToken.Type == OFFSET {
			p.nextToken() // move to OFFSET
			p.nextToken() // move to integer
			if p.curToken.Type != INT {
				p.errors = append(p.errors, "OFFSET requires an integer")
				return nil
			}
			offsetVal, err := strconv.Atoi(p.curToken.Literal)
			if err != nil {
				p.errors = append(p.errors, "Invalid OFFSET value")
				return nil
			}
			lc.Offset = offsetVal
		}
	} else if p.curToken.Type == OFFSET {
		// Parse OFFSET-first variant (Postgres allows OFFSET before LIMIT)
		p.nextToken()
		if p.curToken.Type != INT {
			p.errors = append(p.errors, "OFFSET requires an integer")
			return nil
		}
		offsetVal, err := strconv.Atoi(p.curToken.Literal)
		if err != nil {
			p.errors = append(p.errors, "Invalid OFFSET value")
			return nil
		}
		lc.Offset = offsetVal
		// Optional LIMIT after OFFSET
		if p.peekToken.Type == LIMIT {
			p.nextToken() // move to LIMIT
			p.nextToken() // move to value
			if p.curToken.Type == ALL {
				lc.Limit = 0
			} else if p.curToken.Type == INT {
				firstVal, err := strconv.Atoi(p.curToken.Literal)
				if err != nil {
					p.errors = append(p.errors, "Invalid LIMIT value")
					return nil
				}
				// MySQL-comma style isn't valid here; treat as simple LIMIT count
				lc.Limit = firstVal
			} else {
				p.errors = append(p.errors, "LIMIT requires ALL or integer")
				return nil
			}
		}
	}
	return lc
}

func (p *Parser) parseFunctionCall() Expression {
	fn := &FunctionCall{
		FunctionName: p.curToken.Literal,
	}
	if !p.expectPeek(LPAREN) {
		return nil
	}
	if strings.EqualFold(fn.FunctionName, "CAST") {
		return p.parseCastFunctionCall(fn)
	}
	p.nextToken()
	fn.Args = p.parseExpressionList(COMMA)
	if !p.expectPeek(RPAREN) {
		return nil
	}
	if p.peekToken.Type == OVER {
		p.nextToken()
		if !p.expectPeek(LPAREN) {
			return nil
		}
		partition, order := p.parseWindowSpec()
		return &WindowFunction{
			Func:        fn,
			PartitionBy: partition,
			OrderBy:     order,
		}
	}
	return fn
}

func (p *Parser) parseCastFunctionCall(fn *FunctionCall) Expression {
	p.nextToken()
	valueExpr := p.parseExpression(0)
	if valueExpr == nil {
		p.errors = append(p.errors, "CAST requires an expression before AS, e.g. CAST(score AS int)")
		return nil
	}
	if !p.expectPeek(AS) {
		p.errors = append(p.errors, "CAST syntax error: use CAST(expr AS type), e.g. CAST(age AS int)")
		return nil
	}
	p.nextToken()
	if p.curToken.Type != IDENT {
		p.errors = append(p.errors, fmt.Sprintf("CAST type must be an identifier, got %q", p.curToken.Literal))
		return nil
	}
	typeExpr := &Literal{Value: p.curToken.Literal}
	if !p.expectPeek(RPAREN) {
		p.errors = append(p.errors, "CAST missing closing ')', expected CAST(expr AS type)")
		return nil
	}
	fn.Args = []Expression{valueExpr, typeExpr}
	return fn
}

func (p *Parser) parseWindowSpec() ([]Expression, *OrderByClause) {
	var partition []Expression
	var order *OrderByClause
	p.nextToken()
	if p.curToken.Type == PARTITION {
		if !p.expectPeek(BY) {
			return nil, nil
		}
		p.nextToken()
		partition = append(partition, p.parseExpression(0))
		for p.peekToken.Type == COMMA {
			p.nextToken()
			p.nextToken()
			partition = append(partition, p.parseExpression(0))
		}
	}
	if p.curToken.Type == ORDER {
		order = p.parseOrderByClause()
	}
	if !p.expectPeek(RPAREN) {
		return nil, nil
	}
	return partition, order
}

func (p *Parser) parseExpressionList(separator TokenType) []Expression {
	var list []Expression
	if p.curToken.Type == RPAREN {
		return list
	}
	list = append(list, p.parseExpression(0))
	for p.peekToken.Type == separator {
		p.nextToken()
		p.nextToken()
		list = append(list, p.parseExpression(0))
	}
	return list
}

func (p *Parser) parseInfixExpression(left Expression) Expression {
	switch p.curToken.Type {
	case DOT:
		if p.peekToken.Type != IDENT {
			p.errors = append(p.errors, "Malformed path after '.', expected a field name or quoted segment like address.\"zip-code\"")
			return nil
		}
		p.nextToken()
		path, ok := mergePathExpressions(left, &Identifier{Value: p.curToken.Literal})
		if !ok {
			p.errors = append(p.errors, "Invalid dotted path segment; use identifier syntax like parent.child")
			return nil
		}
		return &Identifier{Value: path}
	case LBRACKET:
		if p.peekToken.Type != INT {
			p.errors = append(p.errors, "Invalid array index syntax. Use numeric index like tags[0]")
			return nil
		}
		p.nextToken()
		indexToken := p.curToken
		if !p.expectPeek(RBRACKET) {
			p.errors = append(p.errors, "Missing closing ']' in indexed path, e.g. education[0].year")
			return nil
		}
		leftID, ok := left.(*Identifier)
		if !ok {
			p.errors = append(p.errors, "Index syntax can only be applied to fields, e.g. education[0]")
			return nil
		}
		return &Identifier{Value: fmt.Sprintf("%s[%s]", leftID.Value, indexToken.Literal)}
	case TYPECAST:
		if p.peekToken.Type != IDENT {
			p.errors = append(p.errors, "Type cast syntax error: use expr::type, e.g. amount::float")
			return nil
		}
		p.nextToken()
		return &FunctionCall{
			FunctionName: "CAST",
			Args: []Expression{
				left,
				&Literal{Value: p.curToken.Literal},
			},
		}
	case ARROW:
		prec := p.curPrecedence()
		p.nextToken()
		right := p.parseExpression(prec)
		if path, ok := mergePathExpressions(left, right); ok {
			return &Identifier{Value: path}
		}
		p.errors = append(p.errors, "Malformed path with '->'. Use field->nestedField, e.g. education->year")
		return nil
	case BETWEEN:
		p.nextToken()
		lower := p.parseExpression(0)
		if lower == nil {
			return nil
		}
		if !p.expectPeek(AND) {
			return nil
		}
		p.nextToken()
		upper := p.parseExpression(0)
		if upper == nil {
			return nil
		}
		return &BinaryExpression{
			Left: &BinaryExpression{
				Left:     left,
				Operator: ">=",
				Right:    lower,
			},
			Operator: "AND",
			Right: &BinaryExpression{
				Left:     left,
				Operator: "<=",
				Right:    upper,
			},
		}
	case IS:
		not := false
		p.nextToken()
		if p.curToken.Type == NOT {
			not = true
			p.nextToken()
		}
		if !p.expectPeek(NULL) {
			return nil
		}
		op := "IS NULL"
		if not {
			op = "IS NOT NULL"
		}
		return &BinaryExpression{
			Left:     left,
			Operator: op,
			Right:    &Literal{Value: nil},
		}
	case IN:
		p.nextToken() // consume IN
		if p.curToken.Type != LPAREN {
			p.peekError(LPAREN)
			return nil
		}
		p.nextToken() // consume (
		if p.curToken.Type == SELECT {
			subStmt := p.ParseQueryStatementWithCTE(p.currentWith)
			if !p.expectPeek(RPAREN) {
				return nil
			}
			return &InExpression{
				Left:     left,
				Not:      false,
				Subquery: &Subquery{Query: subStmt.Query},
			}
		} else {
			list := p.parseExpressionList(COMMA)
			if !p.expectPeek(RPAREN) {
				return nil
			}
			return &InExpression{
				Left: left,
				Not:  false,
				List: list,
			}
		}
	case NOT:
		if p.peekToken.Type == IN {
			p.nextToken() // consume NOT
			p.nextToken() // consume IN
			if p.curToken.Type != LPAREN {
				p.peekError(LPAREN)
				return nil
			}
			p.nextToken() // consume (
			if p.curToken.Type == SELECT {
				subStmt := p.ParseQueryStatementWithCTE(p.currentWith)
				if !p.expectPeek(RPAREN) {
					return nil
				}
				return &InExpression{
					Left:     left,
					Not:      true,
					Subquery: &Subquery{Query: subStmt.Query},
				}
			} else {
				list := p.parseExpressionList(COMMA)
				if !p.expectPeek(RPAREN) {
					return nil
				}
				return &InExpression{
					Left: left,
					Not:  true,
					List: list,
				}
			}
		} else if p.peekToken.Type == LIKE {
			p.nextToken()
			p.nextToken()
			pattern := p.parseExpression(0)
			return &LikeExpression{
				Left:    left,
				Not:     true,
				Pattern: pattern,
			}
		}
		fallthrough
	case LIKE:
		p.nextToken()
		pattern := p.parseExpression(0)
		return &LikeExpression{
			Left:    left,
			Not:     false,
			Pattern: pattern,
		}
	case CONTAINS, OVERLAPS:
		operator := p.curToken.Literal
		if p.peekToken.Type == LPAREN {
			p.nextToken() // consume (
			p.nextToken() // move to first expression
			list := p.parseExpressionList(COMMA)
			if !p.expectPeek(RPAREN) {
				p.errors = append(p.errors, fmt.Sprintf("%s expects a closing ')' for value list", operator))
				return nil
			}
			return &BinaryExpression{
				Left:     left,
				Operator: operator,
				Right:    &ArrayLiteral{Elements: list},
			}
		}
		prec := p.curPrecedence()
		p.nextToken()
		right := p.parseExpression(prec)
		return &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
	default:
		operator := p.curToken.Literal
		// Normalize alternate not-equals representation
		if p.curToken.Type == NOT_EQ {
			operator = "!="
		}
		prec := p.curPrecedence()
		p.nextToken()
		right := p.parseExpression(prec)
		return &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
	}
}

func mergePathExpressions(left, right Expression) (string, bool) {
	leftID, ok := left.(*Identifier)
	if !ok {
		return "", false
	}
	rightID, ok := right.(*Identifier)
	if !ok {
		return "", false
	}
	leftPath := strings.ReplaceAll(strings.TrimSpace(leftID.Value), "->", ".")
	rightPath := strings.ReplaceAll(strings.TrimSpace(rightID.Value), "->", ".")
	leftPath = strings.Trim(leftPath, ".")
	rightPath = strings.Trim(rightPath, ".")
	if leftPath == "" || rightPath == "" {
		return "", false
	}
	return leftPath + "." + rightPath, true
}

func (p *Parser) parseExpression(precedence int) Expression {
	maxDepth := p.runtimeCfg.MaxExpressionDepth
	if maxDepth > 0 && p.exprDepth >= maxDepth {
		p.errors = append(p.errors, fmt.Sprintf("Expression nesting exceeds max depth %d. Simplify expression or increase runtime max_expression_depth.", maxDepth))
		return nil
	}
	p.exprDepth++
	defer func() {
		p.exprDepth--
	}()

	if p.curToken.Type == LPAREN {
		if p.peekToken.Type == SELECT {
			p.nextToken()
			subStmt := p.ParseQueryStatementWithCTE(p.currentWith)
			if !p.expectPeek(RPAREN) {
				return nil
			}
			return &Subquery{Query: subStmt.Query}
		} else {
			p.nextToken()
			exp := p.parseExpression(0)
			if !p.expectPeek(RPAREN) {
				return nil
			}
			return exp
		}
	}
	var leftExp Expression
	switch p.curToken.Type {
	case EXISTS:
		// Parse EXISTS subquery expression.
		if !p.expectPeek(LPAREN) {
			return nil
		}
		p.nextToken()
		subStmt := p.ParseQueryStatementWithCTE(p.currentWith)
		if !p.expectPeek(RPAREN) {
			return nil
		}
		leftExp = &ExistsExpression{Subquery: &Subquery{Query: subStmt.Query}}
	case ASTERISK:
		leftExp = &Star{}
	case CASE:
		leftExp = p.parseCaseExpression()
	case IDENT, COUNT, AVG, SUM, MIN, MAX, DIFF, COALESCE, CONCAT, IF, DATEDIFF, NOW, CURRENT_TIMESTAMP, CURRENT_DATE, ANY, ALL, CAST:
		if p.peekToken.Type == LPAREN {
			leftExp = p.parseFunctionCall()
		} else {
			leftExp = &Identifier{Value: p.curToken.Literal}
		}
	case INT:
		leftExp = &Literal{Value: p.curToken.Literal}
	case FLOAT:
		leftExp = &Literal{Value: p.curToken.Literal}
	case STRING:
		leftExp = &Literal{Value: p.curToken.Literal}
	case BOOL:
		val := strings.ToUpper(p.curToken.Literal) == "TRUE"
		leftExp = &Literal{Value: val}
	case NOT:
		p.nextToken()
		right := p.parseExpression(0)
		leftExp = &PrefixExpression{Operator: "NOT", Right: right}
	case MINUS:
		p.nextToken()
		right := p.parseExpression(0)
		leftExp = &PrefixExpression{Operator: "-", Right: right}
	case PARAM:
		leftExp = &Literal{Value: "?"}
	default:
		return nil
	}
	for p.peekToken.Type != SEMICOLON && p.peekToken.Type != ILLEGAL && precedence < p.peekPrecedence() {
		p.nextToken()
		leftExp = p.parseInfixExpression(leftExp)
	}
	return leftExp
}

func (p *Parser) peekPrecedence() int {
	if prec, ok := precedences[p.peekToken.Type]; ok {
		return prec
	}
	return 0
}

func (p *Parser) curPrecedence() int {
	if prec, ok := precedences[p.curToken.Type]; ok {
		return prec
	}
	return 0
}

func (p *Parser) Errors() []string {
	return p.errors
}
