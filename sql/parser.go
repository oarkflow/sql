package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type Parser struct {
	l         *Lexer
	curToken  Token
	peekToken Token
	errors    []string
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
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
	stmt := &QueryStatement{}
	if p.curToken.Type == WITH {
		stmt.With = p.parseWithClause()
		p.nextToken()
	}
	if p.curToken.Type != SELECT {
		p.errors = append(p.errors, "SQL must begin with SELECT")
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
		compOp := p.peekToken.Type
		p.nextToken()
		p.nextToken()
		rightStmt := p.ParseQueryStatement()
		if rightStmt == nil || rightStmt.Query == nil {
			p.errors = append(p.errors, "Invalid compound query")
			return nil
		}
		stmt.Compound = &CompoundQuery{
			Left:     stmt.Query,
			Operator: compOp,
			Right:    rightStmt.Query,
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
		cteStmt := p.ParseQueryStatement()
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
	// --- Begin change ---
	// Ensure the FROM token is explicitly consumed.
	if p.peekToken.Type == FROM {
		p.nextToken() // consume FROM
	} else if p.curToken.Type != FROM {
		p.errors = append(p.errors, fmt.Sprintf("expected FROM token, got %s instead", p.peekToken.Type))
		return nil
	}
	p.nextToken()
	// --- End change ---
	query.From = p.parseTableReference()
	for p.peekTokenIsOneOf([]TokenType{INNER, LEFT, RIGHT, FULL, CROSS, JOIN}) {
		p.nextToken()
		join := p.parseJoinClause()
		if join != nil {
			query.Joins = append(query.Joins, join)
		}
	}
	if p.peekToken.Type == WHERE {
		p.nextToken()
		p.nextToken()
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
		p.nextToken()
		p.nextToken()
		query.Having = p.parseExpression(0)
	}
	if p.peekToken.Type == ORDER {
		query.OrderBy = p.parseOrderByClause()
	}
	if p.peekToken.Type == LIMIT {
		query.Limit = p.parseLimitClause()
	}
	if p.peekToken.Type == SEMICOLON {
		p.nextToken()
	}
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
	// Check for qualified star (e.g. "p.*")
	if p.curToken.Type == IDENT && strings.HasSuffix(p.curToken.Literal, ".*") {
		alias := strings.TrimSuffix(p.curToken.Literal, ".*")
		return &QualifiedStar{Alias: alias}
	}
	if p.curToken.Type == ASTERISK {
		return &Star{}
	}
	expr := p.parseExpression(0)
	if p.peekToken.Type == AS {
		p.nextToken()
		p.nextToken()
		if p.curToken.Type == IDENT {
			expr = &AliasExpression{Expr: expr, Alias: p.curToken.Literal}
		}
	} else if p.peekToken.Type == IDENT {
		alias := p.peekToken.Literal
		if !isReservedAlias(alias) {
			p.nextToken()
			expr = &AliasExpression{Expr: expr, Alias: p.curToken.Literal}
		}
	}
	return expr
}

func (p *Parser) parseTableReference() *TableReference {
	if p.curToken.Type == IDENT {
		if p.peekToken.Type == LPAREN {
			sourceFunc := strings.ToLower(p.curToken.Literal)
			if strings.HasPrefix(sourceFunc, "read_") {
				tr := &TableReference{Source: sourceFunc}
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
				if p.peekToken.Type == AS {
					p.nextToken()
					p.nextToken()
					if p.curToken.Type == IDENT {
						tr.Alias = p.curToken.Literal
					}
				} else if p.peekToken.Type == IDENT {
					alias := p.peekToken.Literal
					if !isReservedAlias(alias) {
						p.nextToken()
						tr.Alias = p.curToken.Literal
					}
				}
				return tr
			}
			return &TableReference{Name: p.curToken.Literal}
		}
		return &TableReference{Name: p.curToken.Literal}
	}
	if p.curToken.Type == LPAREN {
		if p.peekToken.Type == SELECT {
			p.nextToken()
			subStmt := p.ParseQueryStatement()
			if !p.expectPeek(RPAREN) {
				p.errors = append(p.errors, "Expected closing parenthesis for subquery")
				return nil
			}
			tr := &TableReference{Subquery: subStmt.Query}
			if p.peekToken.Type == AS {
				p.nextToken()
				p.nextToken()
				if p.curToken.Type == IDENT {
					tr.Alias = p.curToken.Literal
				}
			} else if p.peekToken.Type == IDENT {
				alias := p.peekToken.Literal
				if !isReservedAlias(alias) {
					p.nextToken()
					tr.Alias = p.curToken.Literal
				}
			}
			return tr
		} else {
			p.errors = append(p.errors, "Expected SELECT after '(' in table reference")
			return nil
		}
	}
	p.errors = append(p.errors, "Table must be specified using a data source function (e.g. read_file, read_db, read_api) or as a CTE reference")
	return nil
}

func (p *Parser) parseJoinClause() *JoinClause {
	jc := &JoinClause{}
	joinType := ""
	if p.curToken.Type == INNER || p.curToken.Type == LEFT || p.curToken.Type == RIGHT || p.curToken.Type == FULL || p.curToken.Type == CROSS {
		joinType = p.curToken.Literal
		if p.peekToken.Type == OUTER {
			p.nextToken()
			joinType += " " + p.curToken.Literal
		}
		if !p.expectPeek(JOIN) {
			return nil
		}
	} else if p.curToken.Type == JOIN {
		joinType = "INNER"
	}
	jc.JoinType = joinType
	p.nextToken()
	jc.Table = p.parseTableReference()
	if jc.Table == nil {
		p.errors = append(p.errors, "JOIN table must be specified using a valid data source function or subquery")
		return nil
	}
	if joinType != "CROSS" && joinType != "CROSS JOIN" {
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
	}
	if p.curToken.Type == ELSE {
		p.nextToken()
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
		p.nextToken()
		if p.curToken.Type != INT {
			p.errors = append(p.errors, "LIMIT requires an integer")
			return nil
		}
		limitVal, err := strconv.Atoi(p.curToken.Literal)
		if err != nil {
			p.errors = append(p.errors, "Invalid LIMIT value")
			return nil
		}
		lc.Limit = limitVal
	}
	if p.peekToken.Type == OFFSET {
		p.nextToken()
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
	case BETWEEN:
		p.nextToken()
		lower := p.parseExpression(0)
		if lower == nil {
			return nil
		}
		if !p.expectPeek("AND") {
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
		p.nextToken()
		if !p.expectPeek(LPAREN) {
			return nil
		}
		list := p.parseExpressionList(COMMA)
		if !p.expectPeek(RPAREN) {
			return nil
		}
		return &InExpression{
			Left: left,
			Not:  false,
			List: list,
		}
	case NOT:
		if p.peekToken.Type == IN {
			p.nextToken()
			if !p.expectPeek(LPAREN) {
				return nil
			}
			list := p.parseExpressionList(COMMA)
			if !p.expectPeek(RPAREN) {
				return nil
			}
			return &InExpression{
				Left: left,
				Not:  true,
				List: list,
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
	default:
		operator := p.curToken.Literal
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

func (p *Parser) parseExpression(precedence int) Expression {
	if p.curToken.Type == LPAREN {
		if p.peekToken.Type == SELECT {
			p.nextToken()
			subStmt := p.ParseQueryStatement()
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
	case ASTERISK:
		leftExp = &Star{}
	case IDENT, COUNT, AVG, SUM, MIN, MAX, DIFF, COALESCE, CONCAT, IF:
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
