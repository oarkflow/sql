package sql

import (
	"fmt"
	"strings"
	"unicode"
)

var (
	reserved    = []string{"JOIN", "FROM", "WHERE", "GROUP", "HAVING", "ON", "CASE", "WHEN", "THEN", "ELSE", "END", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WITH", "TRUE", "FALSE"}
	precedences = map[TokenType]int{
		PLUS:     10,
		MINUS:    10,
		ASTERISK: 20,
		SLASH:    20,
		ASSIGN:   5,
		NOT_EQ:   5,
		LT:       5,
		GT:       5,
		LTE:      5,
		GTE:      5,
		BETWEEN:  5,
		LPAREN:   30,

		IN:   5,
		LIKE: 5,
		NOT:  5,
	}
)

type TokenType string

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	IDENT  = "IDENT"
	INT    = "INT"
	FLOAT  = "FLOAT"
	STRING = "STRING"
	BOOL   = "BOOL"
	PARAM  = "PARAM"

	ASSIGN   = "="
	NOT_EQ   = "!="
	LT       = "<"
	GT       = ">"
	LTE      = "<="
	GTE      = ">="
	PLUS     = "+"
	MINUS    = "-"
	ASTERISK = "*"
	SLASH    = "/"

	COMMA     = ","
	SEMICOLON = ";"
	LPAREN    = "("
	RPAREN    = ")"

	CURRENT_DATE = "CURRENT_DATE"

	SELECT   = "SELECT"
	FROM     = "FROM"
	WHERE    = "WHERE"
	GROUP    = "GROUP"
	BY       = "BY"
	HAVING   = "HAVING"
	IN       = "IN"
	NOT      = "NOT"
	LIKE     = "LIKE"
	COUNT    = "COUNT"
	AVG      = "AVG"
	SUM      = "SUM"
	MIN      = "MIN"
	MAX      = "MAX"
	DIFF     = "DIFF"
	AS       = "AS"
	JOIN     = "JOIN"
	INNER    = "INNER"
	LEFT     = "LEFT"
	RIGHT    = "RIGHT"
	FULL     = "FULL"
	OUTER    = "OUTER"
	CROSS    = "CROSS"
	ON       = "ON"
	COALESCE = "COALESCE"
	CONCAT   = "CONCAT"
	IF       = "IF"
	WHEN     = "WHEN"
	THEN     = "THEN"
	ELSE     = "ELSE"
	END      = "END"

	DISTINCT  = "DISTINCT"
	ORDER     = "ORDER"
	ASC       = "ASC"
	DESC      = "DESC"
	LIMIT     = "LIMIT"
	OFFSET    = "OFFSET"
	UNION     = "UNION"
	INTERSECT = "INTERSECT"
	EXCEPT    = "EXCEPT"
	WITH      = "WITH"
	OVER      = "OVER"
	PARTITION = "PARTITION"
	BETWEEN   = "BETWEEN"
	IS        = "IS"
	NULL      = "NULL"
	EXISTS    = "EXISTS"
	ANY       = "ANY"
	ALL       = "ALL"

	ROW_NUMBER = "ROW_NUMBER"
	RANK       = "RANK"
	DENSE_RANK = "DENSE_RANK"

	CASE = "CASE"
)

func lookupKeyword(ident string) TokenType {
	// If the identifier contains a dot then it is a qualified name,
	// so we treat it as a generic identifier.
	if strings.Contains(ident, ".") {
		return IDENT
	}
	switch strings.ToUpper(ident) {
	case "SELECT":
		return SELECT
	case "FROM":
		return FROM
	case "WHERE":
		return WHERE
	case "GROUP":
		return GROUP
	case "BY":
		return BY
	case "HAVING":
		return HAVING
	case "CURRENT_DATE":
		return CURRENT_DATE
	case "IN":
		return IN
	case "NOT":
		return NOT
	case "LIKE":
		return LIKE
	case "COUNT":
		return COUNT
	case "AVG":
		return AVG
	case "SUM":
		return SUM
	case "MIN":
		return MIN
	case "MAX":
		return MAX
	case "DIFF":
		return DIFF
	case "AS":
		return AS
	case "JOIN":
		return JOIN
	case "INNER":
		return INNER
	case "LEFT":
		return LEFT
	case "RIGHT":
		return RIGHT
	case "FULL":
		return FULL
	case "OUTER":
		return OUTER
	case "CROSS":
		return CROSS
	case "ON":
		return ON
	case "COALESCE":
		return COALESCE
	case "CONCAT":
		return CONCAT
	case "IF":
		return IF
	case "WHEN":
		return WHEN
	case "THEN":
		return THEN
	case "ELSE":
		return ELSE
	case "END":
		return END
	case "DISTINCT":
		return DISTINCT
	case "ORDER":
		return ORDER
	case "ASC":
		return ASC
	case "DESC":
		return DESC
	case "LIMIT":
		return LIMIT
	case "OFFSET":
		return OFFSET
	case "UNION":
		return UNION
	case "INTERSECT":
		return INTERSECT
	case "EXCEPT":
		return EXCEPT
	case "WITH":
		return WITH
	case "OVER":
		return OVER
	case "PARTITION":
		return PARTITION
	case "BETWEEN":
		return BETWEEN
	case "IS":
		return IS
	case "NULL":
		return NULL
	case "EXISTS":
		return EXISTS
	case "ANY":
		return ANY
	case "ALL":
		return ALL
	case "ROW_NUMBER":
		return ROW_NUMBER
	case "RANK":
		return RANK
	case "DENSE_RANK":
		return DENSE_RANK
	case "CASE":
		return CASE
	case "TRUE":
		return BOOL
	case "FALSE":
		return BOOL
	default:
		return IDENT
	}
}

func isReservedAlias(s string) bool {
	for _, r := range reserved {
		// Use EqualFold for case-insensitive matching.
		if strings.EqualFold(s, r) {
			return true
		}
	}
	return false
}

type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

func newToken(tokenType TokenType, ch byte, line int, column int) Token {
	return Token{Type: tokenType, Literal: string(ch), Line: line, Column: column}
}

func (t Token) Precedence() int {
	if p, ok := precedences[t.Type]; ok {
		return p
	}
	return 0
}

func (t Token) String() string {
	return fmt.Sprintf("Token(%s, %q, Line: %d, Column: %d)", t.Type, t.Literal, t.Line, t.Column)
}

func isIdentifierChar(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' || ch == '.'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func unwrapAlias(expr Expression) (string, Expression) {
	if ae, ok := expr.(*AliasExpression); ok {
		return ae.Alias, ae.Expr
	}
	return "", expr
}

func getFieldName(expr Expression, index int) string {
	alias, underlying := unwrapAlias(expr)
	if alias != "" {
		return alias
	}
	if id, ok := underlying.(*Identifier); ok {
		return id.Value
	}
	return fmt.Sprintf("col%d", index)
}

func sqlDateFormatToGoLayout(sqlFormat string) string {
	layout := sqlFormat
	layout = strings.ReplaceAll(layout, "YYYY", "2006")
	layout = strings.ReplaceAll(layout, "MM", "01")
	layout = strings.ReplaceAll(layout, "DD", "02")
	return layout
}
