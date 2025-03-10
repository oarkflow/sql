package sql

import (
	"strings"
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
	NotEq    = "!="
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
	AND       = "AND"
	NULL      = "NULL"
	EXISTS    = "EXISTS"
	ANY       = "ANY"
	ALL       = "ALL"

	RowNumber = "ROW_NUMBER"
	RANK      = "RANK"
	DenseRank = "DENSE_RANK"

	CASE = "CASE"
)

func lookupKeyword(ident string) TokenType {
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
		return RowNumber
	case "RANK":
		return RANK
	case "DENSE_RANK":
		return DenseRank
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

type Token struct {
	Type    TokenType
	Literal string
}
