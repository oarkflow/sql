package sql

import (
	"strings"
	"unicode"
)

func (l *Lexer) NextToken() Token {
	l.skipWhitespaceAndComments()
	var tok Token
	switch l.ch {
	case '=':
		tok = newToken(ASSIGN, l.ch, l.line, l.column)
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NOT_EQ, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(ILLEGAL, l.ch, l.line, l.column)
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: LTE, Literal: string(ch) + string(l.ch)}
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NOT_EQ, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(LT, l.ch, l.line, l.column)
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: GTE, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(GT, l.ch, l.line, l.column)
		}
	case '+':
		tok = newToken(PLUS, l.ch, l.line, l.column)
	case '-':
		tok = newToken(MINUS, l.ch, l.line, l.column)
	case '*':
		tok = newToken(ASTERISK, l.ch, l.line, l.column)
	case '|':
		if l.peekChar() == '|' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: CONCAT_OP, Literal: string(ch) + string(l.ch)}
		} else {
			tok = newToken(ILLEGAL, l.ch, l.line, l.column)
		}
	case '/':
		tok = newToken(SLASH, l.ch, l.line, l.column)
	case ',':
		tok = newToken(COMMA, l.ch, l.line, l.column)
	case ';':
		tok = newToken(SEMICOLON, l.ch, l.line, l.column)
	case '(':
		tok = newToken(LPAREN, l.ch, l.line, l.column)
	case ')':
		tok = newToken(RPAREN, l.ch, l.line, l.column)
	case '?':
		tok = newToken(PARAM, l.ch, l.line, l.column)
	case '\'':
		tok.Type = STRING
		tok.Literal = l.readString()
	case '"':
		tok.Type = IDENT
		tok.Literal = l.readQuotedIdentifier()
	case '`':
		tok.Type = IDENT
		tok.Literal = l.readBacktickIdentifier()
	case '[':
		tok.Type = IDENT
		tok.Literal = l.readBracketIdentifier()
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if unicode.IsLetter(rune(l.ch)) || l.ch == '_' {
			literal := l.readIdentifier()
			tok.Type = lookupKeyword(literal)
			// Uppercase literals for keywords so downstream comparisons work reliably.
			if tok.Type == IDENT {
				tok.Literal = literal
			} else {
				tok.Literal = strings.ToUpper(literal)
			}
			return tok
		} else if isDigit(l.ch) {
			num := l.readNumber()
			if strings.Contains(num, ".") || strings.ContainsAny(num, "eE") {
				tok.Type = FLOAT
			} else {
				tok.Type = INT
			}
			tok.Literal = num
			return tok
		} else {
			tok = newToken(ILLEGAL, l.ch, l.line, l.column)
		}
	}
	l.readChar()
	return tok
}

func (l *Lexer) readIdentifier() string {
	start := l.position
	for {
		// If current char is '.' and next char is '*' then include them in the identifier.
		if l.ch == '.' && l.peekChar() == '*' {
			l.readChar() // consume '.'
			l.readChar() // consume '*'
			break
		}
		// If current char is '.' and not forming a qualified star then
		// require the following char to be letter or underscore; otherwise break.
		if l.ch == '.' && l.peekChar() != 0 && l.peekChar() != '*' {
			r := rune(l.peekChar())
			if !unicode.IsLetter(r) && r != '_' {
				break
			}
		}
		if !isIdentifierChar(l.ch) {
			break
		}
		l.readChar()
	}
	return l.input[start:l.position]
}

func (l *Lexer) readNumber() string {
	start := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	if l.ch == '.' {
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	if l.ch == 'e' || l.ch == 'E' {
		l.readChar()
		if l.ch == '-' || l.ch == '+' {
			l.readChar()
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	return l.input[start:l.position]
}

func (l *Lexer) readString() string {
	var sb strings.Builder
	l.readChar()
	for {
		if l.ch == 0 {
			break
		}
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				sb.WriteByte('\'')
				l.readChar()
			} else {
				break
			}
		} else if l.ch == '\\' {
			l.readChar()
			switch l.ch {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '\\':
				sb.WriteByte('\\')
			case '\'':
				sb.WriteByte('\'')
			default:
				sb.WriteByte(l.ch)
			}
		} else {
			sb.WriteByte(l.ch)
		}
		l.readChar()
	}
	return sb.String()
}

func (l *Lexer) readQuotedIdentifier() string {
	l.readChar()
	start := l.position
	for l.ch != '"' && l.ch != 0 {
		l.readChar()
	}
	literal := l.input[start:l.position]
	l.readChar()
	return literal
}

func (l *Lexer) readBacktickIdentifier() string {
	l.readChar()
	start := l.position
	for l.ch != '`' && l.ch != 0 {
		l.readChar()
	}
	literal := l.input[start:l.position]
	l.readChar()
	return literal
}

func (l *Lexer) readBracketIdentifier() string {
	l.readChar()
	start := l.position
	for l.ch != ']' && l.ch != 0 {
		l.readChar()
	}
	literal := l.input[start:l.position]
	l.readChar()
	return literal
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
	line         int
	column       int
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input, line: 1, column: 0}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

func (l *Lexer) skipWhitespaceAndComments() {
	for {
		if l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
			l.readChar()
			continue
		}
		if l.ch == '-' && l.peekChar() == '-' {
			l.skipLineComment()
			continue
		}
		if l.ch == '/' && l.peekChar() == '*' {
			l.skipBlockComment()
			continue
		}
		// Support for "#" style comments
		if l.ch == '#' {
			l.skipLineComment()
			continue
		}
		break
	}
}

func (l *Lexer) skipLineComment() {
	for l.ch != '\n' && l.ch != 0 {
		l.readChar()
	}
}

func (l *Lexer) skipBlockComment() {
	l.readChar()
	l.readChar()
	depth := 1
	for depth > 0 {
		if l.ch == 0 {
			break
		}
		if l.ch == '/' && l.peekChar() == '*' {
			l.readChar()
			l.readChar()
			depth++
			continue
		}
		if l.ch == '*' && l.peekChar() == '/' {
			l.readChar()
			l.readChar()
			depth--
			continue
		}
		l.readChar()
	}
}

func (l *Lexer) Reset(input string) {
	l.input = input
	l.position = 0
	l.readPosition = 0
	l.line = 1
	l.column = 0
	l.readChar()
}
