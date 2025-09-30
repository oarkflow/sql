package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func main() {
	// Test basic UNION
	query := `SELECT id FROM read_file('users.csv') WHERE id > 4
UNION SELECT id FROM read_file('users.csv')`

	fmt.Println("=== Testing UNION ===")

	fmt.Println("Tokens:")
	lexer := sql.NewLexer(query)
	for {
		tok := lexer.NextToken()
		fmt.Printf("Token: %s, Literal: %s\n", tok.Type, tok.Literal)
		if tok.Type == "EOF" {
			break
		}
	}

	fmt.Println("\nParsing:")
	lexer2 := sql.NewLexer(query)
	parser := sql.NewParser(lexer2)
	stmt := parser.ParseQueryStatement()

	fmt.Printf("stmt: %v\n", stmt != nil)
	if stmt != nil {
		fmt.Printf("With: %v\n", stmt.With != nil)
		fmt.Printf("Query: %v\n", stmt.Query != nil)
		fmt.Printf("Compound: %v\n", stmt.Compound != nil)
	}

	records, err := sql.Query(context.Background(), query)
	if err != nil {
		fmt.Printf("❌ FAILED: %v\n", err)
		return
	}

	fmt.Printf("✅ SUCCESS: Found %d records\n", len(records))
	for i, record := range records {
		fmt.Printf("  Record %d: %+v\n", i+1, record)
	}
}
