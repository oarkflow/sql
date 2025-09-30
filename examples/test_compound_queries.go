package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func main() {
	fmt.Println("=== Testing All Compound Query Types ===")

	// Test 1: UNION
	fmt.Println("\n1. Testing UNION:")
	query1 := `SELECT id FROM read_file('users.csv') WHERE id > 4 UNION SELECT id FROM read_file('users.csv')`
	testQuery(query1)

	// Test 2: UNION ALL
	fmt.Println("\n2. Testing UNION ALL:")
	query2 := `SELECT id FROM read_file('users.csv') WHERE id > 4 UNION ALL SELECT id FROM read_file('users.csv')`
	testQuery(query2)

	// Test 3: INTERSECT
	fmt.Println("\n3. Testing INTERSECT:")
	query3 := `SELECT id FROM read_file('users.csv') WHERE id > 2 INTERSECT SELECT id FROM read_file('users.csv') WHERE id < 8`
	testQuery(query3)

	// Test 4: EXCEPT
	fmt.Println("\n4. Testing EXCEPT:")
	query4 := `SELECT id FROM read_file('users.csv') WHERE id > 2 EXCEPT SELECT id FROM read_file('users.csv') WHERE id > 6`
	testQuery(query4)

	// Test 5: Multiple compound queries
	fmt.Println("\n5. Testing Multiple Compound Queries:")
	query5 := `SELECT id FROM read_file('users.csv') WHERE id > 4 UNION SELECT id FROM read_file('users.csv') WHERE id > 6 INTERSECT SELECT id FROM read_file('users.csv') WHERE id > 2`
	testQuery(query5)

	fmt.Println("\n=== All Compound Query Tests Completed ===")
}

func testQuery(query string) {
	fmt.Println("Query:", query)

	// Test parsing
	lexer := sql.NewLexer(query)
	parser := sql.NewParser(lexer)
	stmt := parser.ParseQueryStatement()

	if stmt != nil {
		fmt.Printf("  Parsed successfully: Query=%v, Compound=%v\n", stmt.Query != nil, stmt.Compound != nil)

		// Test execution
		records, err := sql.Query(context.Background(), query)
		if err != nil {
			fmt.Printf("  ❌ Execution FAILED: %v\n", err)
		} else {
			fmt.Printf("  ✅ Execution SUCCESS: Found %d records\n", len(records))
		}
	} else {
		fmt.Println("  ❌ Parsing FAILED")
	}
}
