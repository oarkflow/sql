package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func testQuery(description, query string) {
	fmt.Printf("\n=== %s ===\n", description)
	fmt.Printf("Query: %s\n", query)

	records, err := sql.Query(context.Background(), query)
	if err != nil {
		fmt.Printf("❌ FAILED: %v\n", err)
		return
	}

	fmt.Printf("✅ SUCCESS: Found %d records\n", len(records))
	if len(records) > 0 && len(records) <= 3 {
		for i, record := range records {
			fmt.Printf("  Record %d: %+v\n", i+1, record)
		}
	}
}

func main() {
	fmt.Println("🧪 SQL Parser Comprehensive Test Suite")
	fmt.Println("=====================================")

	// Test 1: Basic SELECT
	testQuery("Basic SELECT", "SELECT * FROM read_file('users.csv')")

	// Test 2: SELECT with WHERE
	testQuery("SELECT with WHERE", "SELECT id, first_name FROM read_file('users.csv') WHERE id = 1")

	// Test 3: Aggregate functions
	testQuery("Aggregate functions", "SELECT COUNT(*) as total, AVG(id) as avg_id FROM read_file('users.csv')")

	// Test 4: JOIN
	testQuery("JOIN", "SELECT u.id, u.first_name FROM read_file('users.csv') u JOIN read_file('users.csv') u2 ON u.id = u2.id")

	// Test 5: Subquery
	testQuery("Subquery", "SELECT * FROM read_file('users.csv') WHERE id IN (SELECT id FROM read_file('users.csv') WHERE id <= 3)")

	// Test 6: UNION
	testQuery("UNION", "SELECT id FROM read_file('users.csv') UNION SELECT id FROM read_file('users.csv')")

	// Test 7: Functions
	testQuery("String functions", "SELECT UPPER(first_name) as upper_name, LENGTH(first_name) as name_len FROM read_file('users.csv')")

	// Test 8: CASE expression
	testQuery("CASE expression", "SELECT id, CASE WHEN id <= 3 THEN 'Low' ELSE 'High' END as category FROM read_file('users.csv')")

	// Test 9: ORDER BY
	testQuery("ORDER BY", "SELECT id, first_name FROM read_file('users.csv') ORDER BY id DESC")

	// Test 10: LIMIT
	testQuery("LIMIT", "SELECT id, first_name FROM read_file('users.csv') LIMIT 2")

	fmt.Println("\n🎉 Test suite completed!")
}
