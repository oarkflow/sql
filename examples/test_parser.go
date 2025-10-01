package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func testQuery1(description, query string) {
	fmt.Printf("\n=== %s ===\n", description)
	fmt.Printf("Query: %s\n", query)

	records, err := sql.Query(context.Background(), query)
	if err != nil {
		fmt.Printf("❌ FAILED: %v\n", err)
		return
	}

	fmt.Printf("✅ SUCCESS: Found %d records\n", len(records))
	if len(records) > 0 && (len(records) <= 3 || description == "Single SELECT id" || description == "UNION") {
		for i, record := range records {
			fmt.Printf("  Record %d: %+v\n", i+1, record)
		}
	}
}

func main() {
	fmt.Println("🧪 SQL Parser Comprehensive Test Suite")
	fmt.Println("=====================================")

	// Test 1: Basic SELECT
	testQuery1("Basic SELECT", "SELECT * FROM read_file('users.csv')")

	// Test 2: SELECT with WHERE
	testQuery1("SELECT with WHERE", "SELECT id, first_name FROM read_file('users.csv') WHERE id = 1")

	// Test 3: Aggregate functions
	testQuery1("Aggregate functions", "SELECT COUNT(*) as total, AVG(id) as avg_id FROM read_file('users.csv')")

	// Test 4: JOIN
	testQuery1("JOIN", "SELECT u.id, u.first_name FROM read_file('users.csv') u JOIN read_file('users.csv') u2 ON u.id = u2.id")

	// Test 5: Subquery
	testQuery1("Subquery", "SELECT * FROM read_file('users.csv') WHERE id IN (SELECT id FROM read_file('users.csv'))")

	// Test 6: UNION
	testQuery1("UNION", "SELECT id FROM read_file('users.csv') UNION SELECT id FROM read_file('users.csv')")

	// Test 6.5: Single SELECT id
	testQuery1("Single SELECT id", "SELECT id FROM read_file('users.csv')")

	// Test 7: Functions
	testQuery1("String functions", "SELECT UPPER(first_name) as upper_name, LENGTH(first_name) as name_len FROM read_file('users.csv')")

	// Test 8: CASE expression
	testQuery1("CASE expression", "SELECT id, CASE WHEN id <= 3 THEN 'Low' ELSE 'High' END as category FROM read_file('users.csv')")

	// Test 9: ORDER BY
	testQuery1("ORDER BY", "SELECT id, first_name FROM read_file('users.csv') ORDER BY id DESC")

	// Test 11: CTE
	testQuery1("CTE", "WITH cte AS (SELECT id FROM read_file('users.csv') WHERE id <= 3) SELECT * FROM cte")

	fmt.Println("\n🎉 Test suite completed!")
}
