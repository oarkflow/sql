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
