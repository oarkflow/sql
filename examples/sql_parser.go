package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func main() {
	records, err := sql.Query(context.Background(), "SELECT id, first_name FROM read_file('users.csv') WHERE id = 1")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Found %d records\n", len(records))
	for i, r := range records {
		fmt.Printf("Record %d: %+v\n", i+1, r)
	}
}
