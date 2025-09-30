package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func main() {
	records, err := sql.Query(context.Background(), "WITH cte1 AS (SELECT id FROM read_file('users.csv') WHERE id <= 2), cte2 AS (SELECT id FROM read_file('users.csv') WHERE id >= 4) SELECT * FROM cte1 UNION SELECT * FROM cte2")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Found %d records\n", len(records))
	for i, r := range records {
		fmt.Printf("Record %d: %+v\n", i+1, r)
	}
}
