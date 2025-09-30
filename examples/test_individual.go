package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql"
)

func main() {
	q1 := `SELECT id FROM read_file('users.csv') WHERE id > 4`
	q2 := `SELECT id FROM read_file('users.csv') WHERE id <= 2`

	r1, _ := sql.Query(context.Background(), q1)
	r2, _ := sql.Query(context.Background(), q2)

	fmt.Printf("Q1: %d records\n", len(r1))
	for _, r := range r1 {
		fmt.Printf("  %+v\n", r)
	}
	fmt.Printf("Q2: %d records\n", len(r2))
	for _, r := range r2 {
		fmt.Printf("  %+v\n", r)
	}
}
