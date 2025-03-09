package main

import (
	"fmt"
	"os"

	"github.com/oarkflow/etl/sql"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go \"<SQL query string>\"")
		os.Exit(1)
	}
	queryStr := os.Args[1]
	fmt.Println(sql.Query(queryStr))
}
