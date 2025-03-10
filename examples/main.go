package main

import (
	"fmt"
	"os"
	"time"

	"github.com/oarkflow/etl/sql"
)

func main() {
	data, err := os.ReadFile("query.sql")
	if err != nil {
		panic(err)
	}
	queryStr := string(data)
	start := time.Now()
	fmt.Println(sql.Query(queryStr))
	fmt.Println(fmt.Sprintf("Latency %s", time.Since(start)))
}
