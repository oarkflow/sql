package main

import (
	"fmt"
	"io"
	"os"
	"time"

	goccy "github.com/goccy/go-json"
	"github.com/oarkflow/json"

	"github.com/oarkflow/etl/sql"
)

func init() {
	json.SetMarshaler(goccy.Marshal)
	json.SetUnmarshaler(goccy.Unmarshal)
	json.SetDecoder(func(reader io.Reader) json.IDecoder {
		return goccy.NewDecoder(reader)
	})
	json.SetEncoder(func(writer io.Writer) json.IEncoder {
		return goccy.NewEncoder(writer)
	})
}

func main() {
	bytes, err := os.ReadFile("query.sql")
	if err != nil {
		panic(err)
	}
	queryStr := string(bytes)
	start := time.Now()
	fmt.Println(sql.Query(queryStr))
	fmt.Println("Took", time.Since(start))
	fmt.Println()
}
