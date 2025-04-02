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
	/* sql.AddIntegration("test_db", sql.Integration{
		Type: "postgres",
		DataConfig: &config.DataConfig{
			Driver:   "postgres",
			Host:     "127.0.0.1",
			Port:     5432,
			Username: "postgres",
			Password: "postgres",
			Database: "clear_dev",
		},
	})
	sql.AddIntegration("posts", sql.Integration{
		Type:     "rest",
		Endpoint: "https://jsonplaceholder.typicode.com/posts",
	})
	sql.AddIntegration("comments", sql.Integration{
		Type:     "rest",
		Endpoint: "https://jsonplaceholder.typicode.com/comments",
	}) */
	sql.AddIntegration("articles", sql.Integration{
		Type:     "web",
		Endpoint: "http://metalsucks.net",
		// CSS selector for row container; here it selects each article element.
		Rules: "article",
		// These options are not used in the multi-target branch.
		Target:       "text",
		OutputFormat: "string",
		FieldMappings: []sql.FieldMapping{
			{
				Field:    "title",
				Selector: ".post-title a", // Adjust the relative selector as needed.
				Target:   "text",
			},
		},
	})
	bytes, err := os.ReadFile("crawl.sql")
	if err != nil {
		panic(err)
	}
	queryStr := string(bytes)
	start := time.Now()
	fmt.Println(sql.Query(queryStr))
	fmt.Println("Took", time.Since(start))
	fmt.Println()
}
