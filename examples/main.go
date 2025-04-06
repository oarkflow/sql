package main

import (
	"fmt"
	"os"
	"time"

	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/sql"
)

func main() {
	sql.RegisterIntegration("test_db", &sql.SQLIntegration{
		DataConfig: &config.DataConfig{
			Driver:   "postgres",
			Host:     "127.0.0.1",
			Port:     5432,
			Username: "postgres",
			Password: "postgres",
			Database: "clear_dev",
		},
	})
	sql.RegisterIntegration("posts", &sql.RESTIntegration{
		Endpoint: "https://jsonplaceholder.typicode.com/posts",
	})
	sql.RegisterIntegration("comments", &sql.RESTIntegration{
		Endpoint: "https://jsonplaceholder.typicode.com/comments",
	})
	sql.RegisterIntegration("articles", &sql.WebIntegration{
		Endpoint:     "http://metalsucks.net",
		Rules:        "article",
		Target:       "text",
		OutputFormat: "string",
		FieldMappings: []sql.FieldMapping{
			{
				Field:    "title",
				Selector: ".post-title a",
				Target:   "text",
			},
		},
	})
	queries := []string{
		"query.sql",
		// "crawl.sql",
		// "db_query.sql",
	}
	for _, query := range queries {
		bytes, err := os.ReadFile(query)
		if err != nil {
			panic(err)
		}
		queryStr := string(bytes)
		start := time.Now()
		fmt.Println(sql.Query(queryStr))
		fmt.Println("Took", time.Since(start))
		fmt.Println()
	}
}
