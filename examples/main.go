package main

import (
	"fmt"
	"os"
	"time"

	sql2 "github.com/oarkflow/sql"
	"github.com/oarkflow/sql/pkg/config"
)

func main() {
	sql2.RegisterIntegration("test_db", &sql2.SQLIntegration{
		DataConfig: &config.DataConfig{
			Driver:   "postgres",
			Host:     "127.0.0.1",
			Port:     5432,
			Username: "postgres",
			Password: "postgres",
			Database: "clear_dev",
		},
	})
	sql2.RegisterIntegration("posts", &sql2.RESTIntegration{
		Endpoint: "https://jsonplaceholder.typicode.com/posts",
	})
	sql2.RegisterIntegration("comments", &sql2.RESTIntegration{
		Endpoint: "https://jsonplaceholder.typicode.com/comments",
	})
	sql2.RegisterIntegration("articles", &sql2.WebIntegration{
		Endpoint:     "http://metalsucks.net",
		Rules:        "article",
		Target:       "text",
		OutputFormat: "string",
		FieldMappings: []sql2.FieldMapping{
			{
				Field:    "title",
				Selector: ".post-title a",
				Target:   "text",
			},
		},
	})
	queries := []string{
		"query.sql",
		"crawl.sql",
		"db_query.sql",
	}
	for _, query := range queries {
		bytes, err := os.ReadFile(query)
		if err != nil {
			panic(err)
		}
		queryStr := string(bytes)
		start := time.Now()
		fmt.Println(sql2.Query(queryStr))
		fmt.Println("Took", time.Since(start))
		fmt.Println()
	}
}
