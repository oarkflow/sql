package main

import (
	"fmt"
	"os"
	"time"

	"github.com/oarkflow/sql"
	"github.com/oarkflow/sql/integrations"
)

func main() {
	dbService := integrations.Service{
		Name: "test_db",
		Type: integrations.ServiceTypeDB,
		Config: integrations.DatabaseConfig{
			Driver:   "postgres",
			Host:     "127.0.0.1",
			Port:     5432,
			Database: "clear_dev",
		},
	}
	dbCredential := integrations.Credential{
		Type: integrations.CredentialTypeDatabase,
		Data: integrations.DatabaseCredential{
			Username: "postgres",
			Password: "postgres",
		},
	}
	postService := integrations.Service{
		Name: "posts",
		Type: integrations.ServiceTypeAPI,
		Config: integrations.APIConfig{
			URL:    "https://jsonplaceholder.typicode.com/posts",
			Method: "GET",
		},
	}
	commentService := integrations.Service{
		Name: "comments",
		Type: integrations.ServiceTypeAPI,
		Config: integrations.APIConfig{
			URL:    "https://jsonplaceholder.typicode.com/comments",
			Method: "GET",
		},
	}
	articleService := integrations.Service{
		Name: "articles",
		Type: integrations.ServiceTypeWebCrawler,
		Config: integrations.WebCrawlerConfig{
			Endpoint:     "http://metalsucks.net",
			Rules:        "article",
			Target:       "text",
			OutputFormat: "string",
			FieldMappings: []integrations.FieldMapping{
				{
					Field:    "title",
					Selector: ".post-title a",
					Target:   "text",
				},
			},
		},
	}
	sql.RegisterIntegration(dbService, dbCredential)
	sql.RegisterIntegration(postService)
	sql.RegisterIntegration(commentService)
	sql.RegisterIntegration(articleService)
	queries := []string{
		"query.sql",
		"crawl.sql",
		"db_query.sql",
	}
	queries = []string{}
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
	query := `SELECT p.*, c.comment
FROM read_service('posts') AS p
JOIN read_service('comments') AS c ON p.id = c.postId;`
	fmt.Println(sql.Query(query))
}
