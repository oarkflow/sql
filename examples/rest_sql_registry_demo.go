package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/oarkflow/sql"
	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/utils"
)

func main() {
	ctx := context.WithValue(context.Background(), "user_id", "demo-rest-user")

	// Local mock REST endpoints for service1/service2 so the exact test queries work.
	mockServer := setupMockServer()
	defer mockServer.Close()

	services := []integrations.Service{
		{
			Name: "users",
			Type: integrations.ServiceTypeAPI,
			Config: integrations.APIConfig{
				URL:     "https://jsonplaceholder.typicode.com/users",
				Method:  http.MethodGet,
				Timeout: "10s",
			},
			Enabled: true,
		},
		{
			Name: "posts",
			Type: integrations.ServiceTypeAPI,
			Config: integrations.APIConfig{
				URL:     "https://jsonplaceholder.typicode.com/posts",
				Method:  http.MethodGet,
				Timeout: "10s",
			},
			Enabled: true,
		},
		{
			Name: "service1",
			Type: integrations.ServiceTypeAPI,
			Config: integrations.APIConfig{
				URL:     mockServer.URL + "/service1",
				Method:  http.MethodGet,
				Timeout: "10s",
			},
			Enabled: true,
		},
		{
			Name: "service2",
			Type: integrations.ServiceTypeAPI,
			Config: integrations.APIConfig{
				URL:     mockServer.URL + "/service2",
				Method:  http.MethodGet,
				Timeout: "10s",
			},
			Enabled: true,
		},
	}

	for _, svc := range services {
		if err := sql.RegisterIntegrationForUser(ctx, svc); err != nil {
			panic(fmt.Errorf("register service %s failed: %w", svc.Name, err))
		}
	}

	// Plugin-style function registration example.
	sql.RegisterScalarFunction("REVERSE", func(evalCtx *sql.EvalContext, execCtx context.Context, args []sql.Expression, row utils.Record) any {
		if len(args) == 0 {
			return ""
		}
		s := fmt.Sprintf("%v", evalCtx.Eval(execCtx, args[0], row))
		r := []rune(s)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return string(r)
	})

	queries := []string{
		// Your test query against JSONPlaceholder users API.
		"SELECT address.city, id FROM read_service('users') LIMIT 5",
		// Your test query against array-of-objects data (mock REST /service1).
		"SELECT id, COUNT(*) FROM read_service('service1') WHERE education-> year > 2025 GROUP BY id",
		// Your test query against array-of-strings data (mock REST /service2).
		"SELECT id, COUNT(*) FROM read_service('service2') WHERE tags IN ('mysql', 'db') GROUP BY id",
		// Extra checks with JSONPlaceholder posts API and new functions.
		"SELECT id, YEAR('2026-02-23') as y, MONTH('2026-02-23') as m FROM read_service('posts') LIMIT 1",
		"SELECT REVERSE(title) AS reversed_title FROM read_service('posts') LIMIT 1",
	}

	for i, q := range queries {
		fmt.Printf("\n[%d] %s\n", i+1, q)
		start := time.Now()
		rows, err := sql.Query(ctx, q)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		fmt.Printf("rows=%d elapsed=%s\n", len(rows), time.Since(start))
		for i := 0; i < len(rows); i++ {
			fmt.Printf("  row[%d] = %+v\n", i, rows[i])
		}
	}
}

func setupMockServer() *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/service1", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
  {"id":1,"education":[{"year":2023},{"year":2026}]},
  {"id":2,"education":[{"year":2020}]},
  {"id":3,"education":[{"year":2030}]}
]`))
	})
	handler.HandleFunc("/service2", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
  {"id":1,"tags":["mysql","go"]},
  {"id":2,"tags":["redis"]},
  {"id":3,"tags":["db","sql"]}
]`))
	})
	return httptest.NewServer(handler)
}
