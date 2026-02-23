package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/oarkflow/sql/pkg/utils"
)

func BenchmarkParserSimpleSelect(b *testing.B) {
	q := "SELECT id, address.city FROM read_service('users') WHERE id > 1 ORDER BY id DESC LIMIT 10"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := NewParser(NewLexer(q))
		stmt := p.ParseQueryStatement()
		if stmt == nil || len(p.Errors()) > 0 {
			b.Fatalf("parse failed: %v", p.Errors())
		}
	}
}

func BenchmarkEvaluatorNestedWhere(b *testing.B) {
	rows := make([]utils.Record, 0, 1000)
	for i := 0; i < 1000; i++ {
		rows = append(rows, utils.Record{
			"id": i,
			"education": []any{
				map[string]any{"year": 2020 + (i % 10)},
				map[string]any{"year": 2025 + (i % 5)},
			},
			"tags": []any{"go", "db", fmt.Sprintf("tag-%d", i%20)},
		})
	}
	parser := NewParser(NewLexer("SELECT id FROM read_service('service1') WHERE education->year > 2025 AND tags OVERLAPS ('db', 'mysql')"))
	stmt := parser.ParseQueryStatement()
	if len(parser.Errors()) > 0 || stmt == nil || stmt.Query == nil {
		b.Fatalf("parse failed: %v", parser.Errors())
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Query.executeQuery(context.Background(), rows); err != nil {
			b.Fatalf("execution failed: %v", err)
		}
	}
}

func FuzzParserNoPanic(f *testing.F) {
	seeds := []string{
		"SELECT * FROM read_service('users')",
		"SELECT address.city FROM read_service('users') WHERE id > 1",
		"SELECT id FROM read_service('users') WHERE tags OVERLAPS ('mysql','db')",
		"SELECT id FROM read_service('users') WHERE education->year > 2025",
		"SELECT id FROM read_service('users') WHERE age::int > 10",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, query string) {
		if strings.TrimSpace(query) == "" {
			return
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("parser panicked for query %q: %v", query, r)
			}
		}()
		p := NewParser(NewLexer(query))
		_ = p.ParseQueryStatement()
	})
}

func FuzzPathResolverNoPanic(f *testing.F) {
	f.Add("$.profile.name")
	f.Add("$.items[0].id")
	f.Add("address.city")
	f.Fuzz(func(t *testing.T, path string) {
		payload := map[string]any{
			"profile": map[string]any{"name": "Leanne"},
			"items":   []any{map[string]any{"id": 1}, map[string]any{"id": 2}},
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("path resolver panicked for path %q: %v", path, r)
			}
		}()
		_, _ = resolvePathFromValue(payload, normalizeJSONPath(path))
	})
}
