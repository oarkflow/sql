package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/oarkflow/sql/pkg/utils"
)

func runQueryOnRows(t *testing.T, query string, rows []utils.Record) []utils.Record {
	t.Helper()
	parser := NewParser(NewLexer(query))
	stmt := parser.ParseQueryStatement()
	if errs := parser.Errors(); len(errs) > 0 {
		t.Fatalf("failed to parse query %q: %v", query, errs)
	}
	if stmt == nil || stmt.Query == nil {
		t.Fatalf("failed to parse query: %s", query)
	}
	result, err := stmt.Query.executeQuery(context.Background(), rows)
	if err != nil {
		t.Fatalf("query execution failed: %v", err)
	}
	return result
}

func TestSelectNestedDotNotation(t *testing.T) {
	rows := []utils.Record{
		{
			"id": 1,
			"address": map[string]any{
				"city": "Gwenborough",
			},
		},
	}
	result := runQueryOnRows(t, "SELECT address.city, id FROM read_service('users')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if city := result[0]["address.city"]; city != "Gwenborough" {
		t.Fatalf("expected city Gwenborough, got %#v", city)
	}
}

func TestWhereArrayOfObjectsPathComparison(t *testing.T) {
	rows := []utils.Record{
		{
			"id": 1,
			"education": []any{
				map[string]any{"year": 2024},
				map[string]any{"year": 2026},
			},
		},
		{
			"id": 2,
			"education": []any{
				map[string]any{"year": 2019},
			},
		},
	}
	result := runQueryOnRows(t, "SELECT id FROM read_service('service1') WHERE education-> year > 2025", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 matching row, got %d", len(result))
	}
	if id := result[0]["id"]; id != 1 {
		t.Fatalf("expected id 1, got %#v", id)
	}
}

func TestWhereInForStringArray(t *testing.T) {
	rows := []utils.Record{
		{"id": 1, "tags": []any{"mysql", "go"}},
		{"id": 2, "tags": []any{"redis"}},
	}
	result := runQueryOnRows(t, "SELECT id FROM read_service('service2') WHERE tags IN ('mysql', 'db')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 matching row, got %d", len(result))
	}
	if id := result[0]["id"]; id != 1 {
		t.Fatalf("expected id 1, got %#v", id)
	}
}

func TestSelectNestedArrayPath(t *testing.T) {
	rows := []utils.Record{
		{
			"id": 1,
			"education": []any{
				map[string]any{"year": 2024},
				map[string]any{"year": 2026},
			},
		},
	}
	result := runQueryOnRows(t, "SELECT education.year FROM read_service('service1')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	years, ok := result[0]["education.year"].([]any)
	if !ok {
		t.Fatalf("expected []any for education.year, got %#v", result[0]["education.year"])
	}
	if len(years) != 2 {
		t.Fatalf("expected 2 years, got %d", len(years))
	}
}

func TestBracketIndexPathSyntax(t *testing.T) {
	rows := []utils.Record{
		{
			"id": 1,
			"education": []any{
				map[string]any{"year": 2022},
				map[string]any{"year": 2026},
			},
			"tags": []any{"go", "mysql"},
		},
	}
	result := runQueryOnRows(t, "SELECT education[1].year, tags[1] FROM read_service('service1')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if got := result[0]["education[1].year"]; got != 2026 {
		t.Fatalf("expected indexed nested year 2026, got %#v", got)
	}
	if got := result[0]["tags[1]"]; got != "mysql" {
		t.Fatalf("expected tags[1]=mysql, got %#v", got)
	}
}

func TestQuotedPathSegment(t *testing.T) {
	rows := []utils.Record{
		{
			"address": map[string]any{
				"zip-code": "92998-3874",
			},
		},
	}
	result := runQueryOnRows(t, `SELECT address."zip-code" FROM read_service('users')`, rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if got := result[0]["address.zip-code"]; got != "92998-3874" {
		t.Fatalf("expected address.zip-code value, got %#v", got)
	}
}

func TestArrayOperatorsAnyAllContainsOverlaps(t *testing.T) {
	rows := []utils.Record{
		{"id": 1, "scores": []any{90, 96}, "tags": []any{"go", "mysql"}},
		{"id": 2, "scores": []any{75, 80}, "tags": []any{"redis"}},
	}

	anyResult := runQueryOnRows(t, "SELECT id FROM read_service('users') WHERE 95 < ANY(scores)", rows)
	if len(anyResult) != 1 || anyResult[0]["id"] != 1 {
		t.Fatalf("expected ANY match for id=1, got %#v", anyResult)
	}

	allResult := runQueryOnRows(t, "SELECT id FROM read_service('users') WHERE 70 < ALL(scores)", rows)
	if len(allResult) != 2 {
		t.Fatalf("expected both rows for ALL condition, got %#v", allResult)
	}

	containsResult := runQueryOnRows(t, "SELECT id FROM read_service('users') WHERE tags CONTAINS 'mysql'", rows)
	if len(containsResult) != 1 || containsResult[0]["id"] != 1 {
		t.Fatalf("expected CONTAINS match for id=1, got %#v", containsResult)
	}

	overlapsResult := runQueryOnRows(t, "SELECT id FROM read_service('users') WHERE tags OVERLAPS ('mysql', 'db')", rows)
	if len(overlapsResult) != 1 || overlapsResult[0]["id"] != 1 {
		t.Fatalf("expected OVERLAPS match for id=1, got %#v", overlapsResult)
	}
}

func TestJSONPathFunctions(t *testing.T) {
	rows := []utils.Record{
		{
			"payload": map[string]any{
				"profile": map[string]any{
					"name": "Leanne",
				},
				"tags": []any{"mysql", "db"},
			},
		},
	}
	result := runQueryOnRows(t, "SELECT JSON_EXTRACT(payload, '$.profile.name') as name, JSON_EXISTS(payload, '$.tags[1]') as has_tag, JSON_VALUE(payload, '$.tags[0]') as first_tag FROM read_service('users')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if got := result[0]["name"]; got != "Leanne" {
		t.Fatalf("expected JSON_EXTRACT name Leanne, got %#v", got)
	}
	if got := result[0]["has_tag"]; got != true {
		t.Fatalf("expected JSON_EXISTS=true, got %#v", got)
	}
	if got := result[0]["first_tag"]; got != "mysql" {
		t.Fatalf("expected JSON_VALUE first_tag=mysql, got %#v", got)
	}
}

func TestTypedCasts(t *testing.T) {
	rows := []utils.Record{
		{"id": 1, "age": "42"},
		{"id": 2, "age": "9"},
	}
	castFn := runQueryOnRows(t, "SELECT id FROM read_service('users') WHERE CAST(age AS int) > 10", rows)
	if len(castFn) != 1 || castFn[0]["id"] != 1 {
		t.Fatalf("expected CAST(age AS int) filter to match id=1, got %#v", castFn)
	}
	castOp := runQueryOnRows(t, "SELECT id FROM read_service('users') WHERE age::int > 10", rows)
	if len(castOp) != 1 || castOp[0]["id"] != 1 {
		t.Fatalf("expected age::int filter to match id=1, got %#v", castOp)
	}
}

func TestLateralUnnest(t *testing.T) {
	rows := []utils.Record{
		{
			"id": 1,
			"education": []any{
				map[string]any{"year": 2023},
				map[string]any{"year": 2026},
			},
		},
	}
	parser := NewParser(NewLexer("SELECT u.id, e.year FROM read_service('users') u CROSS JOIN LATERAL UNNEST(u.education) e"))
	stmt := parser.ParseQueryStatement()
	if errs := parser.Errors(); len(errs) > 0 {
		t.Fatalf("failed to parse lateral unnest query: %v", errs)
	}
	aliased := make([]utils.Record, 0, len(rows))
	for _, row := range rows {
		aliased = append(aliased, utils.ApplyAliasToRecord(row, "u"))
	}
	joined, err := stmt.Query.executeJoins(context.Background(), aliased)
	if err != nil {
		t.Fatalf("executeJoins failed: %v", err)
	}
	projected, err := stmt.Query.executeQuery(context.Background(), joined)
	if err != nil {
		t.Fatalf("executeQuery failed: %v", err)
	}
	if len(projected) != 2 {
		t.Fatalf("expected 2 rows from lateral unnest, got %#v", projected)
	}
}

func TestMalformedPathDiagnostics(t *testing.T) {
	parser := NewParser(NewLexer("SELECT address. FROM read_service('users')"))
	_ = parser.ParseQueryStatement()
	errs := strings.Join(parser.Errors(), " | ")
	if !strings.Contains(errs, "Malformed path after '.'") {
		t.Fatalf("expected parser diagnostic for malformed path, got: %s", errs)
	}
}
