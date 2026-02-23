package sql

import (
	"testing"

	"github.com/oarkflow/sql/pkg/utils"
)

func TestYearMonthFunctions(t *testing.T) {
	rows := []utils.Record{
		{"id": 1, "created_at": "2026-02-23"},
	}
	result := runQueryOnRows(t, "SELECT YEAR(created_at) AS y, MONTH(created_at) AS m FROM read_service('posts')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if result[0]["y"] != 2026 {
		t.Fatalf("expected year 2026, got %#v", result[0]["y"])
	}
	if result[0]["m"] != 2 {
		t.Fatalf("expected month 2, got %#v", result[0]["m"])
	}
}

func TestFirstLastAggregates(t *testing.T) {
	rows := []utils.Record{
		{"id": 3, "name": "c"},
		{"id": 1, "name": "a"},
		{"id": 2, "name": "b"},
	}
	result := runQueryOnRows(t, "SELECT FIRST(id) AS first_id, LAST(id) AS last_id FROM read_service('posts')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if result[0]["first_id"] != 1 {
		t.Fatalf("expected first_id 1, got %#v", result[0]["first_id"])
	}
	if result[0]["last_id"] != 3 {
		t.Fatalf("expected last_id 3, got %#v", result[0]["last_id"])
	}
}

func TestFirstLastOnArrayValues(t *testing.T) {
	rows := []utils.Record{
		{"id": 1, "tags": []any{"go", "sql", "json"}},
	}
	result := runQueryOnRows(t, "SELECT FIRST(tags[0]) AS first_tag, LAST(tags[2]) AS last_tag FROM read_service('posts')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if result[0]["first_tag"] != "go" {
		t.Fatalf("expected first_tag=go, got %#v", result[0]["first_tag"])
	}
	if result[0]["last_tag"] != "json" {
		t.Fatalf("expected last_tag=json, got %#v", result[0]["last_tag"])
	}
	substrRes := runQueryOnRows(t, "SELECT SUBSTR('abcdef', 2, 3) AS part FROM read_service('posts')", rows)
	if len(substrRes) != 1 || substrRes[0]["part"] != "bcd" {
		t.Fatalf("expected substr bcd, got %#v", substrRes)
	}
}
