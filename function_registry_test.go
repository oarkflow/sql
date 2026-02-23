package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/oarkflow/sql/pkg/utils"
)

func TestRegisterScalarFunction(t *testing.T) {
	RegisterScalarFunction("REVERSE", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) == 0 {
			return ""
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)
	})

	rows := []utils.Record{
		{"name": "abc"},
	}
	result := runQueryOnRows(t, "SELECT REVERSE(name) AS reversed FROM read_service('users')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if got := result[0]["reversed"]; got != "cba" {
		t.Fatalf("expected reversed=cba, got %#v", got)
	}
}

func TestRegisterAggregateFunction(t *testing.T) {
	RegisterAggregateFunction("MEDIAN", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 || len(rows) == 0 {
			return nil
		}
		var vals []float64
		for _, row := range rows {
			v := ctx.evalExpression(execCtx, args[0], row)
			n, ok := utils.ToFloat64(v)
			if ok == nil {
				vals = append(vals, n)
			}
		}
		if len(vals) == 0 {
			return nil
		}
		sort.Float64s(vals)
		mid := len(vals) / 2
		if len(vals)%2 == 1 {
			return vals[mid]
		}
		return (vals[mid-1] + vals[mid]) / 2.0
	})

	rows := []utils.Record{
		{"id": 10},
		{"id": 30},
		{"id": 20},
	}
	result := runQueryOnRows(t, "SELECT MEDIAN(id) AS m FROM read_service('posts')", rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if got := fmt.Sprintf("%v", result[0]["m"]); !strings.HasPrefix(got, "20") {
		t.Fatalf("expected median around 20, got %#v", result[0]["m"])
	}
}
