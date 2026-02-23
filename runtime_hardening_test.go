package sql

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/utils"
)

func TestQueryReturnsStructuredParseError(t *testing.T) {
	_, err := Query(context.Background(), "SELCT 1")
	if err == nil {
		t.Fatalf("expected parse error")
	}
	var sqlErr *SQLError
	if !errors.As(err, &sqlErr) {
		t.Fatalf("expected SQLError, got %T", err)
	}
	if sqlErr.Code != ErrCodeParse {
		t.Fatalf("expected ErrCodeParse, got %s", sqlErr.Code)
	}
}

func TestRuntimeMaxRowsCap(t *testing.T) {
	orig := GetRuntimeConfig()
	t.Cleanup(func() { SetRuntimeConfig(orig) })
	cfg := orig
	cfg.MaxResultRows = 2
	SetRuntimeConfig(cfg)

	rows := []utils.Record{
		{"id": 1},
		{"id": 2},
		{"id": 3},
	}
	parser := NewParser(NewLexer("SELECT id FROM read_service('users')"))
	stmt := parser.ParseQueryStatement()
	result, err := stmt.Query.executeQuery(context.Background(), rows)
	if err != nil {
		t.Fatalf("executeQuery failed: %v", err)
	}
	capped := capRows(result, cfg)
	if len(capped) != 2 {
		t.Fatalf("expected capped rows=2, got %d", len(capped))
	}
}

func TestExpressionDepthLimit(t *testing.T) {
	orig := GetRuntimeConfig()
	t.Cleanup(func() { SetRuntimeConfig(orig) })
	cfg := orig
	cfg.MaxExpressionDepth = 3
	SetRuntimeConfig(cfg)

	lexer := NewLexer("SELECT (((((1))))) FROM read_service('users')")
	parser := NewParser(lexer)
	_ = parser.ParseQueryStatement()
	if len(parser.Errors()) == 0 {
		t.Fatalf("expected depth error")
	}
}

func TestFunctionRegistryFreezeAndOverridePolicy(t *testing.T) {
	origOpts := GetFunctionRegistryOptions()
	t.Cleanup(func() { SetFunctionRegistryOptions(origOpts) })
	SetFunctionRegistryOptions(FunctionRegistryOptions{AllowOverride: false, Frozen: false})

	if err := RegisterScalarFunctionE("TMP_FN_A", func(_ *EvalContext, _ context.Context, _ []Expression, _ utils.Record) any { return "a" }); err != nil {
		t.Fatalf("register should succeed: %v", err)
	}
	if err := RegisterScalarFunctionE("TMP_FN_A", func(_ *EvalContext, _ context.Context, _ []Expression, _ utils.Record) any { return "b" }); err == nil {
		t.Fatalf("expected duplicate registration error when override disabled")
	}
	FreezeFunctionRegistry()
	if err := RegisterScalarFunctionE("TMP_FN_B", func(_ *EvalContext, _ context.Context, _ []Expression, _ utils.Record) any { return "x" }); err == nil {
		t.Fatalf("expected registry frozen error")
	}
	UnfreezeFunctionRegistry()
	if err := UnregisterScalarFunction("TMP_FN_A"); err != nil {
		t.Fatalf("unregister should succeed: %v", err)
	}
}

func TestReadServiceIdentifierValidation(t *testing.T) {
	_, err := ReadServiceForUser(context.Background(), "users.bad-table;DROP")
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var sqlErr *SQLError
	if !errors.As(err, &sqlErr) {
		t.Fatalf("expected SQLError, got %T", err)
	}
	if sqlErr.Code != ErrCodeInput {
		t.Fatalf("expected ErrCodeInput, got %s", sqlErr.Code)
	}
}

func TestWithQueryTimeoutUsesContextDeadline(t *testing.T) {
	orig := GetRuntimeConfig()
	t.Cleanup(func() { SetRuntimeConfig(orig) })
	cfg := orig
	cfg.QueryTimeout = 50 * time.Millisecond
	SetRuntimeConfig(cfg)

	ctx, cancel := withQueryTimeout(context.Background(), cfg)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("expected deadline to be set")
	}
	if time.Until(deadline) <= 0 {
		t.Fatalf("expected future deadline")
	}
}

func TestContextScopedRuntimeOverrideForMaxRows(t *testing.T) {
	orig := GetRuntimeConfig()
	t.Cleanup(func() { SetRuntimeConfig(orig) })
	cfg := orig
	cfg.MaxResultRows = 10
	cfg.QueryTimeout = 2 * time.Second
	SetRuntimeConfig(cfg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":1},{"id":2},{"id":3}]`))
	}))
	defer srv.Close()

	ctx := context.WithValue(context.Background(), "user_id", "runtime-ovr-user")
	err := RegisterIntegrationForUser(ctx, integrations.Service{
		Name: "ovrsvc",
		Type: integrations.ServiceTypeAPI,
		Config: integrations.APIConfig{
			URL:     srv.URL,
			Method:  http.MethodGet,
			Timeout: "5s",
		},
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("register integration failed: %v", err)
	}

	maxRows := 1
	ovCtx := WithRuntimeConfigOverride(ctx, RuntimeConfigOverride{
		MaxResultRows: &maxRows,
	})
	rows, err := Query(ovCtx, "SELECT id FROM read_service('ovrsvc')")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row with override, got %d", len(rows))
	}
}
