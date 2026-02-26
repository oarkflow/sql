package integrations

import "testing"

func TestDBAccessControlTableRules(t *testing.T) {
	ac := newDBAccessControl(DatabaseConfig{
		TableWhitelist: []string{"users", "orders"},
		TableDenylist:  []string{"audit_logs"},
	})

	if !ac.tableAllowed("users") {
		t.Fatalf("expected users table to be allowed")
	}
	if !ac.tableAllowed("public.users") {
		t.Fatalf("expected schema-qualified users table to be allowed")
	}
	if ac.tableAllowed("audit_logs") {
		t.Fatalf("expected denylisted table to be blocked")
	}
	if ac.tableAllowed("payments") {
		t.Fatalf("expected table outside whitelist to be blocked")
	}
}

func TestDBAccessControlFieldRules(t *testing.T) {
	ac := newDBAccessControl(DatabaseConfig{
		FieldWhitelist: []string{"users.id", "users.name", "orders.*", "status"},
		FieldDenylist:  []string{"password", "users.secret", "logs.*"},
	})

	if !ac.fieldAllowed("users", "id") {
		t.Fatalf("expected users.id to be allowed")
	}
	if !ac.fieldAllowed("users", "status") {
		t.Fatalf("expected global whitelisted field to be allowed")
	}
	if ac.fieldAllowed("users", "password") {
		t.Fatalf("expected globally denylisted field to be blocked")
	}
	if ac.fieldAllowed("users", "secret") {
		t.Fatalf("expected table-specific denylisted field to be blocked")
	}
	if ac.fieldAllowed("users", "email") {
		t.Fatalf("expected non-whitelisted users.email to be blocked")
	}
	if !ac.fieldAllowed("orders", "amount") {
		t.Fatalf("expected orders.* whitelist to allow field")
	}
	if ac.fieldAllowed("logs", "id") {
		t.Fatalf("expected logs.* denylist to block field")
	}
}

func TestDBAccessControlValidateQuery(t *testing.T) {
	ac := newDBAccessControl(DatabaseConfig{
		TableWhitelist: []string{"users"},
		FieldWhitelist: []string{"users.id", "users.name", "users.status"},
		FieldDenylist:  []string{"users.password"},
	})

	if err := ac.validateQuery("SELECT id, name FROM users WHERE status = 'active'", false); err != nil {
		t.Fatalf("expected query to pass validation, got error: %v", err)
	}
	if err := ac.validateQuery("SELECT password FROM users", false); err != nil {
		t.Fatalf("expected read query to pass; denied fields are filtered from output: %v", err)
	}
	if err := ac.validateQuery("SELECT id FROM users WHERE password = 'x'", false); err != nil {
		t.Fatalf("expected read query predicate to pass validation: %v", err)
	}
	if err := ac.validateQuery("SELECT id FROM payments", false); err == nil {
		t.Fatalf("expected non-whitelisted table to fail validation")
	}
	if err := ac.validateQuery("UPDATE users SET password = 'x' WHERE id = '1'", true); err == nil {
		t.Fatalf("expected write query to fail when denylisted field is used")
	}
}

func TestDBAccessControlValidateQueryWithAlias(t *testing.T) {
	ac := newDBAccessControl(DatabaseConfig{
		FieldDenylist: []string{"users.password"},
	})

	err := ac.validateQuery("SELECT u.password FROM users u", false)
	if err != nil {
		t.Fatalf("expected read query alias to pass validation, got: %v", err)
	}
	err = ac.validateQuery("UPDATE users u SET password = 'x' WHERE u.id = '1'", true)
	if err == nil {
		t.Fatalf("expected write query alias to fail validation")
	}
}

func TestDBAccessControlFilterRows(t *testing.T) {
	ac := newDBAccessControl(DatabaseConfig{
		FieldWhitelist: []string{"id", "name"},
		FieldDenylist:  []string{"password"},
	})
	rows := []map[string]any{
		{"id": 1, "name": "alice", "password": "secret", "status": "active"},
	}

	filtered := ac.filterRows(rows, "users")
	if len(filtered) != 1 {
		t.Fatalf("expected one filtered row, got %d", len(filtered))
	}
	row := filtered[0]
	if _, ok := row["password"]; ok {
		t.Fatalf("expected password field to be removed")
	}
	if _, ok := row["status"]; ok {
		t.Fatalf("expected non-whitelisted field to be removed")
	}
	if _, ok := row["id"]; !ok {
		t.Fatalf("expected id field to remain")
	}
	if _, ok := row["name"]; !ok {
		t.Fatalf("expected name field to remain")
	}
}

func TestAnalyzeQueryAccessExtractsTables(t *testing.T) {
	analysis := analyzeQueryAccess("SELECT u.id, o.total FROM public.users u JOIN orders o ON u.id = o.user_id")
	if len(analysis.Tables) != 2 {
		t.Fatalf("expected two tables, got %d", len(analysis.Tables))
	}
	if _, ok := analysis.Tables["public.users"]; !ok {
		t.Fatalf("expected public.users table to be extracted")
	}
	if _, ok := analysis.Tables["orders"]; !ok {
		t.Fatalf("expected orders table to be extracted")
	}
}
