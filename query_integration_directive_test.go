package sql

import "testing"

func TestParseRuntimeIntegrationDirective_Multiline(t *testing.T) {
	query := `
-- integration: processgate
SELECT * FROM users
`
	integration, cleaned := parseRuntimeIntegrationDirective(query)
	if integration != "processgate" {
		t.Fatalf("expected integration processgate, got %q", integration)
	}
	if cleaned != "SELECT * FROM users" {
		t.Fatalf("expected cleaned query SELECT * FROM users, got %q", cleaned)
	}
}

func TestParseRuntimeIntegrationDirective_InlineEscapedNewline(t *testing.T) {
	query := `-- integration: processgate;\nSELECT * FROM users`
	integration, cleaned := parseRuntimeIntegrationDirective(query)
	if integration != "processgate" {
		t.Fatalf("expected integration processgate, got %q", integration)
	}
	if cleaned != "SELECT * FROM users" {
		t.Fatalf("expected cleaned query SELECT * FROM users, got %q", cleaned)
	}
}
