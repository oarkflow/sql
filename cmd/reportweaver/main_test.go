package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestBackendReportAndQueryFlow(t *testing.T) {
	store := newStateStore(filepath.Join(t.TempDir(), "store.json"))
	app := createApp(store)

	reportPayload := map[string]any{
		"id":   "report-1",
		"name": "Sales Dashboard",
		"pages": []map[string]any{{
			"id":      "page-1",
			"name":    "Page 1",
			"widgets": []map[string]any{},
			"layout":  []map[string]any{},
		}},
	}
	body, _ := json.Marshal(reportPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/reports", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("create report request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/reports", nil)
	resp, err = app.Test(req)
	if err != nil {
		t.Fatalf("list reports request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	queryPayload := map[string]any{
		"sql": "SELECT 1 AS value FROM dual",
	}
	body, _ = json.Marshal(queryPayload)
	req = httptest.NewRequest(http.MethodPost, "/api/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err = app.Test(req)
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	credentialPayload := map[string]any{
		"key":  "api-test-key",
		"type": "api_key",
		"data": map[string]any{"key": "abc123"},
	}
	body, _ = json.Marshal(credentialPayload)
	req = httptest.NewRequest(http.MethodPost, "/api/credentials", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err = app.Test(req)
	if err != nil {
		t.Fatalf("create credential request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/credentials", nil)
	resp, err = app.Test(req)
	if err != nil {
		t.Fatalf("list credentials request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
