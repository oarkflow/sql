package transformers

import (
	"context"
	"testing"

	"github.com/oarkflow/sql/pkg/utils"
)

func TestFieldFormatterTransformer_DateTransform(t *testing.T) {
	transformer, err := NewFieldFormatterTransformer(map[string]string{
		"event_date": `transform(msg_date, "YYYY-MM-DD", "DDMMYYYY")`,
	})
	if err != nil {
		t.Fatalf("unexpected error creating transformer: %v", err)
	}
	rec := utils.Record{"msg_date": "01052024"}
	out, err := transformer.Transform(context.Background(), rec)
	if err != nil {
		t.Fatalf("transform returned error: %v", err)
	}
	if out["event_date"] != "2024-05-01" {
		t.Fatalf("expected event_date to be 2024-05-01, got %v", out["event_date"])
	}
}

func TestFieldFormatterTransformer_ChainedOperations(t *testing.T) {
	transformer, err := NewFieldFormatterTransformer(map[string]string{
		"user_code": `copy(user_id)|prefix("usr-")|uppercase()`,
	})
	if err != nil {
		t.Fatalf("unexpected error creating transformer: %v", err)
	}
	rec := utils.Record{"user_id": "abc123"}
	out, err := transformer.Transform(context.Background(), rec)
	if err != nil {
		t.Fatalf("transform returned error: %v", err)
	}
	if out["user_code"] != "USR-ABC123" {
		t.Fatalf("expected user_code to be USR-ABC123, got %v", out["user_code"])
	}
}

func TestFieldFormatterTransformer_DefaultValue(t *testing.T) {
	transformer, err := NewFieldFormatterTransformer(map[string]string{
		"status_label": `copy(status)|default("pending")|uppercase()`,
	})
	if err != nil {
		t.Fatalf("unexpected error creating transformer: %v", err)
	}
	rec := utils.Record{}
	out, err := transformer.Transform(context.Background(), rec)
	if err != nil {
		t.Fatalf("transform returned error: %v", err)
	}
	if out["status_label"] != "PENDING" {
		t.Fatalf("expected status_label to be PENDING, got %v", out["status_label"])
	}
}
