package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

// MockLoader implements contracts.Loader and simulates errors
type MockLoader struct {
	StoreBatchCalls  int
	StoreSingleCalls int
}

func (m *MockLoader) Setup(ctx context.Context) error { return nil }

func (m *MockLoader) StoreBatch(ctx context.Context, batch []utils.Record) error {
	m.StoreBatchCalls++
	return fmt.Errorf("batch load error") // Fail all batches
}

func (m *MockLoader) StoreSingle(ctx context.Context, rec utils.Record) error {
	m.StoreSingleCalls++
	if id, ok := rec["id"].(int); ok && id == 3 {
		return fmt.Errorf("single load error for id 3")
	}
	return nil
}

func (m *MockLoader) Close() error { return nil }

// MockTransformer simulates errors
type MockTransformer struct{}

func (t *MockTransformer) Name() string { return "mock_transformer" }
func (t *MockTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	if id, ok := rec["id"].(int); ok && id == 2 {
		return nil, fmt.Errorf("transform error for id 2")
	}
	return rec, nil
}

// MockSource implements contracts.Source
type MockSource struct{}

func (m *MockSource) Setup(ctx context.Context) error { return nil }
func (m *MockSource) Name() string { return "mock_source" }
func (m *MockSource) Close() error { return nil }

func (m *MockSource) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 5)
	go func() {
		defer close(out)
		out <- utils.Record{"id": 1, "data": "ok"}         // Should succeed
		out <- utils.Record{"id": 2, "data": "fail_tx"}    // Should fail transform
		out <- utils.Record{"id": 3, "data": "fail_load"}  // Should fail load
	}()
	return out, nil
}

func main() {
	mockLoader := &MockLoader{}
	mockSource := &MockSource{}
	mockTransformer := &MockTransformer{}

	// Create ETL with DryRun=false to test Load errors
	e := etl.NewETL("test_dlq", "Test DLQ",
		etl.WithSources(mockSource),
		etl.WithLoader(mockLoader),
		etl.WithTransformers(mockTransformer),
		etl.WithDryRun(false),
        etl.WithNormalizeSchema(nil),
	)

	// Set disable batch to force StoreSingle? Or keep batch enabled?
    // Default might be batch enabled if not specified in TableConfig, but NewETL sets defaults.
    // e.Wait, default config has EnableBatch=false usually unless specified?
    // Let's force EnableBatch=false to test StoreSingle which gives us per-record errors easily.
    // Actually tableCfg in default build comes from defaults or empty?
    // defaulting happens in Load Node inside Process?
    // "cfg config.TableMapping" is passed in Process. In buildDefaultPipeline, it passes what?

    // In etl.go: e.SetTableConfig(tableCfg)
    e.SetTableConfig(config.TableMapping{EnableBatch: false})

	log.Println("Starting ETL DLQ Test...")
	if err := e.Run(context.Background()); err != nil {
		log.Printf("ETL Run completed with status: %v", err) // Might return generic error if many errors?
	}

	metrics := e.GetMetrics()
	log.Printf("Metrics: %+v", metrics)

    // Expected flows:
    // ID 1: Extract OK -> Transform OK -> Load OK
    // ID 2: Extract OK -> Transform FAIL -> DLQ (Stage: Transform)
    // ID 3: Extract OK -> Transform OK -> Load FAIL -> DLQ (Stage: Load)

    // Metrics expectations:
    // Extracted: 3
    // Transformed: 2 (1 failed) -> Actually if transform fails, it increment Errors but doesn't return record.
    // Loaded: 1 (ID 1)
    // Errors: 2 (1 transform, 1 load)

	if metrics.Extracted != 3 {
		log.Fatalf("Expected 3 extracted, got %d", metrics.Extracted)
	}
	if metrics.Loaded != 1 {
		log.Fatalf("Expected 1 loaded, got %d", metrics.Loaded)
	}
	if metrics.Errors != 2 {
		log.Fatalf("Expected 2 errors, got %d", metrics.Errors)
	}

    // Verify DLQ contents
    dlqRecords := e.GetDeadLetterRecords()
    log.Printf("DLQ contains %d records", len(dlqRecords))

    if len(dlqRecords) != 2 {
        log.Fatalf("Expected 2 DLQ records, got %d", len(dlqRecords))
    }

    // Verify stages
    transformErrorFound := false
    loadErrorFound := false
    etlIDFound := true

    for _, r := range dlqRecords {
        log.Printf("DLQ Record: Stage=%s, Reason=%s, Meta=%v", r.NodeName, r.FailureReason, r.Metadata)

        if r.NodeName == "transform" && r.FailureReason == "transform error for id 2" {
            transformErrorFound = true
        }
        if r.NodeName == "load" && r.FailureReason == "single load error for id 3" {
            loadErrorFound = true
        }

        if r.Metadata == nil || r.Metadata["etl_id"] != "test_dlq" {
            etlIDFound = false
            log.Printf("Missing or incorrect etl_id in metadata: %v", r.Metadata)
        }
    }

    if !transformErrorFound {
        log.Fatal("DLQ missing transform error record")
    }
    if !loadErrorFound {
        log.Fatal("DLQ missing load error record")
    }
    if !etlIDFound {
        log.Fatal("DLQ records missing etl_id metadata")
    }

	log.Println("DLQ verification SUCCESS!")
}
