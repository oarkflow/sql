package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

// MockLoader implements contracts.Loader and tracks calls
type MockLoader struct {
	StoreBatchCalls  int
	StoreSingleCalls int
}

func (m *MockLoader) Setup(ctx context.Context) error { return nil }

func (m *MockLoader) StoreBatch(ctx context.Context, metrics []utils.Record) error {
	m.StoreBatchCalls++
	log.Println("MockLoader.StoreBatch called")
	return nil
}

func (m *MockLoader) StoreSingle(ctx context.Context, metric utils.Record) error {
	m.StoreSingleCalls++
	log.Println("MockLoader.StoreSingle called")
	return nil
}

func (m *MockLoader) Close() error { return nil }

// MockSource implements contracts.Source
type MockSource struct{}

func (m *MockSource) Setup(ctx context.Context) error { return nil }

func (m *MockSource) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 5)
	go func() {
		defer close(out)
		out <- utils.Record{"id": 1, "data": "test1"}
		out <- utils.Record{"id": 2, "data": "test2"}
		out <- utils.Record{"id": 3, "data": "test3"}
	}()
	return out, nil
}
func (m *MockSource) Close() error { return nil }

// MockTransformer simulates errors
type MockTransformer struct{}

func (t *MockTransformer) Name() string { return "mock_transformer" }
func (t *MockTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	if rec["id"] == 2 {
		return nil, fmt.Errorf("simulated transformation error")
	}
	return rec, nil
}

func main() {
	mockLoader := &MockLoader{}
	mockSource := &MockSource{}
	mockTransformer := &MockTransformer{}

	e := etl.NewETL("test_dry_run", "Test Dry Run",
		etl.WithSources(mockSource),
		etl.WithLoader(mockLoader),
		etl.WithTransformers(mockTransformer),
		etl.WithDryRun(true),
        etl.WithNormalizeSchema(nil), // No normalization needed
	)

	// Since we are building pipeline manually or letting default build it, e.sources/loaders are set via Options.

	log.Println("Starting ETL with DryRun=true...")
	if err := e.Run(context.Background()); err != nil {
		log.Fatalf("ETL Run failed: %v", err)
	}

	metrics := e.GetMetrics()
	log.Printf("Metrics: %+v", metrics)

	if metrics.Extracted != 3 {
		log.Fatalf("Expected 3 extracted, got %d", metrics.Extracted)
	}
	// 1 record should fail transformation
	if metrics.Transformed != 2 {
		log.Fatalf("Expected 2 transformed (1 failed), got %d", metrics.Transformed)
	}
	if metrics.Errors != 1 {
		log.Fatalf("Expected 1 error, got %d", metrics.Errors)
	}
	if metrics.Loaded != 2 {
		log.Fatalf("Expected 2 loaded (simulated), got %d", metrics.Loaded)
	}

	if mockLoader.StoreBatchCalls > 0 {
		log.Fatalf("DryRun failed! StoreBatch called %d times", mockLoader.StoreBatchCalls)
	}
	if mockLoader.StoreSingleCalls > 0 {
		log.Fatalf("DryRun failed! StoreSingle called %d times", mockLoader.StoreSingleCalls)
	}

	log.Println("DryRun verification SUCCESS!")
}
