package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/sql/platform"
)

func main() {
	log.Println("Data Ingestion Platform Example")

	// Initialize core components
	messageBus := platform.NewInMemoryMessageBus()
	schemaRegistry := platform.NewInMemorySchemaRegistry()
	metricsCollector := platform.NewMetricsCollector()
	healthChecker := platform.NewHealthChecker()

	// Register a health check
	healthChecker.RegisterCheck("message_bus", func() error {
		return messageBus.Publish("health_check", "ping")
	})

	// Create and register a sample schema
	userSchema := platform.CanonicalModel{
		SchemaName: "user",
		Version:    "1.0",
		Fields: map[string]platform.FieldSchema{
			"id":         {Type: "integer", Required: true},
			"name":       {Type: "string", Required: true},
			"email":      {Type: "string", Required: true, Validation: "email"},
			"created_at": {Type: "date", Required: false},
		},
	}

	if err := schemaRegistry.Register(userSchema); err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}

	// Create a processing pipeline
	pipeline := platform.ProcessingPipeline{
		Name: "user_pipeline",
		Stages: []platform.PipelineStage{
			{Type: "normalize", Config: map[string]any{"schema": "user"}},
			{Type: "validate", Config: map[string]any{"schema": "user"}},
			{Type: "store", Config: map[string]any{"schema": "user", "sink": "database"}},
		},
	}

	executor := platform.NewProcessingPipelineExecutor(pipeline, schemaRegistry)

	// Register parsers and other components
	executor.RegisterParser("json", platform.NewJSONParser())
	executor.RegisterNormalizer("default", platform.NewDefaultNormalizer(schemaRegistry, nil))
	executor.RegisterValidator("default", platform.NewDefaultValidator(schemaRegistry))
	executor.RegisterEnricher("default", platform.NewDefaultEnricher(nil))

	// Test parsing some sample data
	sampleJSON := `{"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": "2023-01-01T00:00:00Z"}`

	ingestMsg := platform.IngestMessage{
		ID:          "test-123",
		RawID:       "raw-123",
		Payload:     []byte(sampleJSON),
		Source:      "test",
		Origin:      "example",
		ContentType: "application/json",
	}

	log.Printf("Testing pipeline with sample data: %s", sampleJSON)

	// Execute the pipeline
	if err := executor.Execute(context.Background(), ingestMsg); err != nil {
		log.Printf("Pipeline execution failed: %v", err)
	} else {
		log.Println("Pipeline executed successfully!")
	}

	// Show registered schemas
	schemas := schemaRegistry.List()
	log.Printf("Registered schemas: %v", schemas)

	// Show metrics
	metrics := metricsCollector.GetMetrics()
	log.Printf("Current metrics: %+v", metrics)

	// Show health status
	health := healthChecker.Check()
	log.Printf("Health checks: %+v", health)

	fmt.Println("Platform example completed successfully!")
}
