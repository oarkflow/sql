package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/sql/platform"
)

func main() {
	log.Println("Starting Data Ingestion Platform...")

	// Initialize components
	messageBus := platform.NewInMemoryMessageBus()
	schemaRegistry := platform.NewInMemorySchemaRegistry()
	metricsCollector := platform.NewMetricsCollector()
	healthChecker := platform.NewHealthChecker()

	// Register health checks
	healthChecker.RegisterCheck("message_bus", func() error {
		return messageBus.Publish("health_check", "ping")
	})

	// Create sample schema
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

	// Create pipeline
	pipeline := platform.ProcessingPipeline{
		Name: "user_pipeline",
		Stages: []platform.PipelineStage{
			{Type: "normalize", Config: map[string]any{"schema": "user"}},
			{Type: "validate", Config: map[string]any{"schema": "user"}},
			{Type: "store", Config: map[string]any{"schema": "user", "sink": "database"}},
		},
	}

	executor := platform.NewProcessingPipelineExecutor(pipeline, schemaRegistry)

	// Register components
	executor.RegisterParser("json", platform.NewJSONParser())
	executor.RegisterNormalizer("default", platform.NewDefaultNormalizer(schemaRegistry, nil))
	executor.RegisterValidator("default", platform.NewDefaultValidator(schemaRegistry))
	executor.RegisterEnricher("default", platform.NewDefaultEnricher(nil))

	// Create HTTP gateway
	storage := &InMemoryStorage{} // Simple in-memory storage
	httpGateway := platform.NewHTTPGateway("8080", messageBus, storage)

	// Subscribe to ingest.raw topic
	if err := messageBus.Subscribe("ingest.raw", func(msg any) {
		if ingestMsg, ok := msg.(platform.IngestMessage); ok {
			log.Printf("Received ingest message: %s", ingestMsg.ID)
			if err := executor.Execute(context.Background(), ingestMsg); err != nil {
				log.Printf("Pipeline execution failed: %v", err)
			}
		}
	}); err != nil {
		log.Fatalf("Failed to subscribe to message bus: %v", err)
	}

	// Start monitoring server
	monitoringServer := platform.NewMonitoringServer(metricsCollector, healthChecker, "9090")
	go func() {
		if err := monitoringServer.Start(); err != nil {
			log.Printf("Monitoring server failed: %v", err)
		}
	}()

	// Start admin API
	adminAPI := platform.NewAdminAPI("8081", schemaRegistry, messageBus, metricsCollector, healthChecker)
	adminAPI.RegisterPipeline("user_pipeline", executor)

	go func() {
		if err := adminAPI.Start(); err != nil {
			log.Printf("Admin API failed: %v", err)
		}
	}()

	// Start HTTP gateway
	log.Println("Platform started successfully!")
	log.Println("HTTP Gateway: http://localhost:8080")
	log.Println("Admin API: http://localhost:8081")
	log.Println("Monitoring: http://localhost:9090")

	if err := httpGateway.Start(); err != nil {
		log.Fatalf("HTTP Gateway failed: %v", err)
	}
}

// Simple in-memory storage implementation
type InMemoryStorage struct {
	data map[string][]byte
}

func (s *InMemoryStorage) Store(id string, data []byte) error {
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	s.data[id] = data
	return nil
}

func (s *InMemoryStorage) Retrieve(id string) ([]byte, error) {
	data, exists := s.data[id]
	if !exists {
		return nil, fmt.Errorf("data not found: %s", id)
	}
	return data, nil
}

func (s *InMemoryStorage) Delete(id string) error {
	delete(s.data, id)
	return nil
}
