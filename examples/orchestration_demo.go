package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/runner"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/events"
	"github.com/oarkflow/sql/pkg/orchestrator"
	"github.com/oarkflow/sql/pkg/workflow"
)

func createJobConfig(name, sourceFile, destFile string, transformerConfig ...string) *config.Config {
	// Determine absolute paths for demo robustness
	absSource, _ := filepath.Abs(sourceFile)
	absDest, _ := filepath.Abs(destFile)

	transformerYaml := ""
	if len(transformerConfig) > 0 {
		transformerYaml = transformerConfig[0]
	}

	yamlConfig := fmt.Sprintf(`
sources:
  - type: csv
    file: "%s"
destinations:
  - type: json
    file: "%s"
    format: json
tables:
  - migrate: true
    enable_batch: true
%s
`, absSource, absDest, transformerYaml)

	fmt.Printf("--- Debug Config for %s ---\n%s\n----------------------------\n", name, yamlConfig)

	cfg, err := config.LoadYamlFromString(yamlConfig)
	if err != nil {
		log.Fatalf("Failed to parse config for %s: %v", name, err)
	}
	return cfg
}

func main() {
	fmt.Println("=== Orchestra: Event-Driven DAG ETL Demo ===")

	// 1. Setup Infrastructure
	eventBus := events.NewEventBus()
	etlManager := etl.NewManager()
	jobRunner := runner.NewDefaultRunner(2, etlManager)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := jobRunner.Start(ctx); err != nil {
		log.Fatalf("Failed to start runner: %v", err)
	}

	orch := orchestrator.New(jobRunner, eventBus)

	// 2. Define a Workflow (DAG)
	dag := workflow.NewDAG()

	// Job 1: Extract Users
	cfgUsers := createJobConfig("extract_users", "../users.csv", "users_output.json")
	dag.AddNode("extract_users", cfgUsers)

	// Job 2: Extract Sales
	cfgSales := createJobConfig("extract_sales", "../sales.csv", "sales_output.json")
	dag.AddNode("extract_sales", cfgSales)

	// Job 3: Enrich Data (Mapping & Transformation)
	// We use Mapping to rename fields to business domain terms.
	// Mapping format is Destination: Source
	// Note: Complex transformers (calculations) can be added via 'transformers' key but are omitted for simplicity here.
	enrichTransformers := `    mapping:
      ClientName: name
      ClientID: id
      SignupDate: created_at
`
	cfgEnrich := createJobConfig("enrich_data", "../users.csv", "enriched_final.json", enrichTransformers)
	dag.AddNode("enrich_data", cfgEnrich, "extract_users", "extract_sales")

	workflowID := "nightly_etl"
	orch.RegisterWorkflow(workflowID, dag)
	fmt.Printf("Registered Workflow: %s\n", workflowID)

	// 3. Setup Trigger
	orch.RegisterTrigger(events.EventFileArrival, workflowID)

	// 4. Simulate Event
	fmt.Println("\n--- ⚡ Simulating Event: File Uploaded to S3 ---")
	uploadEvent := events.Event{
		Type:      events.EventFileArrival,
		Source:    "s3-bucket-watcher",
		Timestamp: time.Now(),
		Payload: map[string]any{
			"file": "s3://bucket/data/sales_2026.csv",
		},
	}

	eventBus.Publish(context.Background(), uploadEvent)

	fmt.Println("Waiting for workflow to complete...")
	time.Sleep(10 * time.Second)

	fmt.Println("\n--- Demo Complete ---")
}
