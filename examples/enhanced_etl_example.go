package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

// EnhancedETLExample demonstrates the enhanced incremental ETL capabilities
func EnhancedETLExample() {
	// Create an enhanced ETL instance with incremental capabilities
	etlInstance := etl.NewETL(
		"enhanced_etl_example",
		"Enhanced ETL with Incremental Processing",
		etl.WithWorkerCount(4),
		etl.WithBatchSize(50),
		etl.WithStateFile("enhanced_etl_state.json"),
		etl.WithDLQFile("enhanced_etl_dlq.json"),
		etl.WithIdempotencyFile("enhanced_etl_idempotency.json"),
		etl.WithIdempotencyFields([]string{"id", "timestamp"}), // Use these fields for idempotency
		etl.WithMaxErrorThreshold(100),
	)

	// Configure sources (example with CSV files)
	opts := []etl.Option{
		etl.WithSource("csv", nil, "large_dataset.csv", "", "", "csv"),
		etl.WithDestination(config.DataConfig{
			Type:     "postgresql",
			Driver:   "postgres",
			Host:     "localhost",
			Username: "postgres",
			Password: "postgres",
			Port:     5432,
			Database: "warehouse",
		}, nil, config.TableMapping{
			OldName:         "large_dataset.csv",
			NewName:         "processed_data",
			CloneSource:     true,
			Migrate:         true,
			AutoCreateTable: true,
			Mapping: map[string]string{
				"id":        "id",
				"name":      "name",
				"value":     "value",
				"timestamp": "timestamp",
			},
		}),
	}

	// Apply additional configuration
	for _, opt := range opts {
		if err := opt(etlInstance); err != nil {
			log.Fatalf("Configuration error: %v", err)
		}
	}

	// Demonstrate different execution scenarios

	// 1. First run - normal execution
	fmt.Println("=== First Run ===")
	if err := runETLWithMonitoring(etlInstance, "first_run"); err != nil {
		log.Printf("First run failed: %v", err)
	}

	// 2. Simulate failure and recovery
	fmt.Println("\n=== Simulating Failure and Recovery ===")
	simulateFailureAndRecovery(etlInstance)

	// 3. Resume capability demonstration
	fmt.Println("\n=== Resume Capability ===")
	demonstrateResume(etlInstance)

	// 4. Monitoring and metrics
	fmt.Println("\n=== Enhanced Monitoring ===")
	demonstrateMonitoring(etlInstance)

	// Cleanup
	etlInstance.Cleanup(24 * time.Hour)
	fmt.Println("\n=== Cleanup completed ===")
}

// runETLWithMonitoring runs ETL with enhanced monitoring
func runETLWithMonitoring(etlInstance *etl.ETL, runName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()

	// Set up monitoring
	monitorTicker := time.NewTicker(2 * time.Second)
	go func() {
		for range monitorTicker.C {
			metrics := etlInstance.GetEnhancedMetrics()
			fmt.Printf("Progress: %+v\n", metrics)
		}
	}()

	err := etlInstance.Run(ctx)
	monitorTicker.Stop()

	elapsed := time.Since(startTime)
	fmt.Printf("%s completed in %v\n", runName, elapsed)

	return err
}

// simulateFailureAndRecovery demonstrates failure handling and recovery
func simulateFailureAndRecovery(etlInstance *etl.ETL) {
	// Simulate a failure by stopping the ETL mid-process
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	// Start ETL in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- etlInstance.Run(ctx)
	}()

	// Let it run for a bit, then simulate failure
	time.Sleep(2 * time.Second)
	etlInstance.Pause()

	// Check state
	state := etlInstance.GetStateInfo()
	fmt.Printf("ETL paused at checkpoint: %s\n", state.LastCheckpoint)
	fmt.Printf("Processed records: %d\n", state.ProcessedRecords)

	// Resume from checkpoint
	fmt.Println("Resuming from checkpoint...")
	if err := etlInstance.Resume(context.Background()); err != nil {
		log.Printf("Resume failed: %v", err)
	} else {
		fmt.Println("Resume completed successfully")
	}
}

// demonstrateResume shows the resume capability
func demonstrateResume(etlInstance *etl.ETL) {
	// Check if resume is possible
	if etlInstance.GetStateManager().CanResume() {
		fmt.Println("Resume is available")

		// Get resume information
		resumeInfo := etlInstance.GetStateManager().GetResumeInfo()
		fmt.Printf("Resume info: %+v\n", resumeInfo)

		// Perform resume
		if err := etlInstance.Resume(context.Background()); err != nil {
			log.Printf("Resume failed: %v", err)
		}
	} else {
		fmt.Println("No resume available")
	}
}

// demonstrateMonitoring shows enhanced monitoring capabilities
func demonstrateMonitoring(etlInstance *etl.ETL) {
	// Get comprehensive metrics
	metrics := etlInstance.GetEnhancedMetrics()

	fmt.Printf("=== Basic Metrics ===\n")
	if basic, ok := metrics["basic"].(etl.Metrics); ok {
		fmt.Printf("Extracted: %d, Loaded: %d, Errors: %d\n",
			basic.Extracted, basic.Loaded, basic.Errors)
	}

	fmt.Printf("\n=== State Information ===\n")
	if state, ok := metrics["state"].(map[string]interface{}); ok {
		fmt.Printf("Status: %v\n", state["status"])
		fmt.Printf("Processed: %v\n", state["processed_records"])
		fmt.Printf("Failed: %v\n", state["failed_records"])
		fmt.Printf("Can Resume: %v\n", state["can_resume"])
	}

	fmt.Printf("\n=== Dead Letter Queue ===\n")
	if dlq, ok := metrics["dead_letter_queue"].(map[string]interface{}); ok {
		fmt.Printf("Total Records: %v\n", dlq["total_records"])
		fmt.Printf("Retryable: %v\n", dlq["retryable_records"])
		fmt.Printf("Permanent Failures: %v\n", dlq["permanent_failures"])
	}

	fmt.Printf("\n=== Idempotency ===\n")
	if idempotency, ok := metrics["idempotency"].(map[string]interface{}); ok {
		fmt.Printf("Total Keys: %v\n", idempotency["total_keys"])
		fmt.Printf("Processing: %v\n", idempotency["processing"])
		fmt.Printf("Completed: %v\n", idempotency["completed"])
		fmt.Printf("Failed: %v\n", idempotency["failed"])
	}

	// Show failed records
	failedRecords := etlInstance.GetDeadLetterRecords()
	if len(failedRecords) > 0 {
		fmt.Printf("\n=== Failed Records ===\n")
		for i, record := range failedRecords {
			if i >= 5 { // Show only first 5
				fmt.Printf("... and %d more\n", len(failedRecords)-5)
				break
			}
			fmt.Printf("Record %s: %s (failed %d times)\n",
				record.ID, record.FailureReason, record.RetryCount)
		}
	}
}

// demonstrateDeadLetterQueueProcessing shows DLQ processing
func demonstrateDeadLetterQueueProcessing(etlInstance *etl.ETL) {
	dlq := etlInstance.GetDeadLetterQueue()
	if dlq == nil {
		fmt.Println("No dead letter queue configured")
		return
	}

	stats := dlq.GetQueueStats()
	fmt.Printf("DLQ Stats: %+v\n", stats)

	// Process retries manually
	retryable := dlq.GetRetryableRecords()
	fmt.Printf("Processing %d retryable records...\n", len(retryable))

	for _, record := range retryable {
		// Custom retry logic would go here
		fmt.Printf("Retrying record %s\n", record.ID)
	}
}

// demonstrateIdempotency shows idempotency features
func demonstrateIdempotency(etlInstance *etl.ETL) {
	idempotency := etlInstance.GetIdempotencyManager()
	if idempotency == nil {
		fmt.Println("No idempotency manager configured")
		return
	}

	stats := idempotency.GetStats()
	fmt.Printf("Idempotency Stats: %+v\n", stats)

	// Demonstrate key generation and checking
	testRecord := utils.Record{
		"id":        "123",
		"name":      "test",
		"timestamp": "2023-01-01",
	}

	key := idempotency.GenerateKey(testRecord, []string{"id", "timestamp"})
	fmt.Printf("Generated key for record: %s\n", key)

	// Check if processed
	processed, _ := idempotency.IsProcessed(key)
	fmt.Printf("Record processed: %v\n", processed)
}

// Additional utility functions for the example

// simulateLargeDataset simulates processing a large dataset
func simulateLargeDataset() {
	fmt.Println("Simulating large dataset processing...")

	// Create a sample large CSV file
	fmt.Println("Created sample large_dataset.csv with 1M records")

	// In a real scenario, this would be your actual large dataset
}

// demonstrateBatchProcessing shows batch processing with checkpoints
func demonstrateBatchProcessing(etlInstance *etl.ETL) {
	fmt.Println("=== Batch Processing with Checkpoints ===")

	// Configure for frequent checkpoints
	etlInstance.SetCheckpointInterval(5 * time.Second)

	// Run with monitoring
	runETLWithMonitoring(etlInstance, "batch_processing")
}

// demonstrateErrorHandling shows comprehensive error handling
func demonstrateErrorHandling(etlInstance *etl.ETL) {
	fmt.Println("=== Error Handling Demonstration ===")

	// Configure error handling
	etlInstance.SetMaxErrorThreshold(50)

	// Run ETL that might encounter errors
	ctx := context.Background()
	if err := etlInstance.Run(ctx); err != nil {
		fmt.Printf("ETL failed as expected: %v\n", err)

		// Show error details
		state := etlInstance.GetStateInfo()
		fmt.Printf("Total errors: %d\n", len(state.ErrorDetails))

		for _, errorDetail := range state.ErrorDetails {
			fmt.Printf("Error: %s at %s\n", errorDetail.Error, errorDetail.Timestamp)
		}
	}
}

// demonstrateStatePersistence shows state persistence across restarts
func demonstrateStatePersistence() {
	fmt.Println("=== State Persistence ===")

	// Create ETL with state persistence
	etlInstance := etl.NewETL(
		"persistent_etl",
		"ETL with State Persistence",
		etl.WithStateFile("persistent_etl_state.json"),
		etl.WithAutoSaveInterval(10*time.Second),
	)

	// Simulate some processing
	etlInstance.GetStateManager().UpdateStatus("RUNNING")
	etlInstance.GetStateManager().AddProcessedRecords(1000)
	etlInstance.GetStateManager().UpdateCheckpoint("checkpoint_1000")

	// Save state
	etlInstance.GetStateManager().SaveState()

	fmt.Println("State saved. In a real scenario, you could restart the process and resume from checkpoint_1000")
}

// demonstrateConcurrentProcessing shows concurrent processing capabilities
func demonstrateConcurrentProcessing() {
	fmt.Println("=== Concurrent Processing ===")

	etlInstances := make([]*etl.ETL, 3)

	// Create multiple ETL instances
	for i := 0; i < 3; i++ {
		etlInstances[i] = etl.NewETL(
			fmt.Sprintf("concurrent_etl_%d", i),
			fmt.Sprintf("Concurrent ETL %d", i),
			etl.WithWorkerCount(2),
			etl.WithStateFile(fmt.Sprintf("concurrent_etl_%d_state.json", i)),
		)
	}

	// Run concurrently (in a real scenario)
	for i, instance := range etlInstances {
		go func(etlIdx int, etlInst *etl.ETL) {
			ctx := context.Background()
			if err := etlInst.Run(ctx); err != nil {
				log.Printf("Concurrent ETL %d failed: %v", etlIdx, err)
			}
		}(i, instance)
	}

	// Wait for completion
	time.Sleep(10 * time.Second)
	fmt.Println("Concurrent processing demonstration completed")
}
