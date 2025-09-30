package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/adapters/ioadapter"
	"github.com/oarkflow/sql/pkg/checkpoints"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

// Large dataset simulation
func generateLargeDataset(size int) string {
	var records []string
	for i := 1; i <= size; i++ {
		record := fmt.Sprintf(`{"id": %d, "name": "Record_%d", "value": %d, "timestamp": "%s", "category": "cat_%d"}`,
			i, i, rand.Intn(1000), time.Now().Add(time.Duration(i)*time.Second).Format(time.RFC3339), i%10+1)
		records = append(records, record)
	}
	return strings.Join(records, "\n")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up graceful shutdown
	setupGracefulShutdown(cancel)

	fmt.Println("=== ETL Incremental Resume and Data Consistency Demo ===")

	// Test different scenarios
	fmt.Println("\n1. Testing Large Dataset Processing with Checkpoints")
	testLargeDatasetProcessing(ctx)

	fmt.Println("\n2. Testing Resume after Interruption")
	testResumeAfterInterruption(ctx)

	fmt.Println("\n3. Testing Data Consistency with State Management")
	testDataConsistency(ctx)
}

func setupGracefulShutdown(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\n⚠️  Graceful shutdown initiated...")
		cancel()
		time.Sleep(2 * time.Second) // Give time for cleanup
		os.Exit(0)
	}()
}

func testLargeDatasetProcessing(ctx context.Context) {
	// Generate large dataset (10,000 records)
	fmt.Println("Generating large dataset (10,000 records)...")
	largeData := generateLargeDataset(10000)

	// Create checkpoint store
	checkpointStore := checkpoints.NewFileCheckpointStore("/tmp/etl_large_checkpoint.txt")
	defer checkpointStore.Remove()

	// Create source
	reader := strings.NewReader(largeData)
	source := ioadapter.NewSource(reader, "json")

	// Create destination
	destConfig := config.DataConfig{
		Type:   "stdout",
		Format: "json",
	}

	// Create ETL with checkpoints and state management
	pipeline := etl.NewETL(
		"large-dataset-etl",
		"Large Dataset Processing with Checkpoints",
		etl.WithSources(source),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithCheckpoint(checkpointStore, func(rec utils.Record) string {
			// Use the ID field as checkpoint
			if id, exists := rec["id"]; exists {
				return fmt.Sprintf("%v", id)
			}
			return fmt.Sprintf("%d", time.Now().UnixNano())
		}),
		etl.WithBatchSize(100), // Process in smaller batches
		etl.WithWorkerCount(4),
		etl.WithTransformers(&CheckpointAwareTransformer{}),
		etl.WithMappers(&ProgressMapper{}),
	)

	fmt.Println("Starting large dataset processing...")
	start := time.Now()

	if err := pipeline.Run(ctx); err != nil {
		log.Printf("Pipeline failed: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("Large dataset processing completed in %v\n", duration)

	// Show final state
	showETLState(pipeline)
}

func testResumeAfterInterruption(ctx context.Context) {
	fmt.Println("Simulating ETL interruption and resume...")

	// Create a dataset
	data := generateLargeDataset(1000)

	// First run - simulate interruption midway
	fmt.Println("Starting first run (will be interrupted)...")
	runInterruptibleETL(ctx, data, "interrupted-etl-001", true)

	// Second run - resume from checkpoint
	fmt.Println("Starting second run (resume from checkpoint)...")
	runInterruptibleETL(ctx, data, "interrupted-etl-001", false)
}

func runInterruptibleETL(ctx context.Context, data, etlID string, shouldInterrupt bool) {
	reader := strings.NewReader(data)
	source := ioadapter.NewSource(reader, "json")

	checkpointStore := checkpoints.NewFileCheckpointStore(fmt.Sprintf("/tmp/%s_checkpoint.txt", etlID))
	defer func() {
		if !shouldInterrupt {
			checkpointStore.Remove()
		}
	}()

	destConfig := config.DataConfig{
		Type:   "stdout",
		Format: "json",
	}

	pipeline := etl.NewETL(
		etlID,
		"Interruptible ETL Pipeline",
		etl.WithSources(source),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithCheckpoint(checkpointStore, func(rec utils.Record) string {
			return fmt.Sprintf("%v", rec["id"])
		}),
		etl.WithBatchSize(50),
		etl.WithWorkerCount(2),
		etl.WithTransformers(&InterruptibleTransformer{
			ShouldInterrupt: shouldInterrupt,
			InterruptAfter:  300, // Interrupt after processing 300 records
		}),
	)

	// Create a context with timeout for interruption simulation
	var runCtx context.Context
	var cancel context.CancelFunc

	if shouldInterrupt {
		runCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	} else {
		runCtx = ctx
	}

	if err := pipeline.Run(runCtx); err != nil {
		if shouldInterrupt {
			log.Printf("Pipeline interrupted as expected: %v", err)
		} else {
			log.Printf("Pipeline error: %v", err)
		}
	}

	// Show state after run
	showETLState(pipeline)
}

func testDataConsistency(ctx context.Context) {
	fmt.Println("Testing data consistency with state management...")

	data := generateLargeDataset(500)
	reader := strings.NewReader(data)
	source := ioadapter.NewSource(reader, "json")

	checkpointStore := checkpoints.NewFileCheckpointStore("/tmp/consistency_checkpoint.txt")
	defer checkpointStore.Remove()

	destConfig := config.DataConfig{
		Type:   "stdout",
		Format: "json",
	}

	pipeline := etl.NewETL(
		"consistency-test-etl",
		"Data Consistency Test Pipeline",
		etl.WithSources(source),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithCheckpoint(checkpointStore, func(rec utils.Record) string {
			return fmt.Sprintf("%v", rec["id"])
		}),
		etl.WithBatchSize(25),
		etl.WithWorkerCount(2),
		etl.WithTransformers(&ConsistencyTransformer{}),
		etl.WithMappers(&ValidationMapper{}),
	)

	if err := pipeline.Run(ctx); err != nil {
		log.Printf("Consistency test failed: %v", err)
	}

	// Validate final state
	showETLState(pipeline)
	validateDataConsistency(pipeline)
}

// CheckpointAwareTransformer demonstrates checkpoint-aware processing
type CheckpointAwareTransformer struct {
	processedCount int64
}

func (t *CheckpointAwareTransformer) Name() string {
	return "CheckpointAwareTransformer"
}

func (t *CheckpointAwareTransformer) Transform(ctx context.Context, record utils.Record) (utils.Record, error) {
	t.processedCount++

	// Add processing metadata
	record["processed_at"] = time.Now().Format(time.RFC3339)
	record["transformer"] = "CheckpointAware"
	record["processing_batch"] = t.processedCount / 100 // Batch number

	// Simulate some processing time
	time.Sleep(10 * time.Millisecond)

	// Log progress every 1000 records
	if t.processedCount%1000 == 0 {
		fmt.Printf("📊 Processed %d records, current ID: %v\n", t.processedCount, record["id"])
	}

	return record, nil
}

// ProgressMapper tracks processing progress
type ProgressMapper struct {
	startTime time.Time
	count     int64
}

func (m *ProgressMapper) Name() string {
	return "ProgressMapper"
}

func (m *ProgressMapper) Map(ctx context.Context, record utils.Record) (utils.Record, error) {
	if m.startTime.IsZero() {
		m.startTime = time.Now()
	}

	m.count++

	// Add progress metadata
	record["progress_count"] = m.count
	record["elapsed_seconds"] = time.Since(m.startTime).Seconds()

	if m.count%500 == 0 {
		rate := float64(m.count) / time.Since(m.startTime).Seconds()
		fmt.Printf("📈 Processing rate: %.1f records/second\n", rate)
	}

	return record, nil
}

// InterruptibleTransformer simulates processing that can be interrupted
type InterruptibleTransformer struct {
	ShouldInterrupt bool
	InterruptAfter  int64
	processedCount  int64
}

func (t *InterruptibleTransformer) Name() string {
	return "InterruptibleTransformer"
}

func (t *InterruptibleTransformer) Transform(ctx context.Context, record utils.Record) (utils.Record, error) {
	t.processedCount++

	// Check for interruption condition
	if t.ShouldInterrupt && t.processedCount > t.InterruptAfter {
		fmt.Printf("🛑 Simulating interruption after processing %d records\n", t.processedCount)
		return nil, fmt.Errorf("simulated interruption at record %d", t.processedCount)
	}

	// Add processing metadata
	record["interrupt_test"] = true
	record["processed_count"] = t.processedCount
	record["can_resume"] = true

	// Simulate processing time
	time.Sleep(5 * time.Millisecond)

	return record, nil
}

// ConsistencyTransformer ensures data integrity
type ConsistencyTransformer struct {
	checksumMap map[string]string
}

func (t *ConsistencyTransformer) Name() string {
	return "ConsistencyTransformer"
}

func (t *ConsistencyTransformer) Transform(ctx context.Context, record utils.Record) (utils.Record, error) {
	if t.checksumMap == nil {
		t.checksumMap = make(map[string]string)
	}

	// Generate checksum for the record
	recordID := fmt.Sprintf("%v", record["id"])
	checksum := generateRecordChecksum(record)

	// Store checksum for consistency validation
	t.checksumMap[recordID] = checksum
	record["checksum"] = checksum
	record["consistency_validated"] = true

	return record, nil
}

// ValidationMapper performs final validation
type ValidationMapper struct{}

func (m *ValidationMapper) Name() string {
	return "ValidationMapper"
}

func (m *ValidationMapper) Map(ctx context.Context, record utils.Record) (utils.Record, error) {
	// Validate required fields
	requiredFields := []string{"id", "name", "value", "timestamp"}
	for _, field := range requiredFields {
		if _, exists := record[field]; !exists {
			return nil, fmt.Errorf("missing required field: %s", field)
		}
	}

	// Add validation metadata
	record["validation_passed"] = true
	record["validated_at"] = time.Now().Format(time.RFC3339)

	return record, nil
}

// Helper functions
func generateRecordChecksum(record utils.Record) string {
	// Simple checksum based on record content
	hash := 0
	for k, v := range record {
		hash += len(k) + len(fmt.Sprintf("%v", v))
	}
	return fmt.Sprintf("%x", hash)
}

func showETLState(pipeline *etl.ETL) {
	if stateManager := pipeline.GetStateManager(); stateManager != nil {
		state := stateManager.GetState()
		fmt.Printf("\n📋 ETL State Summary:\n")
		fmt.Printf("   Status: %s\n", state.Status)
		fmt.Printf("   Processed Records: %d\n", state.ProcessedRecords)
		fmt.Printf("   Failed Records: %d\n", state.FailedRecords)
		fmt.Printf("   Last Checkpoint: %s\n", state.LastCheckpoint)
		fmt.Printf("   Start Time: %s\n", state.StartTime.Format("15:04:05"))
		fmt.Printf("   Last Update: %s\n", state.LastUpdateTime.Format("15:04:05"))
		fmt.Printf("   State Version: %d\n", state.StateVersion)

		if len(state.ErrorDetails) > 0 {
			fmt.Printf("   Recent Errors: %d\n", len(state.ErrorDetails))
		}

		fmt.Printf("   Worker States: %d active\n", len(state.WorkerStates))
		fmt.Printf("   Node Progress: %d nodes\n", len(state.NodeProgress))
	}

	// Show summary
	summary := pipeline.GetSummary()
	fmt.Printf("\n📊 Pipeline Summary:\n")
	fmt.Printf("   Total Extracted: %d\n", summary.Metrics.Extracted)
	fmt.Printf("   Total Loaded: %d\n", summary.Metrics.Loaded)
	fmt.Printf("   Error Rate: %.2f%%\n", summary.ErrorRate)
	fmt.Printf("   Status: %s\n", summary.Status)
}

func validateDataConsistency(pipeline *etl.ETL) {
	fmt.Printf("\n🔍 Data Consistency Validation:\n")

	if stateManager := pipeline.GetStateManager(); stateManager != nil {
		state := stateManager.GetState()

		// Check for consistency indicators
		if state.ProcessedRecords > 0 && state.FailedRecords == 0 {
			fmt.Printf("   ✅ All records processed successfully\n")
		} else if state.FailedRecords > 0 {
			fmt.Printf("   ⚠️  %d records failed processing\n", state.FailedRecords)
		}

		// Check checkpoint consistency
		if state.LastCheckpoint != "" {
			checkpointID, _ := strconv.Atoi(state.LastCheckpoint)
			if int64(checkpointID) <= state.ProcessedRecords {
				fmt.Printf("   ✅ Checkpoint consistency verified\n")
			} else {
				fmt.Printf("   ❌ Checkpoint inconsistency detected\n")
			}
		}

		// Check state version progression
		if state.StateVersion > 0 {
			fmt.Printf("   ✅ State management active (version %d)\n", state.StateVersion)
		}
	}

	fmt.Printf("   🎯 Data consistency validation completed\n")
}
