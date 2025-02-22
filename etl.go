package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/contracts"
	"github.com/oarkflow/sql/etl/loader"
	"github.com/oarkflow/sql/etl/source"
	"github.com/oarkflow/sql/utils"
)

// -----------------------------
// Sample Mapper, Transformer, Validator
// -----------------------------

// LowercaseMapper converts all keys in the record to lowercase.
type LowercaseMapper struct{}

func (m *LowercaseMapper) Map(rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}

// AgeTransformer converts the "age" field from a string to an integer.
type AgeTransformer struct{}

func (t *AgeTransformer) Transform(rec utils.Record) (utils.Record, error) {
	if ageStr, ok := rec["age"].(string); ok {
		age, err := strconv.Atoi(ageStr)
		if err != nil {
			return rec, fmt.Errorf("invalid age value: %v", err)
		}
		rec["age"] = age + 100
	}
	return rec, nil
}

// RequiredFieldValidator checks that a specified field exists and is not empty.
type RequiredFieldValidator struct {
	Field string
}

func (v *RequiredFieldValidator) Validate(rec utils.Record) error {
	if val, ok := rec[v.Field]; !ok || fmt.Sprintf("%v", val) == "" {
		return fmt.Errorf("required field '%s' is missing or empty", v.Field)
	}
	return nil
}

// -----------------------------
// File-Based Checkpoint Store
// -----------------------------

// FileCheckpointStore saves and retrieves a checkpoint from a file.
type FileCheckpointStore struct {
	fileName string
	mu       sync.Mutex
}

func (cs *FileCheckpointStore) SaveCheckpoint(cp string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return os.WriteFile(cs.fileName, []byte(cp), 0644)
}

func (cs *FileCheckpointStore) GetCheckpoint() (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	data, err := os.ReadFile(cs.fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

// -----------------------------
// Main: Configure and Run ETL Pipeline
// -----------------------------

func main() {
	// Create a CSV source to read from "input.csv".
	csvSource := source.NewFileSource("input.csv")

	// Create a JSON loader to write to "output.json".
	jsonLoader := loader.NewFileLoader("output.csv")

	// Instantiate sample mapper, transformer, and validator.
	lowercaseMapper := &LowercaseMapper{}
	ageTransformer := &AgeTransformer{}
	requiredValidator := &RequiredFieldValidator{Field: "name"}

	// Create a file-based checkpoint store.
	checkpointStore := &FileCheckpointStore{fileName: "checkpoint.txt"}

	// Configure the ETL pipeline with all components and options.
	etlInstance := etl.NewETL(
		etl.WithSources(csvSource),
		etl.WithLoaders([]contracts.Loader{jsonLoader}...),
		etl.WithMappers([]contracts.Mapper{lowercaseMapper}...),
		etl.WithTransformers([]contracts.Transformer{ageTransformer}...),
		etl.WithValidators([]contracts.Validator{requiredValidator}...),
		etl.WithCheckpointStore(checkpointStore, func(rec utils.Record) string {
			// In this example, we use the "name" field as the checkpoint.
			if name, ok := rec["name"].(string); ok {
				return name
			}
			return ""
		}),
		etl.WithRawChanBuffer(50), // Buffer size for backpressure management.
		etl.WithWorkerCount(2),
		etl.WithBatchSize(5),
	)

	// Run the ETL pipeline.
	if err := etlInstance.Run(); err != nil {
		log.Fatalf("ETL run failed: %v", err)
	}

	// Close resources.
	if err := etlInstance.Close(); err != nil {
		log.Fatalf("Error closing ETL: %v", err)
	}

	log.Println("ETL processing complete.")
}
