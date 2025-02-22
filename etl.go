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
	"github.com/oarkflow/sql/etl/mapper"
	"github.com/oarkflow/sql/etl/source"
	"github.com/oarkflow/sql/utils"
)

type LowercaseMapper struct{}

func (m *LowercaseMapper) Map(rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}

type VotingTransformer struct{}

func (t *VotingTransformer) Transform(rec utils.Record) (utils.Record, error) {
	fmt.Println(rec)
	if ageStr, ok := rec["age"].(string); ok {
		age, err := strconv.Atoi(ageStr)
		if err != nil {
			return rec, fmt.Errorf("invalid age value: %v", err)
		}
		rec["allowed_voting"] = age > 18
	}
	return rec, nil
}

type RequiredFieldValidator struct {
	Field string
}

func (v *RequiredFieldValidator) Validate(rec utils.Record) error {
	if val, ok := rec[v.Field]; !ok || fmt.Sprintf("%v", val) == "" {
		return fmt.Errorf("required field '%s' is missing or empty", v.Field)
	}
	return nil
}

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

func main() {
	csvSource := source.NewFileSource("input.csv")
	jsonLoader := loader.NewFileLoader("output.json", true)
	fieldMapper := mapper.NewFieldMapper(
		map[string]string{
			"age":  "age",
			"name": "name",
		},
		map[string]any{
			"voting_status": "pending",
		},
		false,
	)
	lowercaseMapper := &LowercaseMapper{}
	ageTransformer := &VotingTransformer{}
	requiredValidator := &RequiredFieldValidator{Field: "name"}
	checkpointStore := &FileCheckpointStore{fileName: "checkpoint.txt"}
	etlInstance := etl.NewETL(
		etl.WithSources(csvSource),
		etl.WithLoaders([]contracts.Loader{jsonLoader}...),
		etl.WithMappers(lowercaseMapper, fieldMapper),
		etl.WithTransformers([]contracts.Transformer{ageTransformer}...),
		etl.WithValidators([]contracts.Validator{requiredValidator}...),
		etl.WithCheckpointStore(checkpointStore, func(rec utils.Record) string {
			if name, ok := rec["name"].(string); ok {
				return name
			}
			return ""
		}),
		etl.WithRawChanBuffer(50),
		etl.WithWorkerCount(2),
		etl.WithBatchSize(5),
	)
	if err := etlInstance.Run(); err != nil {
		log.Fatalf("ETL run failed: %v", err)
	}
	if err := etlInstance.Close(); err != nil {
		log.Fatalf("Error closing ETL: %v", err)
	}
	log.Println("ETL processing complete.")
}
