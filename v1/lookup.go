package v1

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/oarkflow/sql/utils"
)

// GlobalLookupStore holds preloaded lookup datasets.
// Each dataset is stored as a slice of rows where each row is a map[string]string.
var GlobalLookupStore = make(map[string][]map[string]string)

// lookupInCache caches lookup results for a given combination of parameters.
var lookupInCache sync.Map

// lookupIn is the custom function used in mapping expressions. It expects exactly four arguments:
//
//	0: string – the lookup dataset key (e.g. "facilities")
//	1: string – the lookup field name (e.g. "facility_name")
//	2: any    – the source record's value to match (e.g. facility_name value)
//	3: string – the target field name (e.g. "facility_id")
//
// It retrieves the lookup dataset from GlobalLookupStore, searches for a row where the value in the lookup field
// equals the provided source value, and returns the corresponding value from the target field.
// The result is cached to avoid repeated lookups.
func (e *ETL) lookupIn(args ...interface{}) (interface{}, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("lookupIn requires exactly 4 arguments")
	}

	datasetKey, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("lookupIn: first argument must be string (lookup dataset key)")
	}
	lookupField, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("lookupIn: second argument must be string (lookup field name)")
	}
	sourceValStr := fmt.Sprintf("%v", args[2])
	targetField, ok := args[3].(string)
	if !ok {
		return nil, fmt.Errorf("lookupIn: fourth argument must be string (target field name)")
	}

	// Compose a cache key that uniquely identifies this lookup request.
	cacheKey := datasetKey + ":" + lookupField + ":" + sourceValStr + ":" + targetField
	if cached, found := lookupInCache.Load(cacheKey); found {
		return cached, nil
	}

	// Retrieve the lookup dataset.
	dataset, exists := GlobalLookupStore[datasetKey]
	if !exists {
		return nil, fmt.Errorf("lookupIn: no lookup dataset found for key %s", datasetKey)
	}

	// Search for a row where the value in the lookupField matches the source value.
	for _, row := range dataset {
		if row[lookupField] == sourceValStr {
			result := row[targetField]
			lookupInCache.Store(cacheKey, result)
			return result, nil
		}
	}

	return nil, fmt.Errorf("lookupIn: no matching value for %s in dataset %s", sourceValStr, datasetKey)
}

type EvalFieldMapper struct {
	mapping map[string]string
}

func NewEvalFieldMapper(mapping map[string]string) *EvalFieldMapper {
	return &EvalFieldMapper{mapping: mapping}
}

func (efm *EvalFieldMapper) Name() string {
	return "EvalFieldMapper"
}

func (efm *EvalFieldMapper) Map(ctx context.Context, rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for destField, exprStr := range efm.mapping {
		if strings.HasPrefix(exprStr, "eval.{{") && strings.HasSuffix(exprStr, "}}") {
			_, result := utils.GetValue(ctx, exprStr, rec)
			newRec[destField] = result
		} else {
			// Non-eval expressions are copied as is.
			newRec[destField] = exprStr
		}
	}
	return newRec, nil
}

// ---------------------------------------------------------------------
// Helper Functions to Load Lookup Data (Generic Version)
// ---------------------------------------------------------------------

// loadLookupDataFromSQLGeneric loads all rows returned by the query into a slice of maps.
// Each row is represented as a map with column names as keys and their string representations as values.
func loadLookupDataFromSQLGeneric(db *sql.DB, query string) ([]map[string]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]string
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		rowMap := make(map[string]string)
		for i, colName := range cols {
			rowMap[colName] = fmt.Sprintf("%v", columns[i])
		}
		result = append(result, rowMap)
	}
	return result, nil
}

// loadLookupDataFromCSVGeneric loads all rows from the CSV file into a slice of maps.
// The CSV header row is used as keys.
func loadLookupDataFromCSVGeneric(file string) ([]map[string]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	headers, err := r.Read()
	if err != nil {
		return nil, err
	}

	var result []map[string]string
	for {
		row, err := r.Read()
		if err != nil {
			break
		}
		if len(row) != len(headers) {
			continue
		}
		rowMap := make(map[string]string)
		for i, header := range headers {
			rowMap[header] = row[i]
		}
		result = append(result, rowMap)
	}
	return result, nil
}
