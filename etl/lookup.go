package etl

import (
	"fmt"
	"log"
	"sync"
)

// GlobalLookupStore holds lookup datasets.
// Each key maps to a slice of records (each record is a map[string]any).
var GlobalLookupStore = make(map[string][]map[string]any)

// GlobalLookupCache caches lookup results.
var GlobalLookupCache sync.Map

// LookupInGlobal is a global function to perform lookups across all ETL instances.
// It expects exactly 4 arguments:
//   - datasetKey: the key to retrieve the lookup dataset,
//   - lookupField: the field name to match,
//   - sourceVal: the value to compare (converted to string),
//   - targetField: the field name whose value is to be returned.
func LookupInGlobal(args ...any) (any, error) {
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

	// Build a unique cache key.
	cacheKey := datasetKey + ":" + lookupField + ":" + sourceValStr + ":" + targetField
	if cached, found := GlobalLookupCache.Load(cacheKey); found {
		log.Printf("lookupIn: cache hit for key %s", cacheKey)
		return cached, nil
	}

	// Look for the dataset in the global lookup store.
	dataset, exists := GlobalLookupStore[datasetKey]
	if !exists {
		return nil, fmt.Errorf("lookupIn: no lookup dataset found for key %s", datasetKey)
	}

	// Search through the dataset for a matching row.
	for _, row := range dataset {
		if fmt.Sprintf("%v", row[lookupField]) == sourceValStr {
			result := row[targetField]
			GlobalLookupCache.Store(cacheKey, result)
			return result, nil
		}
	}

	return nil, fmt.Errorf("lookupIn: no matching value for %s in dataset %s", sourceValStr, datasetKey)
}

// ClearGlobalLookupCache clears all entries from the GlobalLookupCache.
func ClearGlobalLookupCache() {
	GlobalLookupCache.Range(func(key, value any) bool {
		GlobalLookupCache.Delete(key)
		return true
	})
}
