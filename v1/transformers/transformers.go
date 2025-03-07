package transformers

import (
	"context"
	"fmt"

	"github.com/oarkflow/sql/v1/contracts"
)

type LookupTransformer struct {
	LookupData  map[string]string
	Field       string
	lookupField string
	TargetField string
}

func (lt *LookupTransformer) Transform(ctx context.Context, rec contracts.Record) (contracts.Record, error) {
	if key, ok := rec[lt.Field]; ok {
		keyStr := fmt.Sprintf("%v", key)
		if val, exists := lt.LookupData[keyStr]; exists {
			rec[lt.TargetField] = val
		}
	}
	return rec, nil
}
