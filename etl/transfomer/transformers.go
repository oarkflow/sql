package transfomer

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/sql/utils"
)

type UppercaseTransformer struct{}

func (t *UppercaseTransformer) Transform(_ context.Context, rec utils.Record) (utils.Record, error) {
	for k, v := range rec {
		if s, ok := v.(string); ok {
			rec[k] = strings.ToUpper(s)
		}
	}
	return rec, nil
}

type FilterTransformer struct {
	RequiredKey string
}

func (t *FilterTransformer) Transform(_ context.Context, rec utils.Record) (utils.Record, error) {
	if _, ok := rec[t.RequiredKey]; !ok {
		return nil, fmt.Errorf("record missing required key: %s", t.RequiredKey)
	}
	return rec, nil
}

type BasicValidator struct{}

func (v *BasicValidator) Transform(_ context.Context, rec utils.Record) (utils.Record, error) {
	for k, val := range rec {
		if s, ok := val.(string); ok {
			rec[k] = strings.TrimSpace(s)
		}
	}
	return rec, nil
}
