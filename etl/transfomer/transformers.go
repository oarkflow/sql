package transfomer

import (
	"fmt"
	"strings"

	"github.com/oarkflow/sql/utils"
)

type DotNotationTransformer struct {
	Fields map[string]string
}

func (d *DotNotationTransformer) Transform(rec utils.Record) (utils.Record, error) {
	for outKey, path := range d.Fields {
		val, err := utils.DotGet(rec, path)
		if err != nil {
			return nil, fmt.Errorf("failed to extract %s using path '%s': %w", outKey, path, err)
		}
		rec[outKey] = val
	}
	return rec, nil
}

type UppercaseTransformer struct{}

func (t *UppercaseTransformer) Transform(rec utils.Record) (utils.Record, error) {
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

func (t *FilterTransformer) Transform(rec utils.Record) (utils.Record, error) {
	if _, ok := rec[t.RequiredKey]; !ok {
		return nil, fmt.Errorf("record missing required key: %s", t.RequiredKey)
	}
	return rec, nil
}

type BasicValidator struct{}

func (v *BasicValidator) Transform(rec utils.Record) (utils.Record, error) {
	for k, val := range rec {
		if s, ok := val.(string); ok {
			rec[k] = strings.TrimSpace(s)
		}
	}
	return rec, nil
}
