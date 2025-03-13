package transformer

import (
	"context"
	"fmt"
	"strings"

	utils2 "github.com/oarkflow/etl/pkg/utils"
)

type KeyValueTransformer struct {
	ExtraValues   map[string]any
	IncludeFields []string
	ExcludeFields []string
	KeyField      string
	ValueField    string
}

func NewKeyValue(keyField, valueField string, includeFields, excludeFields []string, extraValues map[string]any) *KeyValueTransformer {
	return &KeyValueTransformer{
		ExtraValues:   extraValues,
		IncludeFields: includeFields,
		ExcludeFields: excludeFields,
		KeyField:      keyField,
		ValueField:    valueField,
	}
}

func (kt *KeyValueTransformer) Name() string {
	return "KeyValueTransformer"
}

func (kt *KeyValueTransformer) Transform(ctx context.Context, rec utils2.Record) (utils2.Record, error) {
	recs, err := kt.TransformMany(ctx, rec)
	if err != nil {
		return nil, err
	}
	if len(recs) > 0 {
		return recs[0], nil
	}
	return nil, fmt.Errorf("no output from KeyValueTransformer")
}

func (kt *KeyValueTransformer) TransformMany(ctx context.Context, rec utils2.Record) ([]utils2.Record, error) {
	base := make(map[string]any)
	for newField, srcFieldRaw := range kt.ExtraValues {
		srcField := strings.ToLower(fmt.Sprintf("%v", srcFieldRaw))
		if val, ok := rec[srcField]; ok {
			base[newField] = val
		}
	}
	ignore := make(map[string]struct{})
	for _, v := range kt.ExtraValues {
		ignore[strings.ToLower(fmt.Sprintf("%v", v))] = struct{}{}
	}
	for _, f := range kt.IncludeFields {
		ignore[strings.ToLower(f)] = struct{}{}
	}
	for _, f := range kt.ExcludeFields {
		ignore[strings.ToLower(f)] = struct{}{}
	}
	var candidates []string
	for k := range rec {
		kl := strings.ToLower(k)
		if _, found := ignore[kl]; !found {
			candidates = append(candidates, kl)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate fields found for key-value conversion")
	}
	var out []utils2.Record
	for _, cand := range candidates {
		newRec := make(utils2.Record)
		for k, v := range base {
			newRec[k] = v
		}
		newRec[kt.KeyField] = cand
		if val, ok := rec[cand]; ok {
			newRec[kt.ValueField] = val
			newRec["value_type"] = utils2.GetDataType(val)
		}
		out = append(out, newRec)
	}
	return out, nil
}
