package mapper

import (
	"github.com/oarkflow/sql/utils"
)

type FieldMapper struct {
	mapping             map[string]string
	additionalFields    map[string]any
	keepUnmatchedFields bool
}

func NewFieldMapper(mapping map[string]string, additionalFields map[string]any, keepUnmatchedFields bool) *FieldMapper {
	return &FieldMapper{
		mapping:             mapping,
		additionalFields:    additionalFields,
		keepUnmatchedFields: keepUnmatchedFields,
	}
}

func (gm *FieldMapper) Map(rec utils.Record) (utils.Record, error) {
	out := make(utils.Record)
	if gm.mapping == nil || len(gm.mapping) == 0 {
		out = rec
	} else {
		for outKey, inKey := range gm.mapping {
			if val, exists := rec[inKey]; exists {
				out[outKey] = val
			} else if gm.keepUnmatchedFields {
				out[inKey] = rec[inKey]
			}
		}
	}
	if out == nil {
		out = make(utils.Record)
	}
	for key, val := range gm.additionalFields {
		out[key] = val
	}
	return out, nil
}
