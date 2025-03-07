package mappers

import (
	"context"
	"strings"

	"github.com/oarkflow/sql/utils"
)

type FieldMapper struct {
	mapping map[string]string
}

func NewFieldMapper(mapping map[string]string) *FieldMapper {
	return &FieldMapper{mapping: mapping}
}

func (fm *FieldMapper) Name() string {
	return "FieldMapper"
}

func (fm *FieldMapper) Map(ctx context.Context, rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for destField, expr := range fm.mapping {
		_, val := utils.GetValue(ctx, expr, rec)
		newRec[destField] = val
	}
	return newRec, nil
}

type LowercaseMapper struct{}

func (lm *LowercaseMapper) Name() string {
	return "LowercaseMapper"
}

func (lm *LowercaseMapper) Map(ctx context.Context, rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}
