package mappers

import (
	"context"
	"strings"

	utils2 "github.com/oarkflow/etl/pkg/utils"
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

func (fm *FieldMapper) Map(ctx context.Context, rec utils2.Record) (utils2.Record, error) {
	newRec := make(utils2.Record)
	for destField, expr := range fm.mapping {
		_, val := utils2.GetValue(ctx, expr, rec)
		newRec[destField] = val
	}
	return newRec, nil
}

type LowercaseMapper struct{}

func (lm *LowercaseMapper) Name() string {
	return "LowercaseMapper"
}

func (lm *LowercaseMapper) Map(_ context.Context, rec utils2.Record) (utils2.Record, error) {
	newRec := make(utils2.Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}
