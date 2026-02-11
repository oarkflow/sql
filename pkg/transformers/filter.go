package transformers

import (
	"context"
	"fmt"

	"github.com/oarkflow/expr"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

// FilterTransformer filters records based on a condition expression
type FilterTransformer struct {
	name      string
	condition string
}

// NewFilterTransformer creates a new filter transformer with a condition
func NewFilterTransformer(name, condition string) (*FilterTransformer, error) {
	if condition == "" {
		return nil, fmt.Errorf("filter condition cannot be empty")
	}

	return &FilterTransformer{
		name:      name,
		condition: condition,
	}, nil
}

func (ft *FilterTransformer) Name() string {
	return ft.name
}

// Transform evaluates the condition and returns the record if true, nil if false
func (ft *FilterTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	// Parse and evaluate the condition
	program, err := expr.Parse(ft.condition)
	if err != nil {
		return nil, fmt.Errorf("filter parse error: %w", err)
	}

	result, err := program.Eval(rec)
	if err != nil {
		// If evaluation fails, reject the record
		return nil, fmt.Errorf("filter evaluation error: %w", err)
	}

	// Check if result is boolean true
	if boolResult, ok := result.(bool); ok && boolResult {
		return rec, nil
	}

	// Record filtered out - return nil to indicate skip
	return nil, nil
}

var _ contracts.Transformer = (*FilterTransformer)(nil)
