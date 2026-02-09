package quality

import (
	"context"
	"fmt"
	"sync"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

type RecordValidator struct {
	config config.QualityConfig
	rules  map[string][]Rule
}

func NewRecordValidator(cfg config.QualityConfig) *RecordValidator {
	v := &RecordValidator{
		config: cfg,
		rules:  make(map[string][]Rule),
	}

	for _, r := range cfg.Rules {
		ruleFunc := GetRule(r.Rule, r.Args...)
		v.rules[r.Field] = append(v.rules[r.Field], ruleFunc)
	}
	return v
}

func (v *RecordValidator) Validate(ctx context.Context, record utils.Record) (*ValidationResult, error) {
	result := &ValidationResult{Valid: true}

	for field, rules := range v.rules {
		val, exists := record[field]
		// Skip check if field missing? Depends on NotNull.
		// If NotNull is a rule, it handles nil/missing.

		for _, rule := range rules {
			if err := rule(val); err != nil {
				if !exists && err.Error() == "value is null" {
					// fine
				}
				// Check if warn only (need to map back to config, simplified here)
				result.Errors = append(result.Errors, fmt.Errorf("field %s: %w", field, err))
				result.Valid = false
			}
		}
	}

	return result, nil
}

type CircuitBreaker struct {
	config     *config.CircuitBreakerConfig
	totalCount int64
	errorCount int64
	mu         sync.Mutex
}

func NewCircuitBreaker(cfg *config.CircuitBreakerConfig) *CircuitBreaker {
	if cfg == nil {
		return nil
	}
	return &CircuitBreaker{config: cfg}
}

func (cb *CircuitBreaker) Record(isError bool) error {
	if cb == nil {
		return nil
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalCount++
	if isError {
		cb.errorCount++
	}

	if cb.totalCount >= int64(cb.config.MinRecordCount) {
		rate := float64(cb.errorCount) / float64(cb.totalCount) * 100
		if rate > cb.config.ErrorThreshold {
			return fmt.Errorf("circuit breaker tripped: error rate %.2f%% exceeds threshold %.2f%%", rate, cb.config.ErrorThreshold)
		}
	}
	return nil
}
