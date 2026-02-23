package sql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/sql/pkg/utils"
)

type RuntimeConfig struct {
	QueryTimeout       time.Duration
	MaxResultRows      int
	MaxExpressionDepth int
	LogQueryExecution  bool
}

var (
	runtimeConfigMu sync.RWMutex
	runtimeConfig   = RuntimeConfig{
		QueryTimeout:       30 * time.Second,
		MaxResultRows:      10000,
		MaxExpressionDepth: 256,
		LogQueryExecution:  true,
	}
)

type runtimeConfigContextKey struct{}

type RuntimeConfigOverride struct {
	QueryTimeout       *time.Duration
	MaxResultRows      *int
	MaxExpressionDepth *int
	LogQueryExecution  *bool
}

func SetRuntimeConfig(cfg RuntimeConfig) {
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	runtimeConfig = cfg
}

func GetRuntimeConfig() RuntimeConfig {
	runtimeConfigMu.RLock()
	defer runtimeConfigMu.RUnlock()
	return runtimeConfig
}

func WithRuntimeConfigOverride(ctx context.Context, override RuntimeConfigOverride) context.Context {
	return context.WithValue(ctx, runtimeConfigContextKey{}, override)
}

func effectiveRuntimeConfig(ctx context.Context) RuntimeConfig {
	cfg := GetRuntimeConfig()
	ov, ok := ctx.Value(runtimeConfigContextKey{}).(RuntimeConfigOverride)
	if !ok {
		return cfg
	}
	if ov.QueryTimeout != nil {
		cfg.QueryTimeout = *ov.QueryTimeout
	}
	if ov.MaxResultRows != nil {
		cfg.MaxResultRows = *ov.MaxResultRows
	}
	if ov.MaxExpressionDepth != nil {
		cfg.MaxExpressionDepth = *ov.MaxExpressionDepth
	}
	if ov.LogQueryExecution != nil {
		cfg.LogQueryExecution = *ov.LogQueryExecution
	}
	return cfg
}

func withQueryTimeout(ctx context.Context, cfg RuntimeConfig) (context.Context, context.CancelFunc) {
	if cfg.QueryTimeout <= 0 {
		return ctx, func() {}
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, cfg.QueryTimeout)
}

func capRows(rows []utils.Record, cfg RuntimeConfig) []utils.Record {
	if cfg.MaxResultRows <= 0 || len(rows) <= cfg.MaxResultRows {
		return rows
	}
	return rows[:cfg.MaxResultRows]
}

func wrapContextErr(err error) error {
	if err == nil {
		return nil
	}
	if err == context.DeadlineExceeded {
		return &SQLError{
			Code:    ErrCodeTimeout,
			Message: "query timed out",
			Cause:   err,
		}
	}
	if err == context.Canceled {
		return &SQLError{
			Code:    ErrCodeCanceled,
			Message: "query canceled",
			Cause:   err,
		}
	}
	return err
}

type ErrorCode string

const (
	ErrCodeParse     ErrorCode = "PARSE_ERROR"
	ErrCodeExecution ErrorCode = "EXECUTION_ERROR"
	ErrCodeTimeout   ErrorCode = "QUERY_TIMEOUT"
	ErrCodeCanceled  ErrorCode = "QUERY_CANCELED"
	ErrCodeInput     ErrorCode = "INPUT_VALIDATION_ERROR"
	ErrCodeRegistry  ErrorCode = "REGISTRY_ERROR"
)

type SQLError struct {
	Code    ErrorCode
	Message string
	Details []string
	Cause   error
}

func (e *SQLError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *SQLError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
