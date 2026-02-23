package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/convert"

	"github.com/oarkflow/sql/pkg/utils"
)

type ScalarFunctionHandler func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any
type AggregateFunctionHandler func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any

type sqlFunctionRegistry struct {
	mu        sync.RWMutex
	scalar    map[string]ScalarFunctionHandler
	aggregate map[string]AggregateFunctionHandler
	opts      FunctionRegistryOptions
}

type FunctionRegistryOptions struct {
	AllowOverride bool
	Frozen        bool
}

var (
	functionRegistryInitOnce sync.Once
	functionRegistry         = &sqlFunctionRegistry{
		scalar:    make(map[string]ScalarFunctionHandler),
		aggregate: make(map[string]AggregateFunctionHandler),
		opts: FunctionRegistryOptions{
			AllowOverride: false,
			Frozen:        false,
		},
	}
)

func RegisterScalarFunction(name string, handler ScalarFunctionHandler) {
	ensureFunctionRegistryInitialized()
	_ = functionRegistry.registerScalar(name, handler, false)
}

func RegisterAggregateFunction(name string, handler AggregateFunctionHandler) {
	ensureFunctionRegistryInitialized()
	_ = functionRegistry.registerAggregate(name, handler, false)
}

func RegisterScalarFunctionE(name string, handler ScalarFunctionHandler) error {
	ensureFunctionRegistryInitialized()
	return functionRegistry.registerScalar(name, handler, false)
}

func RegisterAggregateFunctionE(name string, handler AggregateFunctionHandler) error {
	ensureFunctionRegistryInitialized()
	return functionRegistry.registerAggregate(name, handler, false)
}

func UnregisterScalarFunction(name string) error {
	ensureFunctionRegistryInitialized()
	return functionRegistry.unregisterScalar(name)
}

func UnregisterAggregateFunction(name string) error {
	ensureFunctionRegistryInitialized()
	return functionRegistry.unregisterAggregate(name)
}

func SetFunctionRegistryOptions(opts FunctionRegistryOptions) {
	ensureFunctionRegistryInitialized()
	functionRegistry.mu.Lock()
	functionRegistry.opts = opts
	functionRegistry.mu.Unlock()
}

func GetFunctionRegistryOptions() FunctionRegistryOptions {
	ensureFunctionRegistryInitialized()
	functionRegistry.mu.RLock()
	defer functionRegistry.mu.RUnlock()
	return functionRegistry.opts
}

func FreezeFunctionRegistry() {
	ensureFunctionRegistryInitialized()
	functionRegistry.mu.Lock()
	functionRegistry.opts.Frozen = true
	functionRegistry.mu.Unlock()
}

func UnfreezeFunctionRegistry() {
	ensureFunctionRegistryInitialized()
	functionRegistry.mu.Lock()
	functionRegistry.opts.Frozen = false
	functionRegistry.mu.Unlock()
}

func LookupScalarFunction(name string) (ScalarFunctionHandler, bool) {
	ensureFunctionRegistryInitialized()
	functionRegistry.mu.RLock()
	defer functionRegistry.mu.RUnlock()
	handler, ok := functionRegistry.scalar[strings.ToUpper(strings.TrimSpace(name))]
	return handler, ok
}

func LookupAggregateFunction(name string) (AggregateFunctionHandler, bool) {
	ensureFunctionRegistryInitialized()
	functionRegistry.mu.RLock()
	defer functionRegistry.mu.RUnlock()
	handler, ok := functionRegistry.aggregate[strings.ToUpper(strings.TrimSpace(name))]
	return handler, ok
}

func IsAggregateFunction(name string) bool {
	_, ok := LookupAggregateFunction(name)
	return ok
}

func EvaluateAggregateFunction(name string, ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) (any, bool) {
	handler, ok := LookupAggregateFunction(name)
	if !ok {
		return nil, false
	}
	return handler(ctx, execCtx, args, rows), true
}

func ensureFunctionRegistryInitialized() {
	functionRegistryInitOnce.Do(registerDefaultFunctions)
}

func (r *sqlFunctionRegistry) registerScalar(name string, handler ScalarFunctionHandler, internal bool) error {
	n := strings.ToUpper(strings.TrimSpace(name))
	if n == "" || handler == nil {
		return &SQLError{Code: ErrCodeRegistry, Message: "invalid scalar function registration"}
	}
	r.mu.Lock()
	if r.opts.Frozen && !internal {
		r.mu.Unlock()
		return &SQLError{Code: ErrCodeRegistry, Message: "function registry is frozen"}
	}
	if _, exists := r.scalar[n]; exists && !r.opts.AllowOverride && !internal {
		r.mu.Unlock()
		return &SQLError{Code: ErrCodeRegistry, Message: "scalar function already exists: " + n}
	}
	r.scalar[n] = handler
	r.mu.Unlock()
	return nil
}

func (r *sqlFunctionRegistry) registerAggregate(name string, handler AggregateFunctionHandler, internal bool) error {
	n := strings.ToUpper(strings.TrimSpace(name))
	if n == "" || handler == nil {
		return &SQLError{Code: ErrCodeRegistry, Message: "invalid aggregate function registration"}
	}
	r.mu.Lock()
	if r.opts.Frozen && !internal {
		r.mu.Unlock()
		return &SQLError{Code: ErrCodeRegistry, Message: "function registry is frozen"}
	}
	if _, exists := r.aggregate[n]; exists && !r.opts.AllowOverride && !internal {
		r.mu.Unlock()
		return &SQLError{Code: ErrCodeRegistry, Message: "aggregate function already exists: " + n}
	}
	r.aggregate[n] = handler
	r.mu.Unlock()
	return nil
}

func (r *sqlFunctionRegistry) unregisterScalar(name string) error {
	n := strings.ToUpper(strings.TrimSpace(name))
	if n == "" {
		return &SQLError{Code: ErrCodeRegistry, Message: "invalid scalar function name"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.opts.Frozen {
		return &SQLError{Code: ErrCodeRegistry, Message: "function registry is frozen"}
	}
	delete(r.scalar, n)
	return nil
}

func (r *sqlFunctionRegistry) unregisterAggregate(name string) error {
	n := strings.ToUpper(strings.TrimSpace(name))
	if n == "" {
		return &SQLError{Code: ErrCodeRegistry, Message: "invalid aggregate function name"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.opts.Frozen {
		return &SQLError{Code: ErrCodeRegistry, Message: "function registry is frozen"}
	}
	delete(r.aggregate, n)
	return nil
}

func registerDefaultFunctions() {
	registerDefaultScalarFunctions()
	registerDefaultAggregateFunctions()
}

func registerDefaultScalarFunctions() {
	_ = functionRegistry.registerScalar("NOW", func(_ *EvalContext, _ context.Context, _ []Expression, _ utils.Record) any {
		return time.Now().Format("2006-01-02 15:04:05")
	}, true)
	_ = functionRegistry.registerScalar("CURRENT_TIMESTAMP", func(_ *EvalContext, _ context.Context, _ []Expression, _ utils.Record) any {
		return time.Now().Format("2006-01-02 15:04:05")
	}, true)
	_ = functionRegistry.registerScalar("COALESCE", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		for _, arg := range args {
			val := ctx.evalExpression(execCtx, arg, row)
			if !isNilOrEmpty(val) {
				return val
			}
		}
		return nil
	}, true)
	_ = functionRegistry.registerScalar("CONCAT", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		var parts []string
		for _, arg := range args {
			val := ctx.evalExpression(execCtx, arg, row)
			parts = append(parts, fmt.Sprintf("%v", val))
		}
		return strings.Join(parts, "")
	}, true)
	_ = functionRegistry.registerScalar("IF", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 3 {
			ctx.logError("IF requires at least 3 arguments")
			return nil
		}
		cond := ctx.evalExpression(execCtx, args[0], row)
		if b, ok := cond.(bool); ok && b {
			return ctx.evalExpression(execCtx, args[1], row)
		}
		return ctx.evalExpression(execCtx, args[2], row)
	}, true)
	_ = functionRegistry.registerScalar("SUBSTR", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("SUBSTR requires at least 2 arguments")
			return nil
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		startVal, ok := convert.ToFloat64(ctx.evalExpression(execCtx, args[1], row))
		if !ok {
			return nil
		}
		start := int(startVal) - 1
		if start < 0 || start >= len(s) {
			return ""
		}
		if len(args) == 3 {
			lenVal, ok := convert.ToFloat64(ctx.evalExpression(execCtx, args[2], row))
			if !ok {
				return nil
			}
			length := int(lenVal)
			end := start + length
			if end > len(s) {
				end = len(s)
			}
			return s[start:end]
		}
		return s[start:]
	}, true)
	_ = functionRegistry.registerScalar("LENGTH", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) == 0 {
			return 0
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		return len(s)
	}, true)
	_ = functionRegistry.registerScalar("UPPER", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) == 0 {
			return ""
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		return strings.ToUpper(s)
	}, true)
	_ = functionRegistry.registerScalar("LOWER", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) == 0 {
			return ""
		}
		s := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		return strings.ToLower(s)
	}, true)
	_ = functionRegistry.registerScalar("TO_DATE", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("TO_DATE requires 2 arguments")
			return nil
		}
		dateStr := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		formatStr := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[1], row))
		layout := sqlDateFormatToGoLayout(formatStr)
		t, err := time.Parse(layout, dateStr)
		if err != nil {
			ctx.logError("TO_DATE parse error: " + err.Error())
			return nil
		}
		return t.Format("2006-01-02")
	}, true)
	_ = functionRegistry.registerScalar("TO_NUMBER", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			return nil
		}
		numStr := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			ctx.logError("TO_NUMBER parse error: " + err.Error())
			return nil
		}
		return f
	}, true)
	_ = functionRegistry.registerScalar("ROUND", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("ROUND requires at least 1 argument")
			return nil
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		num, ok := convert.ToFloat64(val)
		if !ok {
			ctx.logError("ROUND: unable to convert argument to number")
			return nil
		}
		precision := 0.0
		if len(args) >= 2 {
			pVal := ctx.evalExpression(execCtx, args[1], row)
			if p, ok := convert.ToFloat64(pVal); ok {
				precision = p
			}
		}
		factor := math.Pow(10, precision)
		return math.Round(num*factor) / factor
	}, true)
	_ = functionRegistry.registerScalar("DATEDIFF", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("DATEDIFF requires 2 arguments")
			return nil
		}
		date1Str := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[0], row))
		date2Str := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[1], row))
		t1, err1 := time.Parse("2006-01-02", date1Str)
		t2, err2 := time.Parse("2006-01-02", date2Str)
		if err1 != nil || err2 != nil {
			ctx.logError("DATEDIFF parse error: invalid date format")
			return nil
		}
		diff := t1.Sub(t2)
		return int(diff.Hours() / 24)
	}, true)
	_ = functionRegistry.registerScalar("ANY", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("ANY requires one argument")
			return quantifiedValues{Mode: "ANY"}
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		return quantifiedValues{Mode: "ANY", Values: toComparableSlice(val)}
	}, true)
	_ = functionRegistry.registerScalar("ALL", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("ALL requires one argument")
			return quantifiedValues{Mode: "ALL"}
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		return quantifiedValues{Mode: "ALL", Values: toComparableSlice(val)}
	}, true)
	_ = functionRegistry.registerScalar("CAST", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("CAST requires 2 arguments: CAST(expr AS type)")
			return nil
		}
		typeName := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[1], row))
		value := ctx.evalExpression(execCtx, args[0], row)
		return castValue(typeName, value)
	}, true)
	_ = functionRegistry.registerScalar("JSON_EXTRACT", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("JSON_EXTRACT requires 2 arguments")
			return nil
		}
		payload := ctx.evalExpression(execCtx, args[0], row)
		path := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[1], row))
		val, ok := resolvePathFromValue(payload, normalizeJSONPath(path))
		if !ok {
			return nil
		}
		return val
	}, true)
	_ = functionRegistry.registerScalar("JSON_EXISTS", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("JSON_EXISTS requires 2 arguments")
			return false
		}
		payload := ctx.evalExpression(execCtx, args[0], row)
		path := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[1], row))
		_, ok := resolvePathFromValue(payload, normalizeJSONPath(path))
		return ok
	}, true)
	_ = functionRegistry.registerScalar("JSON_VALUE", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 2 {
			ctx.logError("JSON_VALUE requires 2 arguments")
			return nil
		}
		payload := ctx.evalExpression(execCtx, args[0], row)
		path := fmt.Sprintf("%v", ctx.evalExpression(execCtx, args[1], row))
		val, ok := resolvePathFromValue(payload, normalizeJSONPath(path))
		if !ok {
			return nil
		}
		if arr, ok := toAnySlice(val); ok {
			for _, item := range arr {
				if item != nil {
					return item
				}
			}
			return nil
		}
		return val
	}, true)
	_ = functionRegistry.registerScalar("YEAR", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("YEAR requires 1 argument")
			return nil
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		t, ok := parseFlexibleDateTime(val)
		if !ok {
			return nil
		}
		return t.Year()
	}, true)
	_ = functionRegistry.registerScalar("MONTH", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("MONTH requires 1 argument")
			return nil
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		t, ok := parseFlexibleDateTime(val)
		if !ok {
			return nil
		}
		return int(t.Month())
	}, true)
	_ = functionRegistry.registerScalar("FIRST", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("FIRST requires 1 argument")
			return nil
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		arr, ok := toAnySlice(val)
		if !ok || len(arr) == 0 {
			return val
		}
		return arr[0]
	}, true)
	_ = functionRegistry.registerScalar("LAST", func(ctx *EvalContext, execCtx context.Context, args []Expression, row utils.Record) any {
		if len(args) < 1 {
			ctx.logError("LAST requires 1 argument")
			return nil
		}
		val := ctx.evalExpression(execCtx, args[0], row)
		arr, ok := toAnySlice(val)
		if !ok || len(arr) == 0 {
			return val
		}
		return arr[len(arr)-1]
	}, true)
}

func registerDefaultAggregateFunctions() {
	_ = functionRegistry.registerAggregate("COUNT", func(_ *EvalContext, _ context.Context, _ []Expression, rows []utils.Record) any {
		return len(rows)
	}, true)
	_ = functionRegistry.registerAggregate("AVG", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 {
			return nil
		}
		sum := 0.0
		count := 0.0
		for _, r := range rows {
			val := ctx.evalExpression(execCtx, args[0], r)
			num, ok := convert.ToFloat64(val)
			if ok {
				sum += num
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / count
	}, true)
	_ = functionRegistry.registerAggregate("SUM", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 {
			return nil
		}
		sum := 0.0
		for _, r := range rows {
			val := ctx.evalExpression(execCtx, args[0], r)
			num, ok := convert.ToFloat64(val)
			if ok {
				sum += num
			}
		}
		return sum
	}, true)
	_ = functionRegistry.registerAggregate("MIN", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 {
			return nil
		}
		var minVal float64
		first := true
		for _, r := range rows {
			val := ctx.evalExpression(execCtx, args[0], r)
			num, ok := convert.ToFloat64(val)
			if ok {
				if first || num < minVal {
					minVal = num
					first = false
				}
			}
		}
		if first {
			return nil
		}
		return minVal
	}, true)
	_ = functionRegistry.registerAggregate("MAX", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 {
			return nil
		}
		var maxVal float64
		first := true
		for _, r := range rows {
			val := ctx.evalExpression(execCtx, args[0], r)
			num, ok := convert.ToFloat64(val)
			if ok {
				if first || num > maxVal {
					maxVal = num
					first = false
				}
			}
		}
		if first {
			return nil
		}
		return maxVal
	}, true)
	_ = functionRegistry.registerAggregate("DIFF", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 {
			return nil
		}
		var minVal, maxVal float64
		first := true
		for _, r := range rows {
			val := ctx.evalExpression(execCtx, args[0], r)
			num, ok := convert.ToFloat64(val)
			if ok {
				if first {
					minVal = num
					maxVal = num
					first = false
				} else {
					if num < minVal {
						minVal = num
					}
					if num > maxVal {
						maxVal = num
					}
				}
			}
		}
		if first {
			return nil
		}
		return maxVal - minVal
	}, true)
	_ = functionRegistry.registerAggregate("FIRST", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 || len(rows) == 0 {
			return nil
		}
		idx := pickDeterministicRowIndex(rows, false)
		return ctx.evalExpression(execCtx, args[0], rows[idx])
	}, true)
	_ = functionRegistry.registerAggregate("LAST", func(ctx *EvalContext, execCtx context.Context, args []Expression, rows []utils.Record) any {
		if len(args) == 0 || len(rows) == 0 {
			return nil
		}
		idx := pickDeterministicRowIndex(rows, true)
		return ctx.evalExpression(execCtx, args[0], rows[idx])
	}, true)
}

func pickDeterministicRowIndex(rows []utils.Record, wantLast bool) int {
	if len(rows) == 0 {
		return 0
	}
	bestIdx := 0
	bestKey := recordStableKey(rows[0])
	for i := 1; i < len(rows); i++ {
		k := recordStableKey(rows[i])
		cmp := strings.Compare(k, bestKey)
		if (!wantLast && cmp < 0) || (wantLast && cmp > 0) {
			bestIdx = i
			bestKey = k
		}
	}
	return bestIdx
}

func recordStableKey(row utils.Record) string {
	b, err := json.Marshal(row)
	if err != nil {
		return fmt.Sprintf("%v", row)
	}
	return string(b)
}
