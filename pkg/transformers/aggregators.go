package transformers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/oarkflow/etl/pkg/utils"
)

// AggregationDefinition defines one aggregation on a source field.
type AggregationDefinition struct {
	SourceField string `json:"source_field" yaml:"source_field"`
	Func        string `json:"func" yaml:"func"`
	OutputField string `json:"output_field" yaml:"output_field"`
}

// AggregatorTransformer groups incoming records by one or more fields (GroupBy)
// and computes aggregations defined in Aggregations.
// It buffers input and produces output only when Flush is called.
type AggregatorTransformer struct {
	GroupBy      []string                `json:"group_by" yaml:"group_by"`
	Aggregations []AggregationDefinition `json:"aggregations" yaml:"aggregations"`

	groups map[string]map[string]*aggValue
}

// aggValue holds intermediate aggregation values.
type aggValue struct {
	sum   float64
	count int
	min   float64
	max   float64
	set   bool // indicates if min/max has been set
}

// NewAggregatorTransformer creates a new aggregator transformer.
func NewAggregatorTransformer(groupBy []string, aggs []AggregationDefinition) *AggregatorTransformer {
	return &AggregatorTransformer{
		GroupBy:      groupBy,
		Aggregations: aggs,
		groups:       make(map[string]map[string]*aggValue),
	}
}

// Name returns the transformer name.
func (at *AggregatorTransformer) Name() string {
	return "AggregatorTransformer"
}

// Transform buffers the record into the aggregator. No output is produced immediately.
func (at *AggregatorTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	at.aggregateRecord(rec)
	return nil, nil
}

// TransformMany behaves similarly.
func (at *AggregatorTransformer) TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error) {
	_, err := at.Transform(ctx, rec)
	if err != nil {
		return nil, err
	}
	return []utils.Record{}, nil
}

// aggregateRecord computes a group key from the GroupBy fields and updates the aggregation state.
func (at *AggregatorTransformer) aggregateRecord(rec utils.Record) {
	var groupKeyBuilder strings.Builder
	for _, key := range at.GroupBy {
		groupKeyBuilder.WriteString(fmt.Sprintf("%v|", rec[key]))
	}
	groupKey := groupKeyBuilder.String()

	if _, exists := at.groups[groupKey]; !exists {
		at.groups[groupKey] = make(map[string]*aggValue)
	}
	group := at.groups[groupKey]

	for _, agg := range at.Aggregations {
		aggKey := agg.OutputField
		if aggKey == "" {
			aggKey = agg.Func + "_" + agg.SourceField
		}
		if _, exists := group[aggKey]; !exists {
			group[aggKey] = &aggValue{}
		}
		value := group[aggKey]
		switch agg.Func {
		case "count":
			value.count++
		case "sum", "avg":
			num, err := toFloat(rec[agg.SourceField])
			if err != nil {
				continue
			}
			value.sum += num
			value.count++
		case "min":
			num, err := toFloat(rec[agg.SourceField])
			if err != nil {
				continue
			}
			if !value.set || num < value.min {
				value.min = num
			}
			value.set = true
		case "max":
			num, err := toFloat(rec[agg.SourceField])
			if err != nil {
				continue
			}
			if !value.set || num > value.max {
				value.max = num
			}
			value.set = true
		default:
			log.Printf("Unsupported aggregation function: %s", agg.Func)
		}
	}
}

// Flush computes and returns the aggregated records. For "avg", the value is rounded to the nearest integer.
func (at *AggregatorTransformer) Flush(ctx context.Context) ([]utils.Record, error) {
	var results []utils.Record
	for groupKey, aggs := range at.groups {
		rec := make(utils.Record)
		groupValues := splitGroupKey(groupKey)
		for i, key := range at.GroupBy {
			if i < len(groupValues) {
				rec[key] = groupValues[i]
			}
		}
		for _, agg := range at.Aggregations {
			aggKey := agg.OutputField
			if aggKey == "" {
				aggKey = agg.Func + "_" + agg.SourceField
			}
			if value, exists := aggs[aggKey]; exists {
				switch agg.Func {
				case "count":
					rec[aggKey] = value.count
				case "sum":
					rec[aggKey] = value.sum
				case "avg":
					if value.count > 0 {
						avg := value.sum / float64(value.count)
						// Rounding average to nearest integer.
						rec[aggKey] = int(mathRound(avg))
					} else {
						rec[aggKey] = nil
					}
				case "min":
					rec[aggKey] = value.min
				case "max":
					rec[aggKey] = value.max
				}
			}
		}
		results = append(results, rec)
	}
	return results, nil
}

// toFloat converts a value to float64.
func toFloat(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %v to float", val)
	}
}

// splitGroupKey splits a group key using "|" as delimiter.
func splitGroupKey(key string) []string {
	parts := strings.Split(key, "|")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// mathRound rounds a float64 to the nearest integer.
func mathRound(f float64) float64 {
	return float64(int(f + 0.5))
}
