package transformers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

type AggregatorTransformer struct {
	GroupBy      []string                       `json:"group_by" yaml:"group_by"`
	Aggregations []config.AggregationDefinition `json:"aggregations" yaml:"aggregations"`

	groups map[string]map[string]*aggValue
}

type aggValue struct {
	sum   float64
	count int
	min   float64
	max   float64
	set   bool
}

func NewAggregatorTransformer(groupBy []string, aggs []config.AggregationDefinition) *AggregatorTransformer {
	return &AggregatorTransformer{
		GroupBy:      groupBy,
		Aggregations: aggs,
		groups:       make(map[string]map[string]*aggValue),
	}
}

func (at *AggregatorTransformer) Name() string {
	return "AggregatorTransformer"
}

func (at *AggregatorTransformer) Transform(_ context.Context, rec utils.Record) (utils.Record, error) {
	at.aggregateRecord(rec)
	return nil, nil
}

func (at *AggregatorTransformer) TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error) {
	_, err := at.Transform(ctx, rec)
	if err != nil {
		return nil, err
	}
	return []utils.Record{}, nil
}

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

func (at *AggregatorTransformer) Flush(_ context.Context) ([]utils.Record, error) {
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

func splitGroupKey(key string) []string {
	parts := strings.Split(key, "|")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func mathRound(f float64) float64 {
	return float64(int(f + 0.5))
}
