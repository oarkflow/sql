package main

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/oarkflow/date"
)

var mysqlDataTypes = map[string]string{
	"int64":       "BIGINT",
	"float64":     "DOUBLE",
	"bool":        "BOOLEAN",
	"time":        "DATETIME",
	"complex":     "TEXT",
	"string":      "VARCHAR(255)",
	"json":        "JSON",
	"bytes":       "BLOB",
	"slice":       "JSON",
	"map":         "JSON",
	"struct":      "JSON",
	"json.Number": "BIGINT",
}

var postgresDataTypes = map[string]string{
	"int64":       "BIGINT",
	"float64":     "DOUBLE PRECISION",
	"bool":        "BOOLEAN",
	"time":        "TIMESTAMP",
	"complex":     "TEXT",
	"string":      "VARCHAR(255)",
	"json":        "JSONB",
	"bytes":       "bytea",
	"slice":       "JSONB",
	"map":         "JSONB",
	"struct":      "JSONB",
	"json.Number": "BIGINT",
}

var sqliteDataTypes = map[string]string{
	"int64":       "INTEGER",
	"float64":     "REAL",
	"bool":        "NUMERIC",
	"time":        "DATETIME",
	"complex":     "TEXT",
	"string":      "TEXT",
	"json":        "JSON",
	"bytes":       "BLOB",
	"slice":       "JSON",
	"map":         "JSON",
	"struct":      "JSON",
	"json.Number": "INTEGER",
}

type FieldSchema struct {
	FieldName       string
	DataType        string
	IsNullable      bool
	IsPrimaryKey    bool
	MaxStringLength int
}

type FieldStats struct {
	countNonNull         int
	nullable             bool
	uniqueValues         map[string]bool
	duplicateFound       bool
	typeCounts           map[string]int
	hasNumeric           bool
	numericAllIntegral   bool
	totalStringCount     int
	stringAsTimeCount    int
	stringAsComplexCount int
	stringAsIntCount     int
	stringAsFloatCount   int
	maxStringLength      int
}

func dereference(value any) any {
	for value != nil && reflect.TypeOf(value).Kind() == reflect.Ptr {
		v := reflect.ValueOf(value)
		if v.IsNil() {
			return nil
		}
		value = v.Elem().Interface()
	}
	return value
}

func isStringTime(s string) bool {
	_, err := date.Parse(s)
	return err == nil
}

func isStringComplex(s string) bool {
	_, err := strconv.ParseComplex(s, 128)
	return err == nil
}

func updateSchema(schema map[string]FieldSchema, field, finalType string, stats *FieldStats) {
	schema[field] = FieldSchema{
		FieldName:       field,
		DataType:        finalType,
		IsNullable:      stats.nullable,
		IsPrimaryKey:    false,
		MaxStringLength: stats.maxStringLength,
	}
}

func DetectSchema(data []map[string]any, sampleSize int) map[string]FieldSchema {
	schema := make(map[string]FieldSchema)
	fieldStats := make(map[string]*FieldStats)
	totalSize := len(data)
	if sampleSize > totalSize {
		sampleSize = totalSize
	}
	for i := 0; i < sampleSize; i++ {
		row := data[i]
		for key, origValue := range row {
			value := dereference(origValue)
			if _, ok := fieldStats[key]; !ok {
				fieldStats[key] = &FieldStats{
					uniqueValues:       make(map[string]bool),
					typeCounts:         make(map[string]int),
					numericAllIntegral: true,
				}
			}
			stats := fieldStats[key]
			valStr := fmt.Sprintf("%v", value)
			if value != nil {
				if stats.uniqueValues[valStr] {
					stats.duplicateFound = true
				} else {
					stats.uniqueValues[valStr] = true
				}
			}
			if value == nil {
				stats.nullable = true
				continue
			}
			stats.countNonNull++
			switch v := value.(type) {
			case json.RawMessage:
				stats.typeCounts["json"]++
			case []byte:
				stats.typeCounts["bytes"]++
			case int, int8, int16, int32, int64:
				stats.typeCounts["int"]++
				stats.hasNumeric = true
			case uint, uint8, uint16, uint32, uint64:
				stats.typeCounts["int"]++
				stats.hasNumeric = true
			case float32:
				f := float64(v)
				stats.hasNumeric = true
				if f != math.Trunc(f) {
					stats.numericAllIntegral = false
					stats.typeCounts["float"]++
				} else {
					stats.typeCounts["int"]++
				}
			case float64:
				f := v
				stats.hasNumeric = true
				if f != math.Trunc(f) {
					stats.numericAllIntegral = false
					stats.typeCounts["float"]++
				} else {
					stats.typeCounts["int"]++
				}
			case json.Number:
				stats.typeCounts["json.Number"]++
				stats.hasNumeric = true
				if _, err := v.Int64(); err == nil {

				} else if f, err := v.Float64(); err == nil {
					if f != math.Trunc(f) {
						stats.numericAllIntegral = false
						stats.typeCounts["float"]++
					} else {
						stats.typeCounts["int"]++
					}
				}
			case bool:
				stats.typeCounts["bool"]++
			case time.Time:
				stats.typeCounts["time"]++
			case complex64, complex128:
				stats.typeCounts["complex"]++
			case string:
				stats.typeCounts["string"]++
				stats.totalStringCount++
				if len(v) > stats.maxStringLength {
					stats.maxStringLength = len(v)
				}
				if isStringTime(v) {
					stats.stringAsTimeCount++
				}
				if isStringComplex(v) {
					stats.stringAsComplexCount++
				}
				if _, err := strconv.ParseInt(v, 10, 64); err == nil {
					stats.stringAsIntCount++
					stats.hasNumeric = true
				} else if f, err := strconv.ParseFloat(v, 64); err == nil {
					if f != math.Trunc(f) {
						stats.stringAsFloatCount++
						stats.numericAllIntegral = false
					} else {
						stats.stringAsIntCount++
					}
					stats.hasNumeric = true
				}
			default:
				rv := reflect.ValueOf(v)
				switch rv.Kind() {
				case reflect.Slice, reflect.Array:
					stats.typeCounts["slice"]++
				case reflect.Map:
					stats.typeCounts["map"]++
				case reflect.Struct:
					stats.typeCounts["struct"]++
				default:
					stats.typeCounts[reflect.TypeOf(v).String()]++
				}
			}
		}
	}
	for field, stats := range fieldStats {
		finalType := "unknown"
		total := stats.countNonNull
		counts := stats.typeCounts
		timeCount := counts["time"] + stats.stringAsTimeCount
		if total > 0 && timeCount == total {
			updateSchema(schema, field, "time", stats)
			continue
		}
		complexCount := counts["complex"] + stats.stringAsComplexCount
		if total > 0 && complexCount == total {
			updateSchema(schema, field, "complex", stats)
			continue
		}
		numericCount := counts["int"] + counts["float"] + stats.stringAsIntCount + stats.stringAsFloatCount + counts["json.Number"]
		if total > 0 && numericCount == total {
			if stats.numericAllIntegral && stats.stringAsFloatCount == 0 {
				finalType = "int64"
			} else {
				finalType = "float64"
			}
		} else if cnt, ok := counts["bool"]; ok && cnt == total {
			finalType = "bool"
		} else if cnt, ok := counts["slice"]; ok && cnt == total {
			finalType = "slice"
		} else if cnt, ok := counts["map"]; ok && cnt == total {
			finalType = "map"
		} else if cnt, ok := counts["struct"]; ok && cnt == total {
			finalType = "struct"
		} else if cnt, ok := counts["string"]; ok && cnt == total {
			finalType = "string"
		} else if cnt, ok := counts["json"]; ok && cnt == total {
			finalType = "json"
		} else if cnt, ok := counts["bytes"]; ok && cnt == total {
			finalType = "bytes"
		} else {
			bestType := ""
			maxCount := 0
			for t, count := range counts {
				if count > maxCount {
					bestType = t
					maxCount = count
				}
			}
			finalType = bestType
		}
		updateSchema(schema, field, finalType, stats)
	}
	return schema
}

func SetPrimaryKey(schema map[string]FieldSchema, keys ...string) map[string]FieldSchema {
	if len(keys) == 0 {
		return schema
	}
	for field, s := range schema {
		if slices.Contains(keys, field) {
			s.IsPrimaryKey = true
		} else {
			s.IsPrimaryKey = false
		}
		schema[field] = s
	}
	return schema
}

func SetNullable(schema map[string]FieldSchema, keys ...string) map[string]FieldSchema {
	if len(keys) == 0 {
		return schema
	}
	for field, s := range schema {
		if slices.Contains(keys, field) {
			s.IsNullable = true
		}
		schema[field] = s
	}
	return schema
}

func MapDataTypeToDBType(field FieldSchema, driver string) string {
	var mapping map[string]string
	switch driver {
	case "mysql":
		mapping = mysqlDataTypes
	case "postgres":
		mapping = postgresDataTypes
	case "sqlite":
		mapping = sqliteDataTypes
	default:
		return "TEXT"
	}
	if field.DataType == "string" {
		if driver == "mysql" && field.MaxStringLength > 255 {
			return "LONGTEXT"
		}
		if driver == "postgres" && field.MaxStringLength > 255 {
			return "TEXT"
		}
	}
	if dbType, ok := mapping[field.DataType]; ok {
		return dbType
	}
	return "TEXT"
}

func main() {
	data := []map[string]any{
		{
			"id":             1,
			"timestamp":      time.Now(),
			"timestamp_str":  time.Now().Format(time.RFC3339),
			"score":          42.0,
			"rating":         3.5,
			"complex_val":    complex(2, 3),
			"complex_str":    "1+2i",
			"active":         true,
			"name":           "Alice",
			"json_number":    json.Number("12345"),
			"optional_field": nil,
		},
		{
			"id":             2,
			"timestamp":      time.Now(),
			"timestamp_str":  time.Now().Format(time.RFC3339),
			"score":          100.0,
			"rating":         4.7,
			"complex_val":    complex(3, 4),
			"complex_str":    "3+4i",
			"active":         false,
			"name":           "Bob",
			"json_number":    json.Number("67890"),
			"optional_field": "present",
		},
		{
			"id":            3,
			"timestamp":     time.Now(),
			"timestamp_str": time.Now().Format(time.RFC3339),
			"score":         "200",
			"rating":        "3.1415",
			"active":        "true",
			"name":          "Charlie",
		},
	}

	schema := DetectSchema(data, 5)
	schema = SetPrimaryKey(schema, "id")
	schema = SetNullable(schema, "rating")

	driver := "postgres"
	fmt.Printf("Mapping types for driver: %s\n", driver)
	for _, field := range schema {
		dbType := MapDataTypeToDBType(field, driver)
		fmt.Printf("Field: %-15s GoType: %-10s Nullable: %-5v PrimaryKey: %-5v MaxStrLen: %-4d -> DB Type: %s\n",
			field.FieldName, field.DataType, field.IsNullable, field.IsPrimaryKey, field.MaxStringLength, dbType)
	}
}
