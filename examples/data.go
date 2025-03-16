package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/date"
	"github.com/oarkflow/json"
)

var (
	mysqlDataTypes = map[string]string{
		"int64":       "BIGINT",
		"int32":       "INT",
		"uint32":      "INT UNSIGNED",
		"float64":     "DOUBLE",
		"bool":        "BOOLEAN",
		"time":        "DATETIME",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "VARCHAR(255)",
		"json":        "JSON",
		"bytes":       "BLOB",
		"slice":       "JSON",
		"map":         "JSON",
		"struct":      "JSON",
		"json.Number": "BIGINT",
		"uuid":        "CHAR(36)",
	}

	postgresDataTypes = map[string]string{
		"int64":       "BIGINT",
		"int32":       "INTEGER",
		"uint32":      "INTEGER",
		"float64":     "DOUBLE PRECISION",
		"bool":        "BOOLEAN",
		"time":        "TIMESTAMP",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "VARCHAR(255)",
		"json":        "JSONB",
		"bytes":       "bytea",
		"slice":       "JSONB",
		"map":         "JSONB",
		"struct":      "JSONB",
		"json.Number": "BIGINT",
		"uuid":        "UUID",
	}

	sqliteDataTypes = map[string]string{
		"int64":       "INTEGER",
		"int32":       "INTEGER",
		"uint32":      "INTEGER",
		"float64":     "REAL",
		"bool":        "NUMERIC",
		"time":        "DATETIME",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "TEXT",
		"json":        "JSON",
		"bytes":       "BLOB",
		"slice":       "JSON",
		"map":         "JSON",
		"struct":      "JSON",
		"json.Number": "INTEGER",
		"uuid":        "TEXT",
	}
)

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
	minNumeric           float64
	maxNumeric           float64
	numericInitialized   bool
	stringAsBoolCount    int
	stringAsDateCount    int
	stringAsJsonCount    int
	stringAsUUIDCount    int
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

func fastString(value any) string {
	if value == nil {
		return "nil"
	}
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int8, int16, int32, int64:
		return strconv.FormatInt(reflect.ValueOf(v).Int(), 10)
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(reflect.ValueOf(v).Uint(), 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", value)
	}
}

func isStringTime(s string) bool {
	_, err := date.Parse(s)
	return err == nil
}

func isStringComplex(s string) bool {
	_, err := strconv.ParseComplex(s, 128)
	return err == nil
}

func isStringDate(s string) bool {
	if len(s) == 10 {
		_, err := time.Parse("2006-01-02", s)
		return err == nil
	}
	return false
}

func isStringBool(s string) (bool, error) {
	lower := strings.ToLower(strings.TrimSpace(s))
	if lower == "true" || lower == "false" {
		return true, nil
	}
	return false, errors.New("not a bool")
}

func isJSONString(s string) bool {
	start, end := 0, len(s)-1
	for start <= end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end >= start && (s[end] == ' ' || s[end] == '\t' || s[end] == '\n' || s[end] == '\r') {
		end--
	}
	if start > end {
		return false
	}
	if (s[start] == '{' && s[end] == '}') || (s[start] == '[' && s[end] == ']') {
		var js interface{}

		if err := json.Unmarshal([]byte(s[start:end+1]), &js); err == nil {
			return true
		}
	}
	return false
}

func isStringUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	return true
}

func updateNumericRange(stats *FieldStats, num float64) {
	if !stats.numericInitialized {
		stats.minNumeric = num
		stats.maxNumeric = num
		stats.numericInitialized = true
	} else {
		if num < stats.minNumeric {
			stats.minNumeric = num
		}
		if num > stats.maxNumeric {
			stats.maxNumeric = num
		}
	}
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

	var schema map[string]FieldSchema
	if len(data) > 0 {
		schema = make(map[string]FieldSchema, len(data[0]))
	} else {
		schema = make(map[string]FieldSchema)
	}
	fieldStats := make(map[string]*FieldStats, len(schema))
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
					uniqueValues:       make(map[string]bool, sampleSize),
					typeCounts:         make(map[string]int, 8),
					numericAllIntegral: true,
				}
			}
			stats := fieldStats[key]

			valStr := fastString(value)
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
				var num float64
				switch t := v.(type) {
				case int:
					num = float64(t)
				case int8:
					num = float64(t)
				case int16:
					num = float64(t)
				case int32:
					num = float64(t)
				case int64:
					num = float64(t)
				}
				updateNumericRange(stats, num)
			case uint, uint8, uint16, uint32, uint64:
				stats.typeCounts["int"]++
				stats.hasNumeric = true
				var num float64
				switch t := v.(type) {
				case uint:
					num = float64(t)
				case uint8:
					num = float64(t)
				case uint16:
					num = float64(t)
				case uint32:
					num = float64(t)
				case uint64:
					num = float64(t)
				}
				updateNumericRange(stats, num)
			case float32:
				f := float64(v)
				stats.hasNumeric = true
				if f != math.Trunc(f) {
					stats.numericAllIntegral = false
					stats.typeCounts["float"]++
				} else {
					stats.typeCounts["int"]++
				}
				updateNumericRange(stats, f)
			case float64:
				f := v
				stats.hasNumeric = true
				if f != math.Trunc(f) {
					stats.numericAllIntegral = false
					stats.typeCounts["float"]++
				} else {
					stats.typeCounts["int"]++
				}
				updateNumericRange(stats, f)
			case json.Number:
				stats.typeCounts["json.Number"]++
				stats.hasNumeric = true
				if i, err := v.Int64(); err == nil {
					updateNumericRange(stats, float64(i))
				} else if f, err := v.Float64(); err == nil {
					if f != math.Trunc(f) {
						stats.numericAllIntegral = false
						stats.typeCounts["float"]++
					} else {
						stats.typeCounts["int"]++
					}
					updateNumericRange(stats, f)
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
				if isStringDate(v) {
					stats.stringAsDateCount++
				}
				if isStringComplex(v) {
					stats.stringAsComplexCount++
				}
				if ok, _ := isStringBool(v); ok {
					stats.stringAsBoolCount++
				}
				if isJSONString(v) {
					stats.stringAsJsonCount++
				}
				if isStringUUID(v) {
					stats.stringAsUUIDCount++
				}
				if i, err := strconv.ParseInt(v, 10, 64); err == nil {
					stats.stringAsIntCount++
					stats.hasNumeric = true
					updateNumericRange(stats, float64(i))
				} else if f, err := strconv.ParseFloat(v, 64); err == nil {
					if f != math.Trunc(f) {
						stats.stringAsFloatCount++
						stats.numericAllIntegral = false
					} else {
						stats.stringAsIntCount++
					}
					stats.hasNumeric = true
					updateNumericRange(stats, f)
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
		numericCount := counts["int"] + counts["float"] + stats.stringAsIntCount + stats.stringAsFloatCount + counts["json.Number"]
		if total > 0 && (counts["time"]+stats.stringAsTimeCount == total) {
			finalType = "time"
		} else if total > 0 && (stats.stringAsDateCount == total) {
			finalType = "date"
		} else if total > 0 && (stats.stringAsUUIDCount == total) {
			finalType = "uuid"
		} else if total > 0 && (counts["complex"]+stats.stringAsComplexCount == total) {
			finalType = "complex"
		} else if total > 0 && (counts["bool"]+stats.stringAsBoolCount == total) {
			finalType = "bool"
		} else if total > 0 && numericCount == total {
			if stats.numericAllIntegral && stats.stringAsFloatCount == 0 {
				if stats.numericInitialized {
					if stats.minNumeric >= 0 && stats.maxNumeric <= float64(math.MaxUint32) {
						finalType = "uint32"
					} else if stats.minNumeric >= math.MinInt32 && stats.maxNumeric <= math.MaxInt32 {
						finalType = "int32"
					} else {
						finalType = "int64"
					}
				} else {
					finalType = "int64"
				}
			} else {
				finalType = "float64"
			}
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

var names = []string{"Alice", "Bob", "Charlie", "David", "Eve"}

func generateRandomRow(i int) map[string]any {
	row := make(map[string]any, 13)
	row["id"] = i + 1
	baseTime := time.Now().Add(-time.Hour * 24)
	randomDuration := time.Duration(rand.Intn(86400)) * time.Second
	ts := baseTime.Add(randomDuration)
	row["timestamp"] = ts
	row["timestamp_str"] = ts.Format(time.RFC3339)
	if rand.Float64() < 0.5 {
		row["score"] = float64(rand.Intn(1000))
	} else {
		row["score"] = strconv.Itoa(rand.Intn(1000))
	}
	if rand.Float64() < 0.5 {
		row["rating"] = float64(rand.Intn(50)) / 10.0
	} else {
		row["rating"] = fmt.Sprintf("%.4f", float64(rand.Intn(50))/10.0)
	}
	cv := complex(float64(rand.Intn(10)), float64(rand.Intn(10)))
	row["complex_val"] = cv
	row["complex_str"] = fmt.Sprintf("%.0f+%.0fi", real(cv), imag(cv))
	if rand.Float64() < 0.5 {
		row["active"] = true
	} else {
		row["active"] = "true"
	}
	row["name"] = names[rand.Intn(len(names))]
	row["json_number"] = json.Number(strconv.Itoa(rand.Intn(100000)))
	if rand.Float64() < 0.2 {
		row["optional_field"] = nil
	} else {
		row["optional_field"] = "present"
	}
	row["birth_date"] = ts.Add(-time.Hour * 24 * time.Duration(rand.Intn(365*30))).Format("2006-01-02")
	row["uuid_field"] = fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		rand.Uint32(),
		rand.Uint32()&0xffff,
		rand.Uint32()&0xffff,
		rand.Uint32()&0xffff,
		rand.Uint64()&0xffffffffffff,
	)
	return row
}

func main() {
	rand.Seed(time.Now().UnixNano())
	totalRows := 1000000
	data := make([]map[string]any, totalRows)
	fmt.Printf("Generating %d rows of data...\n", totalRows)
	for i := 0; i < totalRows; i++ {
		data[i] = generateRandomRow(i)
	}
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})
	sampleSize := 100
	sampleData := data[:sampleSize]
	fmt.Printf("Detecting schema based on a random sample of %d rows...\n", sampleSize)
	schema := DetectSchema(sampleData, sampleSize)
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
