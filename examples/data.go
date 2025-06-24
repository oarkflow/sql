package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/date"
)

// Data type mappings for different drivers.
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
	// Add a generic fallback mapping for unknown drivers.
	genericDataTypes = map[string]string{
		"int64":       "BIGINT",
		"int32":       "INTEGER",
		"uint32":      "INTEGER",
		"float64":     "DOUBLE",
		"bool":        "BOOLEAN",
		"time":        "TIMESTAMP",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "VARCHAR(255)",
		"json":        "JSON",
		"bytes":       "BLOB",
		"slice":       "JSON",
		"map":         "JSON",
		"struct":      "JSON",
		"json.Number": "BIGINT",
		"uuid":        "UUID",
	}
)

var globalWorkerCount int = 4

// FieldSchema holds the detected schema for a field.
type FieldSchema struct {
	FieldName       string   `json:"field_name"`
	DataType        string   `json:"data_type"`
	IsNullable      bool     `json:"is_nullable"`
	IsPrimaryKey    bool     `json:"is_primary_key"`
	MaxStringLength int      `json:"max_string_length"`
	MinValue        float64  `json:"min_value,omitempty"`
	MaxValue        float64  `json:"max_value,omitempty"`
	IsUnique        bool     `json:"is_unique"`
	EnumValues      []string `json:"enum_values,omitempty"`
	Comment         string   `json:"comment,omitempty"`
	ForeignKey      string   `json:"foreign_key,omitempty"`
}

// FieldStats aggregates statistics and heuristics for a field.
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
	mu                   sync.Mutex // protect concurrent updates
}

// Logging helper for warnings and errors.
func warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func dereference(value any) any {
	if value == nil || reflect.TypeOf(value) == nil {
		return nil
	}
	// Cache type for efficiency.
	typ := reflect.TypeOf(value)
	for value != nil && typ != nil && typ.Kind() == reflect.Ptr {
		v := reflect.ValueOf(value)
		if v.IsNil() {
			return nil
		}
		value = v.Elem().Interface()
		typ = reflect.TypeOf(value)
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

var uuidRegex = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

func isStringUUID(s string) bool {
	return uuidRegex.MatchString(s)
}

func updateNumericRange(stats *FieldStats, num float64) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
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
	minVal, maxVal := 0.0, 0.0
	if stats.hasNumeric && stats.numericInitialized {
		minVal = stats.minNumeric
		maxVal = stats.maxNumeric
	}
	schema[field] = FieldSchema{
		FieldName:       field,
		DataType:        finalType,
		IsNullable:      stats.nullable,
		IsPrimaryKey:    false,
		MaxStringLength: stats.maxStringLength,
		MinValue:        minVal,
		MaxValue:        maxVal,
	}
}

// DetectSchema processes the sample data concurrently to detect field schemas.
func DetectSchema(data []map[string]any, sampleSize int) map[string]FieldSchema {
	schema := make(map[string]FieldSchema)
	fieldStats := make(map[string]*FieldStats)
	var mu sync.Mutex

	// Determine effective sample size.
	totalSize := len(data)
	if sampleSize > totalSize {
		sampleSize = totalSize
	}

	workerCount := globalWorkerCount
	rowsCh := make(chan map[string]any, sampleSize)
	var wg sync.WaitGroup

	// Worker function to update fieldStats concurrently.
	worker := func() {
		defer wg.Done()
		for row := range rowsCh {
			for key, origValue := range row {
				value := dereference(origValue)
				mu.Lock()
				stats, exists := fieldStats[key]
				if !exists {
					stats = &FieldStats{
						uniqueValues:       make(map[string]bool, sampleSize),
						typeCounts:         make(map[string]int),
						numericAllIntegral: true,
					}
					fieldStats[key] = stats
				}
				mu.Unlock()

				valStr := fastString(value)
				stats.mu.Lock()
				if value != nil {
					if stats.uniqueValues[valStr] {
						stats.duplicateFound = true
					} else {
						stats.uniqueValues[valStr] = true
					}
				}
				stats.mu.Unlock()

				if value == nil {
					stats.mu.Lock()
					stats.nullable = true
					stats.mu.Unlock()
					continue
				}
				stats.mu.Lock()
				stats.countNonNull++
				stats.mu.Unlock()
				switch v := value.(type) {
				case json.RawMessage:
					stats.mu.Lock()
					stats.typeCounts["json"]++
					stats.mu.Unlock()
				case []byte:
					stats.mu.Lock()
					stats.typeCounts["bytes"]++
					stats.mu.Unlock()
				case int, int8, int16, int32, int64:
					stats.mu.Lock()
					stats.typeCounts["int"]++
					stats.hasNumeric = true
					stats.mu.Unlock()
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
					stats.mu.Lock()
					stats.typeCounts["int"]++
					stats.hasNumeric = true
					stats.mu.Unlock()
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
					stats.mu.Lock()
					stats.hasNumeric = true
					if f != math.Trunc(f) {
						stats.numericAllIntegral = false
						stats.typeCounts["float"]++
					} else {
						stats.typeCounts["int"]++
					}
					stats.mu.Unlock()
					updateNumericRange(stats, f)
				case float64:
					f := v
					stats.mu.Lock()
					stats.hasNumeric = true
					if f != math.Trunc(f) {
						stats.numericAllIntegral = false
						stats.typeCounts["float"]++
					} else {
						stats.typeCounts["int"]++
					}
					stats.mu.Unlock()
					updateNumericRange(stats, f)
				case json.Number:
					stats.mu.Lock()
					stats.typeCounts["json.Number"]++
					stats.hasNumeric = true
					stats.mu.Unlock()
					if i, err := v.Int64(); err == nil {
						updateNumericRange(stats, float64(i))
					} else if f, err := v.Float64(); err == nil {
						stats.mu.Lock()
						if f != math.Trunc(f) {
							stats.numericAllIntegral = false
							stats.typeCounts["float"]++
						} else {
							stats.typeCounts["int"]++
						}
						stats.mu.Unlock()
						updateNumericRange(stats, f)
					} else {
						warnf("Failed to parse json.Number: %v", v)
					}
				case bool:
					stats.mu.Lock()
					stats.typeCounts["bool"]++
					stats.mu.Unlock()
				case time.Time:
					stats.mu.Lock()
					stats.typeCounts["time"]++
					stats.mu.Unlock()
				case complex64, complex128:
					stats.mu.Lock()
					stats.typeCounts["complex"]++
					stats.mu.Unlock()
				case string:
					stats.mu.Lock()
					stats.typeCounts["string"]++
					stats.totalStringCount++
					if len(v) > stats.maxStringLength {
						stats.maxStringLength = len(v)
					}
					stats.mu.Unlock()
					if isStringTime(v) {
						stats.mu.Lock()
						stats.stringAsTimeCount++
						stats.mu.Unlock()
					}
					if isStringDate(v) {
						stats.mu.Lock()
						stats.stringAsDateCount++
						stats.mu.Unlock()
					}
					if isStringComplex(v) {
						stats.mu.Lock()
						stats.stringAsComplexCount++
						stats.mu.Unlock()
					}
					if ok, _ := isStringBool(v); ok {
						stats.mu.Lock()
						stats.stringAsBoolCount++
						stats.mu.Unlock()
					}
					if isJSONString(v) {
						stats.mu.Lock()
						stats.stringAsJsonCount++
						stats.mu.Unlock()
					}
					if isStringUUID(v) {
						stats.mu.Lock()
						stats.stringAsUUIDCount++
						stats.mu.Unlock()
					}
					if i, err := strconv.ParseInt(v, 10, 64); err == nil {
						stats.mu.Lock()
						stats.stringAsIntCount++
						stats.hasNumeric = true
						stats.mu.Unlock()
						updateNumericRange(stats, float64(i))
					} else if f, err := strconv.ParseFloat(v, 64); err == nil {
						stats.mu.Lock()
						if f != math.Trunc(f) {
							stats.stringAsFloatCount++
							stats.numericAllIntegral = false
						} else {
							stats.stringAsIntCount++
						}
						stats.hasNumeric = true
						stats.mu.Unlock()
						updateNumericRange(stats, f)
					}
				default:
					rv := reflect.ValueOf(v)
					switch rv.Kind() {
					case reflect.Slice, reflect.Array:
						stats.mu.Lock()
						stats.typeCounts["slice"]++
						stats.mu.Unlock()
					case reflect.Map:
						stats.mu.Lock()
						stats.typeCounts["map"]++
						stats.mu.Unlock()
					case reflect.Struct:
						stats.mu.Lock()
						stats.typeCounts["struct"]++
						stats.mu.Unlock()
					default:
						stats.mu.Lock()
						stats.typeCounts[reflect.TypeOf(v).String()]++
						stats.mu.Unlock()
					}
				}
			}
		}
	}

	// Start worker goroutines.
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	// Feed the rows into the channel.
	for i := 0; i < sampleSize; i++ {
		rowsCh <- data[i]
	}
	close(rowsCh)
	wg.Wait()

	// After workers, decide on final type for each field.
	for field, stats := range fieldStats {
		finalType := "unknown"
		total := stats.countNonNull
		counts := stats.typeCounts
		var enumValues []string
		if cnt, ok := counts["string"]; ok && cnt == total && len(stats.uniqueValues) < 50 {
			for v := range stats.uniqueValues {
				enumValues = append(enumValues, v)
			}
			sort.Strings(enumValues)
		}
		// Set uniqueness flag.
		unique := false
		if stats.countNonNull > 0 && !stats.duplicateFound {
			unique = true
		}
		schema[field] = FieldSchema{
			FieldName:       field,
			DataType:        finalType,
			IsNullable:      stats.nullable,
			IsPrimaryKey:    false,
			MaxStringLength: stats.maxStringLength,
			MinValue: func() float64 {
				if stats.numericInitialized {
					return stats.minNumeric
				} else {
					return 0
				}
			}(),
			MaxValue:   stats.maxNumeric,
			IsUnique:   unique,
			EnumValues: enumValues,
		}
	}
	return schema
}

// SetPrimaryKey marks the given keys as primary keys (supports composite PKs).
func SetPrimaryKey(schema map[string]FieldSchema, keys ...string) map[string]FieldSchema {
	if len(keys) == 0 {
		return schema
	}
	for field, s := range schema {
		if contains(keys, field) {
			s.IsPrimaryKey = true
		} else {
			s.IsPrimaryKey = false
		}
		schema[field] = s
	}
	return schema
}

// SetNullable sets fields as nullable.
func SetNullable(schema map[string]FieldSchema, keys ...string) map[string]FieldSchema {
	if len(keys) == 0 {
		return schema
	}
	for field, s := range schema {
		if contains(keys, field) {
			s.IsNullable = true
		}
		schema[field] = s
	}
	return schema
}

func contains(arr []string, s string) bool {
	for _, a := range arr {
		if a == s {
			return true
		}
	}
	return false
}

// Helper: convert CamelCase to snake_case.
func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		result = append(result, r)
	}
	return strings.ToLower(string(result))
}

// MapDataTypeToDBType maps the detected type to a database-specific type, with custom overrides.
// Now supports any RDBMS driver by falling back to genericDataTypes.
func MapDataTypeToDBType(field FieldSchema, driver string, overrides map[string]string) string {
	if overrides != nil {
		if t, ok := overrides[field.FieldName]; ok {
			return t
		}
	}
	var mapping map[string]string
	switch strings.ToLower(driver) {
	case "mysql":
		mapping = mysqlDataTypes
	case "postgres", "postgresql":
		mapping = postgresDataTypes
	case "sqlite", "sqlite3":
		mapping = sqliteDataTypes
	default:
		mapping = genericDataTypes
	}
	if field.DataType == "string" {
		if (driver == "mysql" && field.MaxStringLength > 255) ||
			(driver == "postgres" && field.MaxStringLength > 255) ||
			(driver == "postgresql" && field.MaxStringLength > 255) {
			return "TEXT"
		}
	}
	if dbType, ok := mapping[field.DataType]; ok {
		return dbType
	}
	return "TEXT"
}

// Data generation helper.
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
	// Added new CLI flags.
	var (
		totalRows     = flag.Int("rows", 1000000, "Total number of rows to generate")
		sampleSize    = flag.Int("sample", 100, "Sample size for schema detection")
		driver        = flag.String("driver", "postgres", "Database driver (mysql, postgres, sqlite, or any other RDBMS)")
		outJSON       = flag.Bool("json", false, "Output schema as JSON")
		flagWorkers   = flag.Int("workers", 4, "Number of concurrent workers for schema detection")
		tableName     = flag.String("table", "my_table", "Name of the table to generate SQL for")
		createSQL     = flag.Bool("createSQL", true, "Output CREATE TABLE SQL statement")
		useStdin      = flag.Bool("stdin", false, "Read input data from standard input (JSONL)")
		fieldsFlag    = flag.String("fields", "", "Comma separated list of fields to detect")
		dryRun        = flag.Bool("dry-run", false, "Only show type suggestions without generating SQL")
		typeOverride  = flag.String("type-override", "", "JSON object for custom type mapping, e.g. '{\"id\":\"UUID\"}'")
		fieldComments = flag.String("field-comments", "", "JSON object for field comments, e.g. '{\"id\":\"Primary key\"}'")
		configFile    = flag.String("config", "", "Path to config JSON file")
		sampleOut     = flag.Bool("sample-out", false, "Output a sample of generated data")
		foreignKeys   = flag.String("foreign-keys", "", "JSON object for foreign keys, e.g. '{\"user_id\":\"users(id)\"}'")
	)
	flag.Parse()

	// Set the global worker count.
	globalWorkerCount = *flagWorkers

	rand.Seed(time.Now().UnixNano())

	// Load config file if provided.
	typeOverrides := map[string]string{}
	fieldCommentMap := map[string]string{}
	foreignKeyMap := map[string]string{}
	if *configFile != "" {
		f, err := os.Open(*configFile)
		if err != nil {
			log.Fatalf("Failed to open config file: %v", err)
		}
		defer f.Close()
		var cfg struct {
			TypeOverride  map[string]string `json:"type_override"`
			FieldComments map[string]string `json:"field_comments"`
			ForeignKeys   map[string]string `json:"foreign_keys"`
		}
		if err := json.NewDecoder(f).Decode(&cfg); err != nil {
			log.Fatalf("Failed to parse config file: %v", err)
		}
		typeOverrides = cfg.TypeOverride
		fieldCommentMap = cfg.FieldComments
		foreignKeyMap = cfg.ForeignKeys
	}
	if *typeOverride != "" {
		_ = json.Unmarshal([]byte(*typeOverride), &typeOverrides)
	}
	if *fieldComments != "" {
		_ = json.Unmarshal([]byte(*fieldComments), &fieldCommentMap)
	}
	if *foreignKeys != "" {
		_ = json.Unmarshal([]byte(*foreignKeys), &foreignKeyMap)
	}

	var sampleData []map[string]any
	// If --stdin is provided, stream input data as JSONL.
	if *useStdin {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			var row map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &row); err == nil {
				sampleData = append(sampleData, row)
				if len(sampleData) >= *sampleSize {
					break
				}
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("Error reading stdin: %v", err)
		}
	} else {
		// Reservoir sampling for schema detection.
		sampleData = make([]map[string]any, 0, *sampleSize)
		log.Printf("Generating %d rows (reservoir sampling for %d samples)...", *totalRows, *sampleSize)
		for i := 0; i < *totalRows; i++ {
			row := generateRandomRow(i)
			// Filter by --fields if provided.
			if *fieldsFlag != "" {
				fields := strings.Split(*fieldsFlag, ",")
				trimmed := make(map[string]any)
				for _, f := range fields {
					f = strings.TrimSpace(f)
					if v, ok := row[f]; ok {
						trimmed[f] = v
					}
				}
				row = trimmed
			}
			if i < *sampleSize {
				sampleData = append(sampleData, row)
			} else {
				j := rand.Intn(i + 1)
				if j < *sampleSize {
					sampleData[j] = row
				}
			}
		}
	}

	if *sampleOut {
		out, err := json.MarshalIndent(sampleData, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal sample data: %v", err)
		}
		fmt.Println(string(out))
		return
	}

	log.Printf("Detecting schema based on a sample of %d rows...", len(sampleData))
	schema := DetectSchema(sampleData, len(sampleData))
	schema = SetPrimaryKey(schema, "id")
	schema = SetNullable(schema, "rating")

	// Apply field comments and foreign keys
	for k, v := range fieldCommentMap {
		if f, ok := schema[k]; ok {
			f.Comment = v
			schema[k] = f
		}
	}
	for k, v := range foreignKeyMap {
		if f, ok := schema[k]; ok {
			f.ForeignKey = v
			schema[k] = f
		}
	}

	if *dryRun {
		// Only output type suggestions.
		for _, field := range schema {
			fmt.Printf("Field: %s -> Suggested Type: %s\n", field.FieldName, field.DataType)
			if len(field.EnumValues) > 0 {
				fmt.Printf("  Enum values: %v\n", field.EnumValues)
			}
		}
		return
	}

	// Map the Go types to database types.
	if *outJSON {
		out, err := json.MarshalIndent(schema, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal schema: %v", err)
		}
		fmt.Println(string(out))
	} else if *createSQL {
		// Generate deterministic CREATE TABLE SQL statement.
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", *tableName))
		// Build a sorted slice of field names.
		var keys []string
		for k := range schema {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		first := true
		pkFields := []string{}
		for _, key := range keys {
			field := schema[key]
			if field.IsPrimaryKey {
				pkFields = append(pkFields, toSnakeCase(field.FieldName))
			}
		}
		for _, key := range keys {
			field := schema[key]
			if !first {
				sb.WriteString(",\n")
			}
			first = false
			dbType := MapDataTypeToDBType(field, *driver, typeOverrides)
			nullStr := "NOT NULL"
			if field.IsNullable {
				nullStr = "NULL"
			}
			colName := toSnakeCase(field.FieldName)
			commentStr := ""
			if field.Comment != "" {
				commentStr = fmt.Sprintf(" -- %s", field.Comment)
			}
			enumStr := ""
			if len(field.EnumValues) > 0 {
				enumStr = fmt.Sprintf(" /* enum: %v */", field.EnumValues)
			}
			fkStr := ""
			if field.ForeignKey != "" {
				fkStr = fmt.Sprintf(" REFERENCES %s", field.ForeignKey)
			}
			sb.WriteString(fmt.Sprintf("    %s %s %s%s%s%s", colName, dbType, nullStr, fkStr, enumStr, commentStr))
		}
		if len(pkFields) > 0 {
			sb.WriteString(fmt.Sprintf(",\n    PRIMARY KEY (%s)", strings.Join(pkFields, ", ")))
		}
		sb.WriteString("\n);")
		fmt.Println(sb.String())
		// Output CREATE INDEX for unique fields
		for _, key := range keys {
			field := schema[key]
			if field.IsUnique && !field.IsPrimaryKey {
				fmt.Printf("CREATE UNIQUE INDEX idx_%s_%s ON %s(%s);\n", *tableName, toSnakeCase(field.FieldName), *tableName, toSnakeCase(field.FieldName))
			}
		}
	} else {
		log.Printf("Mapping types for driver: %s", *driver)
		for _, field := range schema {
			dbType := MapDataTypeToDBType(field, *driver, typeOverrides)
			fmt.Printf("Field: %-15s GoType: %-10s Nullable: %-5v PrimaryKey: %-5v MaxStrLen: %-4d -> DB Type: %s\n",
				field.FieldName, field.DataType, field.IsNullable, field.IsPrimaryKey, field.MaxStringLength, dbType)
			if len(field.EnumValues) > 0 {
				fmt.Printf("  Enum values: %v\n", field.EnumValues)
			}
			if field.Comment != "" {
				fmt.Printf("  Comment: %s\n", field.Comment)
			}
			if field.ForeignKey != "" {
				fmt.Printf("  ForeignKey: %s\n", field.ForeignKey)
			}
		}
	}
}
