package utils

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Field struct {
	Name       string `json:"name" gorm:"column:name"`
	OldName    string `json:"old_name" gorm:"column:old_name"`
	Key        string `json:"key" gorm:"column:key"`
	IsNullable string `json:"is_nullable" gorm:"column:is_nullable"`
	DataType   string `json:"type" gorm:"column:type"`
	Precision  int    `json:"precision" gorm:"column:precision"`
	Scale      int    `json:"scale" gorm:"column:scale"`
	Comment    string `json:"comment" gorm:"column:comment"`
	Default    any    `json:"default" gorm:"column:default"`
	Length     int    `json:"length" gorm:"column:length"`
	Extra      string `json:"extra" gorm:"column:extra"`
}

type TypeDetector struct {
	dbType string
}

func NewTypeDetector(dbType string) *TypeDetector {
	return &TypeDetector{dbType: strings.ToLower(dbType)}
}

// Enhanced type detection with better precision
func (td *TypeDetector) detectType(values []string, columnName string) Field {
	f := Field{
		Name:       columnName,
		IsNullable: "NO",
	}

	if strings.ToLower(columnName) == "id" {
		f.Key = "pri"
	}

	if len(values) == 0 {
		f.DataType = td.getDefaultStringType(255)
		f.IsNullable = "YES"
		return f
	}

	maxLen := 0
	maxIntegerPart := 0
	maxDecimalPart := 0
	nullCount := 0

	// Type tracking
	isInt, isFloat, isBool, isDate, isJSON, isUUID, isEmail, isURL := true, true, true, true, true, true, true, true
	intMin, intMax := int64(0), int64(0)

	// Enhanced date patterns
	datePatterns := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02",
		"01/02/2006",
		"02/01/2006",
		"2006-01-02 15:04:05",
		"01/02/2006 15:04:05",
		"02/01/2006 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"January 2, 2006",
		"Jan 2, 2006",
		"2006/01/02",
	}

	// Regular expressions for pattern matching
	uuidRegex := regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	urlRegex := regexp.MustCompile(`^https?://[^\s]+$`)

	for _, v := range values {
		v = strings.TrimSpace(v)

		// Handle null/empty values
		if v == "" || strings.ToLower(v) == "null" || strings.ToLower(v) == "n/a" {
			f.IsNullable = "YES"
			nullCount++
			continue
		}

		// Track max string length
		if len(v) > maxLen {
			maxLen = len(v)
		}

		// Integer detection
		if isInt {
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				if intMin == 0 && intMax == 0 {
					intMin, intMax = intVal, intVal
				} else {
					if intVal < intMin {
						intMin = intVal
					}
					if intVal > intMax {
						intMax = intVal
					}
				}
			} else {
				isInt = false
			}
		}

		// Float detection with precision tracking
		if isFloat {
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				// Calculate precision properly, handling scientific notation
				floatStr := strconv.FormatFloat(floatVal, 'f', -1, 64)
				if dotIndex := strings.Index(floatStr, "."); dotIndex != -1 {
					integerPart := len(strings.TrimLeft(floatStr[:dotIndex], "-"))
					decimalPart := len(floatStr[dotIndex+1:])
					if integerPart > maxIntegerPart {
						maxIntegerPart = integerPart
					}
					if decimalPart > maxDecimalPart {
						maxDecimalPart = decimalPart
					}
				} else {
					integerPart := len(strings.TrimLeft(floatStr, "-"))
					if integerPart > maxIntegerPart {
						maxIntegerPart = integerPart
					}
				}
				_ = floatVal
			} else {
				isFloat = false
			}
		}

		// Boolean detection (enhanced)
		if isBool {
			lv := strings.ToLower(v)
			validBools := map[string]bool{
				"true": true, "false": true, "yes": true, "no": true,
				"y": true, "n": true, "1": true, "0": true,
				"t": true, "f": true, "on": true, "off": true,
			}
			if !validBools[lv] {
				isBool = false
			}
		}

		// Date detection
		if isDate {
			validDate := false
			for _, pattern := range datePatterns {
				if _, err := time.Parse(pattern, v); err == nil {
					validDate = true
					break
				}
			}
			if !validDate {
				isDate = false
			}
		}

		// JSON detection
		if isJSON {
			var js interface{}
			if err := json.Unmarshal([]byte(v), &js); err != nil {
				isJSON = false
			}
		}

		// UUID detection
		if isUUID {
			if !uuidRegex.MatchString(v) {
				isUUID = false
			}
		}

		// Email detection
		if isEmail {
			if !emailRegex.MatchString(v) {
				isEmail = false
			}
		}

		// URL detection
		if isURL {
			if !urlRegex.MatchString(v) {
				isURL = false
			}
		}
	}

	f.Length = maxLen
	f.Precision = maxIntegerPart + maxDecimalPart
	f.Scale = maxDecimalPart

	// Enhanced type inference with database-specific mappings
	f.DataType = td.inferDataType(isInt, isFloat, isBool, isDate, isJSON, isUUID, isEmail, isURL,
		maxLen, maxIntegerPart, maxDecimalPart, intMin, intMax)

	if f.Key == "pri" && strings.Contains(strings.ToUpper(f.DataType), "INT") {
		f.Extra = "AUTO_INCREMENT"
	}

	return f
}

func (td *TypeDetector) inferDataType(isInt, isFloat, isBool, isDate, isJSON, isUUID, isEmail, isURL bool,
	maxLen, maxIntegerPart, maxDecimalPart int, intMin, intMax int64) string {

	switch {
	case isBool:
		return td.getBooleanType()

	case isUUID:
		return td.getUUIDType()

	case isInt:
		return td.getIntegerType(intMin, intMax)

	case isFloat:
		return td.getDecimalType(maxIntegerPart, maxDecimalPart)

	case isDate:
		return td.getDateTimeType()

	case isJSON:
		return td.getJSONType()

	case isEmail:
		return td.getStringType(320) // RFC standard max email length

	case isURL:
		return td.getStringType(2048) // Common URL length limit

	default:
		return td.getStringType(maxLen)
	}
}

// Database-specific type mappings
func (td *TypeDetector) getBooleanType() string {
	switch td.dbType {
	case "postgres", "postgresql":
		return "BOOLEAN"
	case "sqlite":
		return "INTEGER" // SQLite uses INTEGER for boolean
	default: // MySQL
		return "TINYINT(1)"
	}
}

func (td *TypeDetector) getUUIDType() string {
	switch td.dbType {
	case "postgres", "postgresql":
		return "UUID"
	case "sqlite":
		return "TEXT"
	default: // MySQL
		return "CHAR(36)"
	}
}

func (td *TypeDetector) getIntegerType(min, max int64) string {
	switch td.dbType {
	case "postgres", "postgresql":
		if min >= -128 && max <= 127 {
			return "SMALLINT"
		} else if min >= -32768 && max <= 32767 {
			return "SMALLINT"
		} else if min >= -2147483648 && max <= 2147483647 {
			return "INTEGER"
		} else {
			return "BIGINT"
		}
	case "sqlite":
		return "INTEGER"
	default: // MySQL
		if min >= -128 && max <= 127 {
			return "TINYINT"
		} else if min >= -32768 && max <= 32767 {
			return "SMALLINT"
		} else if min >= -8388608 && max <= 8388607 {
			return "MEDIUMINT"
		} else if min >= -2147483648 && max <= 2147483647 {
			return "INT"
		} else {
			return "BIGINT"
		}
	}
}

func (td *TypeDetector) getDecimalType(integerPart, decimalPart int) string {
	precision := integerPart + decimalPart
	if precision > 65 {
		precision = 65 // MySQL max precision
	}
	if decimalPart > 30 {
		decimalPart = 30 // MySQL max scale
	}

	switch td.dbType {
	case "postgres", "postgresql":
		if precision == 0 {
			return "NUMERIC"
		}
		return fmt.Sprintf("NUMERIC(%d,%d)", precision, decimalPart)
	case "sqlite":
		return "REAL"
	default: // MySQL
		if precision == 0 {
			return "DECIMAL"
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", precision, decimalPart)
	}
}

func (td *TypeDetector) getDateTimeType() string {
	switch td.dbType {
	case "postgres", "postgresql":
		return "TIMESTAMP WITH TIME ZONE"
	case "sqlite":
		return "DATETIME"
	default: // MySQL
		return "TIMESTAMP"
	}
}

func (td *TypeDetector) getJSONType() string {
	switch td.dbType {
	case "postgres", "postgresql":
		return "JSONB"
	case "sqlite":
		return "TEXT" // SQLite stores JSON as TEXT
	default: // MySQL
		return "JSON"
	}
}

func (td *TypeDetector) getStringType(length int) string {
	if length == 0 {
		length = 255
	}

	switch td.dbType {
	case "postgres", "postgresql":
		if length <= 255 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "TEXT"
	case "sqlite":
		return "TEXT"
	default: // MySQL
		if length <= 255 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		} else if length <= 65535 {
			return "TEXT"
		} else if length <= 16777215 {
			return "MEDIUMTEXT"
		} else {
			return "LONGTEXT"
		}
	}
}

func (td *TypeDetector) getDefaultStringType(length int) string {
	return td.getStringType(length)
}

// Additional methods for mandatory fields
func (td *TypeDetector) getTimestampType() string {
	switch td.dbType {
	case "postgres", "postgresql":
		return "TIMESTAMP WITH TIME ZONE"
	case "sqlite":
		return "DATETIME"
	default:
		return "TIMESTAMP"
	}
}

func (td *TypeDetector) getNowDefault() string {
	switch td.dbType {
	case "postgres", "postgresql":
		return "CURRENT_TIMESTAMP"
	case "sqlite":
		return "CURRENT_TIMESTAMP"
	default:
		return "CURRENT_TIMESTAMP"
	}
}

// Enhanced CSV parsing with better error handling
func parseCSV(filePath, dbType string) ([]Field, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	detector := NewTypeDetector(dbType)
	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	// Clean headers (remove BOM, trim spaces, handle special characters)
	for i, header := range headers {
		headers[i] = cleanColumnName(header)
	}

	// Initialize column storage
	cols := make([][]string, len(headers))
	rowCount := 0
	maxSampleRows := 1000 // Sample first 1000 rows for type detection

	// Read sample data
	for rowCount < maxSampleRows {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row %d: %w", rowCount+2, err)
		}

		// Handle rows with different column counts
		for i := 0; i < len(headers); i++ {
			if i < len(record) {
				cols[i] = append(cols[i], record[i])
			} else {
				cols[i] = append(cols[i], "") // Fill missing columns with empty string
			}
		}
		rowCount++
	}

	// Detect types for each column
	var fields []Field
	for i, col := range cols {
		field := detector.detectType(col, headers[i])
		fields = append(fields, field)
	}

	return fields, nil
}

// Enhanced JSON parsing
func parseJSON(filePath, dbType string) ([]Field, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var data []map[string]interface{}
	decoder := json.NewDecoder(file)

	// Read opening bracket
	_, err = decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}

	maxSampleObjects := 1000
	objectCount := 0

	for decoder.More() && objectCount < maxSampleObjects {
		var item map[string]interface{}
		if err := decoder.Decode(&item); err != nil {
			return nil, fmt.Errorf("failed to decode JSON object %d: %w", objectCount+1, err)
		}
		data = append(data, item)
		objectCount++
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("empty JSON array")
	}

	detector := NewTypeDetector(dbType)

	// Extract all unique column names
	columnMap := make(map[string][]string)
	for _, row := range data {
		for key, value := range row {
			cleanKey := cleanColumnName(key)
			var strValue string
			if value == nil {
				strValue = ""
			} else {
				strValue = fmt.Sprintf("%v", value)
			}
			columnMap[cleanKey] = append(columnMap[cleanKey], strValue)
		}
	}

	// Detect types for each column
	var fields []Field
	for columnName, values := range columnMap {
		field := detector.detectType(values, columnName)
		fields = append(fields, field)
	}

	return fields, nil
}

// Add mandatory fields if they don't exist
func addMandatoryFields(fields []Field, td *TypeDetector) []Field {
	mandatory := []Field{
		{Name: "created_at", DataType: td.getTimestampType(), IsNullable: "NO", Default: td.getNowDefault()},
		{Name: "updated_at", DataType: td.getTimestampType(), IsNullable: "NO", Default: td.getNowDefault()},
		{Name: "deleted_at", DataType: td.getTimestampType(), IsNullable: "YES", Default: nil},
		{Name: "is_active", DataType: td.getBooleanType(), IsNullable: "NO", Default: true},
		{Name: "status", DataType: td.getStringType(20), IsNullable: "YES", Default: "active"},
	}

	existing := make(map[string]bool)
	for _, f := range fields {
		existing[strings.ToLower(f.Name)] = true
	}

	for _, f := range mandatory {
		if !existing[strings.ToLower(f.Name)] {
			fields = append(fields, f)
		}
	}

	return fields
}

// Clean column names for database compatibility
func cleanColumnName(name string) string {
	// Remove BOM if present
	name = strings.TrimPrefix(name, "\ufeff")

	// Trim whitespace
	name = strings.TrimSpace(name)

	// Replace spaces and special characters with underscores
	reg := regexp.MustCompile(`[^\w]`)
	name = reg.ReplaceAllString(name, "_")

	// Remove consecutive underscores
	reg = regexp.MustCompile(`_{2,}`)
	name = reg.ReplaceAllString(name, "_")

	// Remove leading/trailing underscores
	name = strings.Trim(name, "_")

	// Ensure it doesn't start with a number
	if len(name) > 0 && unicode.IsDigit(rune(name[0])) {
		name = "col_" + name
	}

	// Handle empty names
	if name == "" {
		name = "unnamed_column"
	}

	return name
}

// Detect table name from file path
func detectTableName(filePath string) string {
	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return cleanColumnName(name)
}

// Generate CREATE TABLE statement
func generateCreateTable(tableName string, fields []Field, dbType string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	for i, field := range fields {
		dataType := field.DataType
		if field.Extra == "AUTO_INCREMENT" && dbType == "postgres" {
			if strings.Contains(dataType, "SMALLINT") {
				dataType = "SMALLSERIAL"
			} else if strings.Contains(dataType, "INTEGER") {
				dataType = "SERIAL"
			} else {
				dataType = "BIGSERIAL"
			}
		}
		sb.WriteString(fmt.Sprintf("    %s %s", field.Name, dataType))

		if field.Extra == "AUTO_INCREMENT" && dbType == "mysql" {
			sb.WriteString(" AUTO_INCREMENT")
		}

		if field.IsNullable == "NO" {
			sb.WriteString(" NOT NULL")
		}

		if field.Default != nil {
			switch v := field.Default.(type) {
			case bool:
				if v {
					sb.WriteString(" DEFAULT TRUE")
				} else {
					sb.WriteString(" DEFAULT FALSE")
				}
			case string:
				sb.WriteString(fmt.Sprintf(" DEFAULT %s", v))
			default:
				sb.WriteString(fmt.Sprintf(" DEFAULT %v", v))
			}
		}

		if i < len(fields)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	// Add primary key constraint
	var pk string
	for _, f := range fields {
		if f.Key == "pri" {
			if pk != "" {
				pk += ", "
			}
			pk += f.Name
		}
	}
	if pk != "" {
		sb.WriteString(fmt.Sprintf(",\n    PRIMARY KEY (%s)", pk))
	}

	sb.WriteString("\n);")
	return sb.String()
}

func parseFile(filePath, dbType string) ([]Field, error) {
	if strings.HasSuffix(strings.ToLower(filePath), ".csv") {
		return parseCSV(filePath, dbType)
	} else if strings.HasSuffix(strings.ToLower(filePath), ".json") {
		return parseJSON(filePath, dbType)
	} else {
		return nil, fmt.Errorf("unsupported file type: %s", filePath)
	}
}

func DetectSchema(tableName, filePath, dbType string, addMandatory ...bool) (string, []Field, error) {
	if tableName == "" {
		tableName = detectTableName(filePath)
	}
	fields, err := parseFile(filePath, dbType)
	if err != nil {
		return "", nil, err
	}

	if len(addMandatory) > 0 && addMandatory[0] {
		detector := NewTypeDetector(dbType)
		fields = addMandatoryFields(fields, detector)
	}

	createTableSQL := generateCreateTable(tableName, fields, dbType)
	return createTableSQL, fields, nil
}
