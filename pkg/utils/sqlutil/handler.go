package sqlutil

import (
	"fmt"
	"log"
	"strings"

	"github.com/oarkflow/squealx"
)

func BuildUpdateStatement(table string, rec map[string]interface{}) (string, []interface{}) {
	var setParts []string
	var args []interface{}
	var id interface{}
	for k, v := range rec {
		if strings.ToLower(k) == "id" {
			id = v
			continue
		}
		args = append(args, v)
		setParts = append(setParts, fmt.Sprintf("%s = $%d", k, len(args)))
	}
	if id == nil {
		return "", nil
	}
	args = append(args, id)
	q := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", table, strings.Join(setParts, ", "), len(args))
	return q, args
}

func BuildDeleteStatement(table string, rec map[string]interface{}) (string, []interface{}) {
	id, ok := rec["id"]
	if !ok {
		return "", nil
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1", table)
	return q, []interface{}{id}
}

func UpdateSequence(db *squealx.DB, table string) error {
	seqName := fmt.Sprintf("%s_seq", table)
	q := fmt.Sprintf("SELECT setval('%s', (SELECT MAX(id) FROM %s))", seqName, table)
	if _, err := db.Exec(q); err != nil {
		log.Printf("Error updating sequence %s: %v", seqName, err)
		return err
	}
	log.Printf("Sequence %s updated", seqName)
	return nil
}

func CreateTableFromRecord(db *squealx.DB, driver, table string, schema map[string]string) error {
	if len(schema) == 0 {
		return fmt.Errorf("schema cannot be empty for table %s", table)
	}
	var columns []string
	var keys []string
	for k := range schema {
		keys = append(keys, k)
	}
	for _, k := range keys {
		sqlType, err := GetDataType(schema[k], driver)
		if err != nil {
			return err
		}
		columns = append(columns, fmt.Sprintf("%s %s", k, sqlType))
	}
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", table, strings.Join(columns, ", "))
	_, err := db.Exec(createStmt)
	return err
}

func CreateKeyValueTable(db *squealx.DB, tableName string, keyField, valueField string, truncate bool, extraValues map[string]any) error {
	columns := []string{
		fmt.Sprintf("%s TEXT", keyField),
	}
	for extraField := range extraValues {
		columns = append(columns, fmt.Sprintf("%s TEXT", extraField))
	}
	columns = append(columns, fmt.Sprintf("%s TEXT", valueField))
	columns = append(columns, "value_type TEXT")
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))
	_, err := db.Exec(createStmt)
	if err != nil {
		return err
	}
	if truncate {
		_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	mysql = map[string]string{
		// Go types
		"bool":        "TINYINT(1)",
		"int":         "INT",
		"integer":     "INT",
		"int8":        "TINYINT",
		"int16":       "SMALLINT",
		"int32":       "INT",
		"int64":       "BIGINT",
		"uint":        "INT UNSIGNED",
		"uint8":       "TINYINT UNSIGNED",
		"uint16":      "SMALLINT UNSIGNED",
		"uint32":      "INT UNSIGNED",
		"uint64":      "BIGINT UNSIGNED",
		"float":       "FLOAT",
		"float32":     "FLOAT",
		"float64":     "DOUBLE",
		"string":      "VARCHAR(255)",
		"time.Time":   "DATETIME",
		"[]byte":      "BLOB",
		"map":         "JSON",
		"interface{}": "TEXT",
		"any":         "TEXT",
		"text":        "TEXT",
		"pointer":     "TEXT",
		// SQL types
		"SMALLINT":   "SMALLINT",
		"MEDIUMINT":  "MEDIUMINT",
		"BIGINT":     "BIGINT",
		"TINYINT":    "TINYINT",
		"VARCHAR":    "VARCHAR",
		"CHAR":       "CHAR",
		"TEXT":       "TEXT",
		"TINYTEXT":   "TINYTEXT",
		"MEDIUMTEXT": "MEDIUMTEXT",
		"LONGTEXT":   "LONGTEXT",
		"DECIMAL":    "DECIMAL",
		"NUMERIC":    "DECIMAL",
		"FLOAT":      "FLOAT",
		"DOUBLE":     "DOUBLE",
		"REAL":       "DOUBLE",
		"BOOLEAN":    "TINYINT(1)",
		"BOOL":       "TINYINT(1)",
		"TIMESTAMP":  "TIMESTAMP",
		"DATETIME":   "DATETIME",
		"DATE":       "DATE",
		"TIME":       "TIME",
		"YEAR":       "YEAR",
		"BLOB":       "BLOB",
		"TINYBLOB":   "TINYBLOB",
		"MEDIUMBLOB": "MEDIUMBLOB",
		"LONGBLOB":   "LONGBLOB",
		"JSON":       "JSON",
		"UUID":       "CHAR(36)",
		"ENUM":       "ENUM",
		"SET":        "SET",
		// Cross-driver types (PostgreSQL/SQLite to MySQL)
		"INTEGER":                  "INT",
		"DOUBLE PRECISION":         "DOUBLE",
		"TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
		"TIME WITH TIME ZONE":      "TIME",
		"INTERVAL":                 "VARCHAR(255)",
		"BYTEA":                    "BLOB",
		"JSONB":                    "JSON",
		"SERIAL":                   "INT AUTO_INCREMENT",
		"BIGSERIAL":                "BIGINT AUTO_INCREMENT",
		"SMALLSERIAL":              "SMALLINT AUTO_INCREMENT",
	}
	postgres = map[string]string{
		// Go types
		"bool":        "BOOLEAN",
		"int":         "INTEGER",
		"integer":     "INTEGER",
		"int8":        "SMALLINT",
		"int16":       "SMALLINT",
		"int32":       "INTEGER",
		"int64":       "BIGINT",
		"uint":        "BIGINT",
		"uint8":       "SMALLINT",
		"uint16":      "INTEGER",
		"uint32":      "BIGINT",
		"uint64":      "BIGINT",
		"float":       "REAL",
		"float32":     "REAL",
		"float64":     "DOUBLE PRECISION",
		"string":      "VARCHAR(255)",
		"time.Time":   "TIMESTAMP",
		"[]byte":      "BYTEA",
		"map":         "JSONB",
		"interface{}": "TEXT",
		"any":         "TEXT",
		"text":        "TEXT",
		"pointer":     "TEXT",
		// SQL types
		"SMALLINT":                 "SMALLINT",
		"INTEGER":                  "INTEGER",
		"BIGINT":                   "BIGINT",
		"VARCHAR":                  "VARCHAR",
		"CHAR":                     "CHAR",
		"TEXT":                     "TEXT",
		"DECIMAL":                  "DECIMAL",
		"NUMERIC":                  "NUMERIC",
		"REAL":                     "REAL",
		"DOUBLE PRECISION":         "DOUBLE PRECISION",
		"BOOLEAN":                  "BOOLEAN",
		"BOOL":                     "BOOLEAN",
		"TIMESTAMP":                "TIMESTAMP",
		"TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
		"TIME":                     "TIME",
		"TIME WITH TIME ZONE":      "TIME WITH TIME ZONE",
		"DATE":                     "DATE",
		"INTERVAL":                 "INTERVAL",
		"BYTEA":                    "BYTEA",
		"JSON":                     "JSON",
		"JSONB":                    "JSONB",
		"UUID":                     "UUID",
		"SERIAL":                   "SERIAL",
		"BIGSERIAL":                "BIGSERIAL",
		"SMALLSERIAL":              "SMALLSERIAL",
		// Cross-driver types (MySQL/SQLite to PostgreSQL)
		"INT":        "INTEGER",
		"TINYINT":    "SMALLINT",
		"MEDIUMINT":  "INTEGER",
		"DOUBLE":     "DOUBLE PRECISION",
		"FLOAT":      "REAL",
		"DATETIME":   "TIMESTAMP",
		"YEAR":       "SMALLINT",
		"TINYTEXT":   "TEXT",
		"MEDIUMTEXT": "TEXT",
		"LONGTEXT":   "TEXT",
		"TINYBLOB":   "BYTEA",
		"MEDIUMBLOB": "BYTEA",
		"LONGBLOB":   "BYTEA",
		"BLOB":       "BYTEA",
		"ENUM":       "VARCHAR(255)",
		"SET":        "TEXT",
	}
	sqlite = map[string]string{
		// Go types
		"bool":        "INTEGER",
		"int":         "INTEGER",
		"integer":     "INTEGER",
		"int8":        "INTEGER",
		"int16":       "INTEGER",
		"int32":       "INTEGER",
		"int64":       "INTEGER",
		"uint":        "INTEGER",
		"uint8":       "INTEGER",
		"uint16":      "INTEGER",
		"uint32":      "INTEGER",
		"uint64":      "INTEGER",
		"float":       "REAL",
		"float32":     "REAL",
		"float64":     "REAL",
		"string":      "TEXT",
		"time.Time":   "DATETIME",
		"[]byte":      "BLOB",
		"map":         "TEXT",
		"interface{}": "TEXT",
		"any":         "TEXT",
		"text":        "TEXT",
		"pointer":     "TEXT",
		// SQL types
		"SMALLINT":         "INTEGER",
		"INTEGER":          "INTEGER",
		"BIGINT":           "INTEGER",
		"VARCHAR":          "TEXT",
		"CHAR":             "TEXT",
		"TEXT":             "TEXT",
		"DECIMAL":          "REAL",
		"NUMERIC":          "REAL",
		"REAL":             "REAL",
		"DOUBLE PRECISION": "REAL",
		"BOOLEAN":          "INTEGER",
		"BOOL":             "INTEGER",
		"TIMESTAMP":        "DATETIME",
		"DATETIME":         "DATETIME",
		"DATE":             "DATE",
		"TIME":             "TIME",
		"BLOB":             "BLOB",
		"JSON":             "TEXT",
		"UUID":             "TEXT",
	}
)

func GetDataType(dataType, driver string) (string, error) {
	driver = strings.ToLower(driver)
	dataType = strings.ToUpper(dataType) // Normalize to uppercase for matching

	var baseType string
	var params string

	if idx := strings.Index(dataType, "("); idx != -1 {
		baseType = strings.TrimSpace(dataType[:idx])
		params = dataType[idx:]
	} else {
		baseType = strings.TrimSpace(dataType)
	}

	var typeMap map[string]string
	switch driver {
	case "mysql", "mariadb":
		typeMap = mysql
	case "postgres", "postgresql", "pgx4", "pgx5":
		typeMap = postgres
	case "sqlite", "sqlite3":
		typeMap = sqlite
	default:
		return "", fmt.Errorf("unsupported driver: %s", driver)
	}

	if sqlType, ok := typeMap[baseType]; ok {
		if params != "" {
			return sqlType + params, nil
		}
		return sqlType, nil
	}

	// If not found in map, assume it's already a valid SQL type
	return strings.ToLower(dataType), nil
}
