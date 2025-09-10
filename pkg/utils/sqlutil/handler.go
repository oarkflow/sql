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
		"bool":        "TINYINT(1)", // MySQL uses 0 or 1 for booleans
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
		"time.Time":   "DATETIME", // For time.Time, use DATETIME or TIMESTAMP
		"[]byte":      "BLOB",     // Use BLOB for slices or binary data
		"map":         "JSON",     // JSON objects
		"interface{}": "TEXT",     // Handle dynamic types as TEXT
		"any":         "TEXT",     // Handle dynamic types as TEXT
		"text":        "TEXT",     // Handle dynamic types as TEXT
		"pointer":     "TEXT",     // Pointers are treated like TEXT or BLOB
	}
	postgres = map[string]string{
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
		"time.Time":   "TIMESTAMP", // PostgreSQL uses TIMESTAMP for time.Time
		"[]byte":      "BYTEA",     // Use BYTEA for binary data
		"map":         "JSONB",     // JSONB for JSON objects
		"interface{}": "TEXT",      // Handle dynamic types as TEXT
		"any":         "TEXT",      // Handle dynamic types as TEXT
		"text":        "TEXT",      // Handle dynamic types as TEXT
		"pointer":     "TEXT",      // Pointers are treated like TEXT or BYTEA
	}
	sqlite = map[string]string{
		"bool":        "INTEGER", // SQLite uses INTEGER for booleans
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
		"time.Time":   "DATETIME", // Use DATETIME for time.Time
		"[]byte":      "BLOB",     // Use BLOB for binary data
		"map":         "TEXT",     // SQLite can store JSON as TEXT
		"interface{}": "TEXT",     // Interface as TEXT
		"any":         "TEXT",     // Handle dynamic types as TEXT
		"text":        "TEXT",     // Handle dynamic types as TEXT
		"pointer":     "TEXT",     // Pointers as TEXT
	}
)

func GetDataType(dataType, driver string) (string, error) {
	driver = strings.ToLower(driver)
	switch driver {
	case "mysql", "mariadb":
		if dataTypeSQL, ok := mysql[dataType]; ok {
			return dataTypeSQL, nil
		}
	case "postgres", "postgresql", "pgx4", "pgx5":
		if dataTypeSQL, ok := postgres[dataType]; ok {
			return dataTypeSQL, nil
		}
	case "sqlite", "sqlite3":
		if dataTypeSQL, ok := sqlite[dataType]; ok {
			return dataTypeSQL, nil
		}
	}
	return "", fmt.Errorf("Unknown data type: %s for driver: %s", dataType, driver)
}
