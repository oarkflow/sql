package sqlutil

import (
	"fmt"
	"log"
	"strings"

	"github.com/oarkflow/squealx"
)

func BuildUpdateStatement(table string, rec map[string]any) (string, []any) {
	var setParts []string
	var args []any
	var id any
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

func BuildDeleteStatement(table string, rec map[string]any) (string, []any) {
	id, ok := rec["id"]
	if !ok {
		return "", nil
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1", table)
	return q, []any{id}
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
		"any":         "TEXT",
		"interface{}": "TEXT",
		"text":        "TEXT",
		"pointer":     "TEXT",
		// SQL types (lowercase) - excluding duplicates with Go types
		"smallint":   "SMALLINT",
		"mediumint":  "MEDIUMINT",
		"bigint":     "BIGINT",
		"tinyint":    "TINYINT",
		"varchar":    "VARCHAR",
		"char":       "CHAR",
		"tinytext":   "TINYTEXT",
		"mediumtext": "MEDIUMTEXT",
		"longtext":   "LONGTEXT",
		"decimal":    "DECIMAL",
		"numeric":    "DECIMAL",
		"double":     "DOUBLE",
		"real":       "DOUBLE",
		"timestamp":  "TIMESTAMP",
		"datetime":   "DATETIME",
		"date":       "DATE",
		"time":       "TIME",
		"year":       "YEAR",
		"blob":       "BLOB",
		"tinyblob":   "TINYBLOB",
		"mediumblob": "MEDIUMBLOB",
		"longblob":   "LONGBLOB",
		"json":       "JSON",
		"uuid":       "CHAR(36)",
		"enum":       "ENUM",
		"set":        "SET",
		// Cross-driver types (PostgreSQL/SQLite to MySQL) - excluding duplicates
		"double precision":         "DOUBLE",
		"timestamp with time zone": "TIMESTAMP",
		"time with time zone":      "TIME",
		"interval":                 "VARCHAR(255)",
		"bytea":                    "BLOB",
		"jsonb":                    "JSON",
		"serial":                   "INT AUTO_INCREMENT",
		"bigserial":                "BIGINT AUTO_INCREMENT",
		"smallserial":              "SMALLINT AUTO_INCREMENT",
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
		"any":         "TEXT",
		"interface{}": "TEXT",
		"text":        "TEXT",
		"pointer":     "TEXT",
		// SQL types (lowercase) - excluding duplicates with Go types
		"smallint":                 "SMALLINT",
		"bigint":                   "BIGINT",
		"varchar":                  "VARCHAR",
		"char":                     "CHAR",
		"decimal":                  "DECIMAL",
		"numeric":                  "NUMERIC",
		"real":                     "REAL",
		"double precision":         "DOUBLE PRECISION",
		"timestamp":                "TIMESTAMP",
		"timestamp with time zone": "TIMESTAMP WITH TIME ZONE",
		"time":                     "TIME",
		"time with time zone":      "TIME WITH TIME ZONE",
		"date":                     "DATE",
		"interval":                 "INTERVAL",
		"bytea":                    "BYTEA",
		"json":                     "JSON",
		"jsonb":                    "JSONB",
		"uuid":                     "UUID",
		"serial":                   "SERIAL",
		"bigserial":                "BIGSERIAL",
		"smallserial":              "SMALLSERIAL",
		// Cross-driver types (MySQL/SQLite to PostgreSQL) - excluding duplicates
		"tinyint":    "SMALLINT",
		"mediumint":  "INTEGER",
		"double":     "DOUBLE PRECISION",
		"datetime":   "TIMESTAMP",
		"year":       "SMALLINT",
		"tinytext":   "TEXT",
		"mediumtext": "TEXT",
		"longtext":   "TEXT",
		"tinyblob":   "BYTEA",
		"mediumblob": "BYTEA",
		"longblob":   "BYTEA",
		"blob":       "BYTEA",
		"enum":       "VARCHAR(255)",
		"set":        "TEXT",
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
		"any":         "TEXT",
		"interface{}": "TEXT",
		"text":        "TEXT",
		"pointer":     "TEXT",
		// SQL types (lowercase) - excluding duplicates with Go types
		"smallint":         "INTEGER",
		"bigint":           "INTEGER",
		"varchar":          "TEXT",
		"char":             "TEXT",
		"decimal":          "REAL",
		"numeric":          "REAL",
		"real":             "REAL",
		"double precision": "REAL",
		"timestamp":        "DATETIME",
		"datetime":         "DATETIME",
		"date":             "DATE",
		"time":             "TIME",
		"blob":             "BLOB",
		"json":             "TEXT",
		"uuid":             "TEXT",
	}
)

func GetDataType(dataType, driver string) (string, error) {
	driver = strings.ToLower(driver)
	dataType = strings.ToLower(dataType) // Normalize to uppercase for matching

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
