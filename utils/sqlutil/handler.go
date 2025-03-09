package sqlutil

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// --- Helper functions ---

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

func UpdateSequence(db *sql.DB, table string) error {
	seqName := fmt.Sprintf("%s_seq", table)
	q := fmt.Sprintf("SELECT setval('%s', (SELECT MAX(id) FROM %s))", seqName, table)
	if _, err := db.Exec(q); err != nil {
		log.Printf("Error updating sequence %s: %v", seqName, err)
		return err
	}
	log.Printf("Sequence %s updated", seqName)
	return nil
}

func CreateTableFromRecord(db *sql.DB, table string, rec map[string]interface{}) error {
	var columns []string
	var keys []string
	for k := range rec {
		keys = append(keys, k)
	}
	for _, k := range keys {
		sqlType := inferSQLType(rec[k])
		columns = append(columns, fmt.Sprintf("%s %s", k, sqlType))
	}
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", table, strings.Join(columns, ", "))
	_, err := db.Exec(createStmt)
	return err
}

func CreateKeyValueTable(db *sql.DB, tableName string, keyField, valueField string, truncate bool, extraValues map[string]any) error {
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

func inferSQLType(val interface{}) string {
	switch val.(type) {
	case int, int32, int64:
		return "INTEGER"
	case float32, float64:
		return "REAL"
	case bool:
		return "BOOLEAN"
	case string:
		return "TEXT"
	default:
		return "TEXT"
	}
}
