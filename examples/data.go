package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/oarkflow/sql/pkg/utils"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run data.go <file.csv|file.json> <database_type> [table_name]")
		fmt.Println("Database types: mysql, postgres, sqlite")
		os.Exit(1)
	}

	filePath := os.Args[1]
	dbType := strings.ToLower(os.Args[2])
	tableName := ""
	if len(os.Args) > 3 {
		tableName = os.Args[3]
	}

	// Validate database type
	validDBTypes := map[string]bool{
		"mysql": true, "mariadb": true, "postgres": true, "postgresql": true, "sqlite": true, "sqlite3": true,
	}
	if !validDBTypes[dbType] {
		fmt.Printf("Unsupported database type: %s\n", dbType)
		fmt.Println("Supported types: mysql, mariadb, postgres, postgresql, sqlite, sqlite3")
		os.Exit(1)
	}
	createTableSQL, fields, err := utils.DetectSchema(tableName, filePath, dbType, true)
	if err != nil {
		fmt.Printf("Error detecting schema: %v\n", err)
		os.Exit(1)
	}
	// Output results
	fmt.Println("=== DETECTED FIELD TYPES ===")
	jsonOutput, _ := json.MarshalIndent(fields, "", "  ")
	fmt.Println(string(jsonOutput))

	fmt.Println("\n=== GENERATED CREATE TABLE STATEMENT ===")
	fmt.Println(createTableSQL)
}
