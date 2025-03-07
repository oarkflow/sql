package v1

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/oarkflow/sql/v1/config"
)

func OpenDB(cfg config.DataConfig) (*sql.DB, error) {
	var dsn string
	if cfg.Driver == "mysql" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	} else if cfg.Driver == "postgres" {
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)
	} else {
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Driver)
	}
	db, err := sql.Open(cfg.Driver, dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CreateTableFromCSV(destDB *sql.DB, csvFileName string, tableName string) error {
	f, err := os.Open(csvFileName)
	if err != nil {
		return fmt.Errorf("opening CSV file: %w", err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header: %w", err)
	}
	var columns []string
	for _, col := range headers {
		col = strings.TrimSpace(col)
		columns = append(columns, fmt.Sprintf("%s TEXT", col))
	}
	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))
	_, err = destDB.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	log.Printf("Table %s created (if not exists) with columns: %v", tableName, headers)
	return nil
}
