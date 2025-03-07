package sources

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/oarkflow/sql/v1/contracts"
)

type SQLSource struct {
	db    *sql.DB
	table string
	query string
}

func NewSQLSource(db *sql.DB, table, query string) *SQLSource {
	return &SQLSource{db: db, table: table, query: query}
}

func (s *SQLSource) Setup(ctx context.Context) error {
	return nil
}

func (s *SQLSource) Extract(ctx context.Context) (<-chan contracts.Record, error) {
	out := make(chan contracts.Record, 100)
	go func() {
		defer close(out)
		var q string
		if s.query != "" {
			q = s.query
		} else {
			q = fmt.Sprintf("SELECT * FROM %s", s.table)
		}
		rows, err := s.db.QueryContext(ctx, q)
		if err != nil {
			log.Printf("SQL query error: %v", err)
			return
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			log.Printf("Error getting columns: %v", err)
			return
		}
		for rows.Next() {
			columns := make([]any, len(cols))
			columnPointers := make([]any, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				log.Printf("Scan error: %v", err)
				continue
			}
			rec := make(contracts.Record)
			for i, colName := range cols {
				rec[colName] = columns[i]
			}
			out <- rec
		}
	}()
	return out, nil
}

func (s *SQLSource) Close() error {
	return nil
}

type CSVSource struct {
	fileName string
}

func NewCSVSource(fileName string) *CSVSource {
	return &CSVSource{fileName: fileName}
}

func (s *CSVSource) Setup(ctx context.Context) error {
	return nil
}

func (s *CSVSource) Extract(ctx context.Context) (<-chan contracts.Record, error) {
	out := make(chan contracts.Record, 100)
	go func() {
		defer close(out)
		file, err := os.Open(s.fileName)
		if err != nil {
			log.Printf("Error opening CSV file: %v", err)
			return
		}
		defer file.Close()
		reader := csv.NewReader(file)
		headers, err := reader.Read()
		if err != nil {
			log.Printf("Error reading CSV headers: %v", err)
			return
		}
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("CSV read error: %v", err)
				continue
			}
			rec := make(contracts.Record)
			for i, header := range headers {
				rec[header] = row[i]
			}
			out <- rec
		}
	}()
	return out, nil
}

func (s *CSVSource) Close() error {
	return nil
}

type JSONSource struct {
	fileName string
}

func NewJSONSource(fileName string) *JSONSource {
	return &JSONSource{fileName: fileName}
}

func (s *JSONSource) Setup(ctx context.Context) error {
	return nil
}

func (s *JSONSource) Extract(ctx context.Context) (<-chan contracts.Record, error) {
	out := make(chan contracts.Record, 100)
	go func() {
		defer close(out)
		file, err := os.Open(s.fileName)
		if err != nil {
			log.Printf("Error opening JSON file: %v", err)
			return
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			var rec contracts.Record
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				log.Printf("JSON unmarshal error: %v", err)
				continue
			}
			out <- rec
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Scanner error: %v", err)
		}
	}()
	return out, nil
}

func (s *JSONSource) Close() error {
	return nil
}
