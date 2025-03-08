package source

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/oarkflow/sql/utils"
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

func (s *SQLSource) Extract(ctx context.Context) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 100)
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
			rec := make(utils.Record)
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
