package adapters

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
	"path/filepath"
	"strings"

	"github.com/oarkflow/etl/config"
	"github.com/oarkflow/etl/contract"
	"github.com/oarkflow/etl/utils"
	"github.com/oarkflow/etl/utils/fileutil"
	"github.com/oarkflow/etl/utils/sqlutil"
)

func NewLookupLoader(lkup config.DataConfig) (contract.LookupLoader, error) {
	switch strings.ToLower(lkup.Type) {
	case "postgresql", "mysql", "sqlite":
		db, err := config.OpenDB(lkup)
		if err != nil {
			log.Fatalf("Error connecting to lookup DB: %v", err)
		}
		return NewSQLAdapterAsSource(db, "", lkup.Source), nil
	case "csv", "json":
		return NewFileAdapter(lkup.File, "source", false), nil
	default:
		return nil, fmt.Errorf("Unsupported lookup type: %s", lkup.Type)
	}
}

type FileAdapter struct {
	mode            string
	Filename        string
	extension       string
	appendMode      bool
	file            *os.File
	bufWriter       *bufio.Writer
	csvWriter       *csv.Writer
	jsonFirstRecord bool
	csvHeader       []string
	headerWritten   bool
}

func NewFileAdapter(fileName, mode string, appendMode bool) *FileAdapter {
	extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
	return &FileAdapter{
		Filename:        fileName,
		extension:       extension,
		appendMode:      appendMode,
		mode:            mode,
		jsonFirstRecord: true,
	}
}

func (fl *FileAdapter) Setup(_ context.Context) error {
	if fl.mode != "loader" {
		return nil
	}
	switch fl.extension {
	case "json":
		if fl.appendMode {
			f, err := os.OpenFile(fl.Filename, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return fmt.Errorf("open file in append mode: %w", err)
			}
			fl.file = f
			info, err := f.Stat()
			if err != nil {
				return fmt.Errorf("stat file: %w", err)
			}
			if info.Size() == 0 {
				if _, err := f.WriteString("[\n"); err != nil {
					return fmt.Errorf("failed to write JSON array start: %w", err)
				}
				fl.jsonFirstRecord = true
			} else {
				if _, err := f.Seek(-2, io.SeekEnd); err != nil {
					return fmt.Errorf("failed to seek in file: %w", err)
				}
				buf := make([]byte, 2)
				if _, err := f.Read(buf); err != nil {
					return fmt.Errorf("failed to read trailing bytes: %w", err)
				}
				trimSize := int64(2)
				if buf[1] == '\n' {
					trimSize = 3
				}
				if err := f.Truncate(info.Size() - trimSize); err != nil {
					return fmt.Errorf("failed to truncate file: %w", err)
				}
				if _, err := f.Seek(0, io.SeekEnd); err != nil {
					return fmt.Errorf("failed to seek to end: %w", err)
				}
				if info.Size()-trimSize > int64(len("[\n")) {
					fl.jsonFirstRecord = false
				} else {
					fl.jsonFirstRecord = true
				}
			}
			fl.bufWriter = bufio.NewWriter(fl.file)
		} else {
			f, err := os.Create(fl.Filename)
			if err != nil {
				return fmt.Errorf("create file: %w", err)
			}
			fl.file = f
			fl.bufWriter = bufio.NewWriter(f)
			if _, err := fl.bufWriter.WriteString("[\n"); err != nil {
				return fmt.Errorf("failed to write JSON array start: %w", err)
			}
			fl.jsonFirstRecord = true
		}
	case "csv":
		var f *os.File
		var err error
		if fl.appendMode {
			f, err = os.OpenFile(fl.Filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				return fmt.Errorf("open CSV file in append mode: %w", err)
			}
			info, err := f.Stat()
			if err != nil {
				return fmt.Errorf("stat CSV file: %w", err)
			}
			if info.Size() > 0 {
				fl.headerWritten = true
			}
		} else {
			f, err = os.Create(fl.Filename)
			if err != nil {
				return fmt.Errorf("create CSV file: %w", err)
			}
		}
		fl.file = f
		fl.bufWriter = bufio.NewWriter(f)
		fl.csvWriter = csv.NewWriter(fl.bufWriter)
	default:
		return fmt.Errorf("unsupported file extension: %s", fl.extension)
	}
	return nil
}

func (fl *FileAdapter) StoreBatch(_ context.Context, records []utils.Record) error {
	switch fl.extension {
	case "json":
		for _, rec := range records {
			if !fl.jsonFirstRecord {
				if _, err := fl.bufWriter.WriteString(",\n"); err != nil {
					return fmt.Errorf("failed to write comma: %w", err)
				}
			}
			data, err := json.Marshal(rec)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON record: %w", err)
			}
			if _, err := fl.bufWriter.WriteString("\t" + string(data)); err != nil {
				return fmt.Errorf("failed to write JSON record: %w", err)
			}
			fl.jsonFirstRecord = false
		}
	case "csv":
		if len(records) > 0 {
			fl.csvHeader = fileutil.ExtractCSVHeader(records[0])
		}
		if !fl.headerWritten && len(records) > 0 {
			if err := fl.csvWriter.Write(fl.csvHeader); err != nil {
				return fmt.Errorf("failed to write CSV header: %w", err)
			}
			fl.headerWritten = true
		}
		for _, rec := range records {
			row, err := fileutil.BuildCSVRow(fl.csvHeader, rec)
			if err != nil {
				return fmt.Errorf("failed to build CSV row: %w", err)
			}
			if err := fl.csvWriter.Write(row); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
		fl.csvWriter.Flush()
		if err := fl.csvWriter.Error(); err != nil {
			return fmt.Errorf("csv writer flush error: %w", err)
		}
	default:
		return fmt.Errorf("unsupported file extension: %s", fl.extension)
	}
	return fl.bufWriter.Flush()
}

func (fl *FileAdapter) Close() error {
	if fl.extension == "json" && fl.bufWriter != nil {
		if _, err := fl.bufWriter.WriteString("\n]\n"); err != nil {
			return fmt.Errorf("failed to write JSON array close: %w", err)
		}
	}
	if fl.bufWriter != nil {
		if err := fl.bufWriter.Flush(); err != nil {
			return err
		}
	}
	if fl.file != nil {
		return fl.file.Close()
	}
	return nil
}

func (fl *FileAdapter) LoadData() ([]utils.Record, error) {
	ch, err := fl.Extract(context.Background())
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (fl *FileAdapter) Extract(_ context.Context) (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		_, err := fileutil.ProcessFile(fl.Filename, func(record utils.Record) {
			out <- record
		})
		if err != nil {
			log.Printf("File extraction error: %v", err)
		}
	}()
	return out, nil
}

type SQLAdapter struct {
	Db             *sql.DB
	mode           string
	Table          string
	truncate       bool
	updateSequence bool
	destType       string
	update         bool
	delete         bool
	query          string
	AutoCreate     bool
	Created        bool
}

func NewSQLAdapterAsLoader(db *sql.DB, destType string, cfg config.TableMapping) *SQLAdapter {
	autoCreate := false
	if !cfg.KeyValueTable && cfg.AutoCreateTable {
		autoCreate = true
	}
	return &SQLAdapter{
		Db:             db,
		destType:       destType,
		Table:          cfg.NewName,
		truncate:       cfg.TruncateDestination,
		updateSequence: cfg.UpdateSequence,
		update:         cfg.Update,
		delete:         cfg.Delete,
		query:          cfg.Query,
		AutoCreate:     autoCreate,
		Created:        false,
		mode:           "loader",
	}
}

func NewSQLAdapterAsSource(db *sql.DB, table, query string) *SQLAdapter {
	return &SQLAdapter{Db: db, Table: table, query: query}
}

func (l *SQLAdapter) Setup(ctx context.Context) error {
	if l.mode != "loader" {
		return nil
	}
	if l.truncate {
		_, err := l.Db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", l.Table))
		if err != nil {
			log.Printf("Truncate error for table %s: %v", l.Table, err)
		}
	}
	return nil
}

func (l *SQLAdapter) StoreBatch(ctx context.Context, batch []utils.Record) error {
	if l.update {
		for _, rec := range batch {
			var q string
			var args []any
			if l.query != "" {
				q = l.query
			} else {
				q, args = sqlutil.BuildUpdateStatement(l.Table, rec)
			}
			if _, err := l.Db.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		return nil
	}
	if l.delete {
		for _, rec := range batch {
			var q string
			var args []any
			if l.query != "" {
				q = l.query
			} else {
				q, args = sqlutil.BuildDeleteStatement(l.Table, rec)
			}
			if _, err := l.Db.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		return nil
	}
	if len(batch) == 0 {
		return nil
	}
	if l.AutoCreate && !l.Created {
		if err := sqlutil.CreateTableFromRecord(l.Db, l.Table, batch[0]); err != nil {
			return err
		}
		l.Created = true
	}
	var keys []string
	for k := range batch[0] {
		keys = append(keys, k)
	}
	var placeholders []string
	var args []any
	argCounter := 1
	for _, rec := range batch {
		var valPlaceholders []string
		for _, k := range keys {
			valPlaceholders = append(valPlaceholders, fmt.Sprintf("$%d", argCounter))
			args = append(args, rec[k])
			argCounter++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(valPlaceholders, ", ")))
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		l.Table,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := l.Db.ExecContext(ctx, q, args...)
	return err
}

func (l *SQLAdapter) LoadData() ([]utils.Record, error) {
	// Use the existing Extract() method to read records from the database.
	ch, err := l.Extract(context.Background())
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (l *SQLAdapter) Extract(ctx context.Context) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 100)
	go func() {
		defer close(out)
		var q string
		if l.query != "" {
			q = l.query
		} else {
			q = fmt.Sprintf("SELECT * FROM %s", l.Table)
		}
		rows, err := l.Db.QueryContext(ctx, q)
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

func (l *SQLAdapter) Close() error {
	if l.mode != "loader" {
		return l.Db.Close()
	}
	if l.destType == "postgresql" && l.updateSequence {
		return sqlutil.UpdateSequence(l.Db, l.Table)
	}
	return l.Db.Close()
}
