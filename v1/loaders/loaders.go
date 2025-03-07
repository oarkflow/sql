package loaders

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"

	"github.com/oarkflow/sql/v1/contracts"
)

type SQLLoader struct {
	db       *sql.DB
	table    string
	truncate bool
}

func NewSQLLoader(db *sql.DB, table string, truncate bool) *SQLLoader {
	return &SQLLoader{db: db, table: table, truncate: truncate}
}

func (l *SQLLoader) Setup(ctx context.Context) error {
	if l.truncate {
		_, err := l.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", l.table))
		if err != nil {
			log.Printf("Truncate error for table %s: %v", l.table, err)
		}
	}
	return nil
}

func (l *SQLLoader) LoadBatch(ctx context.Context, batch []contracts.Record) error {
	if len(batch) == 0 {
		return nil
	}
	keys := []string{}
	for k := range batch[0] {
		keys = append(keys, k)
	}
	placeholders := []string{}
	args := []any{}
	argCounter := 1
	for _, rec := range batch {
		valPlaceholders := []string{}
		for _, k := range keys {
			valPlaceholders = append(valPlaceholders, fmt.Sprintf("$%d", argCounter))
			args = append(args, rec[k])
			argCounter++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(valPlaceholders, ", ")))
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		l.table,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := l.db.ExecContext(ctx, q, args...)
	return err
}

func (l *SQLLoader) Close() error {
	return nil
}

type KeyValueLoader struct {
	db            *sql.DB
	table         string
	keyField      string
	valueField    string
	extraValues   map[string]any
	includeFields []string
	excludeFields []string
	truncate      bool
}

func NewKeyValueLoader(db *sql.DB, table, keyField, valueField string, extraValues map[string]any,
	includeFields, excludeFields []string, truncate bool) *KeyValueLoader {
	return &KeyValueLoader{
		db:            db,
		table:         table,
		keyField:      keyField,
		valueField:    valueField,
		extraValues:   extraValues,
		includeFields: includeFields,
		excludeFields: excludeFields,
		truncate:      truncate,
	}
}

func (l *KeyValueLoader) Setup(ctx context.Context) error {
	if l.truncate {
		_, err := l.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", l.table))
		if err != nil {
			log.Printf("Truncate error for table %s: %v", l.table, err)
		}
	}
	return nil
}

func (l *KeyValueLoader) LoadBatch(ctx context.Context, batch []contracts.Record) error {
	for _, rec := range batch {
		kv := make(map[string]any)
		for k, v := range l.extraValues {
			kv[k] = v
		}
		if len(l.includeFields) > 0 {
			for _, field := range l.includeFields {
				if val, ok := rec[field]; ok {
					kv[field] = val
				}
			}
		} else {
			for k, v := range rec {
				skip := false
				for _, ex := range l.excludeFields {
					if k == ex {
						skip = true
						break
					}
				}
				if !skip {
					kv[k] = v
				}
			}
		}
		jsonData, err := json.Marshal(kv)
		if err != nil {
			return fmt.Errorf("JSON marshal error: %w", err)
		}
		var key any
		if v, ok := rec["id"]; ok {
			key = v
		} else {
			key = rand.Int()
		}
		q := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES ($1, $2)", l.table, l.keyField, l.valueField)
		if _, err := l.db.ExecContext(ctx, q, key, string(jsonData)); err != nil {
			return fmt.Errorf("insert key-value error: %w", err)
		}
	}
	return nil
}

func (l *KeyValueLoader) Close() error {
	return nil
}

type JSONLoader struct {
	fileName    string
	file        *os.File
	mu          sync.Mutex
	firstRecord bool
}

func NewJSONLoader(fileName string) *JSONLoader {
	return &JSONLoader{
		fileName:    fileName,
		firstRecord: true,
	}
}

func (l *JSONLoader) Setup(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return nil
	}
	f, err := os.Create(l.fileName)
	if err != nil {
		return err
	}
	l.file = f
	_, err = l.file.Write([]byte("["))
	return err
}

func (l *JSONLoader) LoadBatch(ctx context.Context, batch []contracts.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, rec := range batch {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if !l.firstRecord {
			if _, err := l.file.Write([]byte(",")); err != nil {
				return err
			}
		}
		l.firstRecord = false
		if _, err := l.file.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (l *JSONLoader) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		if _, err := l.file.Write([]byte("]")); err != nil {
			return err
		}
		return l.file.Close()
	}
	return nil
}

type CSVLoader struct {
	fileName      string
	file          *os.File
	writer        *csv.Writer
	headerWritten bool
	mu            sync.Mutex
}

func NewCSVLoader(fileName string) *CSVLoader {
	return &CSVLoader{fileName: fileName}
}

func (l *CSVLoader) Setup(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return nil
	}
	f, err := os.Create(l.fileName)
	if err != nil {
		return err
	}
	l.file = f
	l.writer = csv.NewWriter(f)
	l.headerWritten = false
	return nil
}

func (l *CSVLoader) LoadBatch(ctx context.Context, batch []contracts.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	var header []string
	for k := range batch[0] {
		header = append(header, k)
	}
	if !l.headerWritten {
		if err := l.writer.Write(header); err != nil {
			return err
		}
		l.headerWritten = true
	}
	for _, rec := range batch {
		row := make([]string, len(header))
		for i, key := range header {
			row[i] = fmt.Sprintf("%v", rec[key])
		}
		if err := l.writer.Write(row); err != nil {
			return err
		}
	}
	l.writer.Flush()
	return l.writer.Error()
}

func (l *CSVLoader) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.writer.Flush()
		return l.file.Close()
	}
	return nil
}
