package loader

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"

	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/utils"
)

type SQLLoader struct {
	db             *sql.DB
	table          string
	truncate       bool
	updateSequence bool
	destType       string
}

func NewSQLLoader(db *sql.DB, destType string, cfg config.TableMapping) *SQLLoader {
	return &SQLLoader{
		db:             db,
		destType:       destType,
		table:          cfg.NewName,
		truncate:       cfg.TruncateDestination,
		updateSequence: cfg.UpdateSequence,
	}
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

func (l *SQLLoader) StoreBatch(ctx context.Context, batch []utils.Record) error {
	if len(batch) == 0 {
		return nil
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
		l.table,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := l.db.ExecContext(ctx, q, args...)
	return err
}

func updateSequence(db *sql.DB, table string) error {
	seqName := fmt.Sprintf("%s_seq", table)
	q := fmt.Sprintf("SELECT setval('%s', (SELECT MAX(id) FROM %s))", seqName, table)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		log.Printf("Error updating sequence %s: %v", seqName, err)
		return err
	} else {
		log.Printf("Sequence %s updated", seqName)
	}
	return nil
}

func (l *SQLLoader) Close() error {
	if l.destType == "postgresql" && l.updateSequence {
		return updateSequence(l.db, l.table)
	}
	return nil
}

type KeyValueLoader struct {
	db             *sql.DB
	table          string
	keyField       string
	valueField     string
	extraValues    map[string]any
	includeFields  []string
	excludeFields  []string
	truncate       bool
	updateSequence bool
	destType       string
}

func NewKeyValueLoader(db *sql.DB, destType string, cfg config.TableMapping) *KeyValueLoader {
	return &KeyValueLoader{
		db:             db,
		destType:       destType,
		table:          cfg.NewName,
		keyField:       cfg.KeyField,
		valueField:     cfg.ValueField,
		extraValues:    cfg.ExtraValues,
		includeFields:  cfg.IncludeFields,
		excludeFields:  cfg.ExcludeFields,
		truncate:       cfg.TruncateDestination,
		updateSequence: cfg.UpdateSequence,
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

func (l *KeyValueLoader) StoreBatch(ctx context.Context, batch []utils.Record) error {
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
	if l.destType == "postgresql" && l.updateSequence {
		return updateSequence(l.db, l.table)
	}
	return nil
}
