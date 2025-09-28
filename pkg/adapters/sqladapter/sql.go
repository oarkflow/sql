package sqladapter

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	
	"github.com/oarkflow/log"
	"github.com/oarkflow/squealx"
	
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/sql/pkg/utils/sqlutil"
)

type Adapter struct {
	Db                  *squealx.DB
	mode                string
	Table               string
	truncate            bool
	updateSequence      bool
	destType            string
	update              bool
	delete              bool
	query               string
	AutoCreate          bool
	Created             bool
	Driver              string
	NormalizeSchema     map[string]string
	disabledForeignKeys bool
	disabledIndexes     bool
}

// NewLoader creates a new adapter loader using squealx.
func NewLoader(db *squealx.DB, destType, driver string, cfg config.TableMapping, normalizeSchema map[string]string) *Adapter {
	autoCreate := false
	if !cfg.KeyValueTable && cfg.AutoCreateTable {
		autoCreate = true
	}
	return &Adapter{
		Db:              db,
		destType:        destType,
		Table:           cfg.NewName,
		truncate:        cfg.TruncateDestination,
		updateSequence:  cfg.UpdateSequence,
		update:          cfg.Update,
		delete:          cfg.Delete,
		query:           cfg.Query,
		AutoCreate:      autoCreate,
		Created:         false,
		Driver:          driver,
		NormalizeSchema: normalizeSchema,
		mode:            "loader",
	}
}

// NewSource creates a new source adapter using squealx.
func NewSource(db *squealx.DB, table, query string) *Adapter {
	return &Adapter{Db: db, Table: table, query: query}
}

func (l *Adapter) Setup(ctx context.Context) error {
	if l.mode != "loader" {
		return nil
	}
	if l.truncate {
		exists, err := tableExists(l.Db, l.Table, l.Driver)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		var truncateSQL string
		switch l.Driver {
		case "postgres", "mysql":
			truncateSQL = fmt.Sprintf("TRUNCATE TABLE %s", l.Table)
		case "sqlite", "sqlite3":
			truncateSQL = fmt.Sprintf("DELETE FROM %s", l.Table)
		default:
			return fmt.Errorf("unsupported driver: %s", l.Driver)
		}
		_, err = l.Db.ExecContext(ctx, truncateSQL)
		if err != nil {
			return fmt.Errorf("truncate error for table %s: %v", l.Table, err)
		}
	}
	// Disable foreign keys and indexes for performance during ETL
	if err := l.disableConstraints(ctx); err != nil {
		return fmt.Errorf("failed to disable constraints: %v", err)
	}
	return nil
}

func (l *Adapter) disableConstraints(ctx context.Context) error {
	if l.mode != "loader" {
		return nil
	}
	// Disable foreign keys
	var fkSQL string
	switch l.Driver {
	case "mysql":
		fkSQL = "SET FOREIGN_KEY_CHECKS = 0"
	case "postgres":
		fkSQL = "SET session_replication_role = 'replica'"
	case "sqlite", "sqlite3":
		fkSQL = "PRAGMA foreign_keys = OFF"
	default:
		return nil // Skip if unsupported
	}
	if _, err := l.Db.ExecContext(ctx, fkSQL); err != nil {
		return fmt.Errorf("failed to disable foreign keys: %v", err)
	}
	l.disabledForeignKeys = true
	
	// Disable indexes for MySQL
	if l.Driver == "mysql" {
		idxSQL := fmt.Sprintf("ALTER TABLE %s DISABLE KEYS", l.Table)
		if _, err := l.Db.ExecContext(ctx, idxSQL); err != nil {
			return fmt.Errorf("failed to disable indexes: %v", err)
		}
		l.disabledIndexes = true
	}
	return nil
}

func (l *Adapter) enableConstraints(ctx context.Context) error {
	// Enable indexes first for MySQL
	if l.disabledIndexes && l.Driver == "mysql" {
		idxSQL := fmt.Sprintf("ALTER TABLE %s ENABLE KEYS", l.Table)
		if _, err := l.Db.ExecContext(ctx, idxSQL); err != nil {
			return fmt.Errorf("failed to enable indexes: %v", err)
		}
		l.disabledIndexes = false
	}
	
	// Enable foreign keys
	if l.disabledForeignKeys {
		var fkSQL string
		switch l.Driver {
		case "mysql":
			fkSQL = "SET FOREIGN_KEY_CHECKS = 1"
		case "postgres":
			fkSQL = "SET session_replication_role = 'origin'"
		case "sqlite", "sqlite3":
			fkSQL = "PRAGMA foreign_keys = ON"
		default:
			return nil
		}
		if _, err := l.Db.ExecContext(ctx, fkSQL); err != nil {
			return fmt.Errorf("failed to enable foreign keys: %v", err)
		}
		l.disabledForeignKeys = false
	}
	return nil
}

func tableExists(db *squealx.DB, tableName, dbType string) (bool, error) {
	var count int
	var query string
	switch dbType {
	case "mysql", "postgres":
		query = fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", tableName)
	case "sqlite":
		query = fmt.Sprintf("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'", tableName)
	default:
		return false, fmt.Errorf("unsupported DBMS type: %s", dbType)
	}
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (l *Adapter) StoreBatch(ctx context.Context, batch []utils.Record) error {
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
		if err := sqlutil.CreateTableFromRecord(l.Db, l.Driver, l.Table, l.NormalizeSchema); err != nil {
			return err
		}
		l.Created = true
	}
	var keys []string
	for k := range batch[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var placeholders []string
	var valPlaceholders []string
	for _, k := range keys {
		valPlaceholders = append(valPlaceholders, fmt.Sprintf(":%s", k))
	}
	placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(valPlaceholders, ", ")))
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", l.Table, strings.Join(keys, ", "), strings.Join(placeholders, ", "))
	_, err := l.Db.NamedExec(q, batch)
	return err
}

// Refactored StoreSingle using ExecWithReturning
func (l *Adapter) StoreSingle(ctx context.Context, rec utils.Record) error {
	
	if l.AutoCreate && !l.Created {
		if err := sqlutil.CreateTableFromRecord(l.Db, l.Driver, l.Table, l.NormalizeSchema); err != nil {
			return err
		}
		l.Created = true
	}
	var keys []string
	for k := range rec {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var placeholders []string
	argCounter := 1
	for _, k := range keys {
		placeholders = append(placeholders, fmt.Sprintf(":%s", k))
		argCounter++
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		l.Table,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	)
	// Use ExecWithReturning with pointer to args as required.
	return l.Db.ExecWithReturn(q, &rec)
}

func (l *Adapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := l.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (l *Adapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	opt := &contracts.SourceOption{Table: "", Query: ""}
	for _, op := range opts {
		op(opt)
	}
	table, query := l.Table, l.query
	if opt.Table != "" {
		table = opt.Table
	}
	if opt.Query != "" {
		query = opt.Query
	}
	var q string
	if query != "" {
		q = query
	} else {
		q = fmt.Sprintf("SELECT * FROM %s", table)
	}
	out := make(chan utils.Record, 100)
	go func(query string) {
		defer close(out)
		var rows squealx.SQLRows
		var err error
		if len(opt.Args) > 0 {
			rows, err = l.Db.QueryContext(ctx, q, opt.Args...)
		} else {
			rows, err = l.Db.QueryContext(ctx, q)
		}
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
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			log.Printf("Error getting column types: %v", err)
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
				var val any
				if b, ok := columns[i].([]byte); ok {
					if b == nil {
						val = nil
					} else {
						dbType := colTypes[i].DatabaseTypeName()
						_, scale, _ := colTypes[i].DecimalSize()
						switch dbType {
						case "INT", "INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT":
							if num, err := strconv.ParseInt(string(b), 10, 64); err == nil {
								val = num
							} else {
								val = string(b)
							}
						case "NUMERIC":
							if scale > 0 {
								if num, err := strconv.ParseInt(string(b), 10, 64); err == nil {
									val = num
								} else {
									val = string(b)
								}
							} else {
								if num, err := strconv.ParseFloat(string(b), 64); err == nil {
									val = num
								} else {
									val = string(b)
								}
							}
						case "FLOAT", "DOUBLE", "DECIMAL":
							if num, err := strconv.ParseFloat(string(b), 64); err == nil {
								val = num
							} else {
								val = string(b)
							}
						default:
							val = string(b)
						}
					}
				} else {
					val = columns[i]
				}
				rec[colName] = val
			}
			out <- rec
		}
	}(q)
	return out, nil
}

func (l *Adapter) Close() error {
	if l.mode != "loader" {
		return l.Db.Close()
	}
	// Enable constraints before closing
	if err := l.enableConstraints(context.Background()); err != nil {
		// Log error but don't fail close
		log.Printf("failed to enable constraints: %v", err)
	}
	if l.destType == "postgresql" && l.updateSequence {
		return sqlutil.UpdateSequence(l.Db, l.Table)
	}
	return l.Db.Close()
}
