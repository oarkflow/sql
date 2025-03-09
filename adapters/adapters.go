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
	"sort"
	"strings"
	"sync"

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
			return nil, fmt.Errorf("error connecting to lookup DB: %v", err)
		}
		return NewSQLAdapterAsSource(db, "", lkup.Source), nil
	case "csv", "json":
		return NewFileAdapter(lkup.File, "source", false), nil
	default:
		return nil, fmt.Errorf("unsupported lookup type: %s", lkup.Type)
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
	Db              *sql.DB
	mode            string
	Table           string
	truncate        bool
	updateSequence  bool
	destType        string
	update          bool
	delete          bool
	query           string
	AutoCreate      bool
	Created         bool
	Driver          string
	NormalizeSchema map[string]string
}

func NewSQLAdapterAsLoader(db *sql.DB, destType, driver string, cfg config.TableMapping, normalizeSchema map[string]string) *SQLAdapter {
	autoCreate := false
	if !cfg.KeyValueTable && cfg.AutoCreateTable {
		autoCreate = true
	}
	return &SQLAdapter{
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

func NewSQLAdapterAsSource(db *sql.DB, table, query string) *SQLAdapter {
	return &SQLAdapter{Db: db, Table: table, query: query}
}

func (l *SQLAdapter) Setup(ctx context.Context) error {
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
		_, err = l.Db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", l.Table))
		if err != nil {
			return fmt.Errorf("truncate error for table %s: %v", l.Table, err)
		}
	}
	return nil
}

func tableExists(db *sql.DB, tableName, dbType string) (bool, error) {
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

func defaultPlaceholder(destType string) func(int) string {
	switch strings.ToLower(destType) {
	case "postgresql":
		return func(i int) string { return fmt.Sprintf("$%d", i) }
	default: // Assume MySQL/SQLite uses "?"
		return func(i int) string { return "?" }
	}
}

// StreamingSQLLoader implements a streaming loader that batches records
// and uses a worker pool with prepared statements for concurrent INSERTs.
type StreamingSQLLoader struct {
	Db            *sql.DB
	Table         string
	batchSize     int
	workerCount   int
	destType      string
	placeholderFn func(int) string
	AutoCreate    bool
	Created       bool
}

// NewStreamingSQLLoader constructs a new StreamingSQLLoader.
func NewStreamingSQLLoader(db *sql.DB, destType string, table string, batchSize, workerCount int, autoCreate bool) *StreamingSQLLoader {
	return &StreamingSQLLoader{
		Db:            db,
		Table:         table,
		batchSize:     batchSize,
		workerCount:   workerCount,
		destType:      destType,
		AutoCreate:    autoCreate,
		Created:       false,
		placeholderFn: defaultPlaceholder(destType),
	}
}

// Setup performs any initialization required for streaming.
// Here you can add table existence or creation logic if needed.
func (loader *StreamingSQLLoader) Setup(ctx context.Context) error {
	// For now, we assume the table already exists.
	return nil
}

// prepareStmt builds a parameterized INSERT statement for a given batch.
func (loader *StreamingSQLLoader) prepareStmt(keys []string, numRecords int) (*sql.Stmt, error) {
	placeholders := []string{}
	argCounter := 1
	for i := 0; i < numRecords; i++ {
		innerPlaceholders := []string{}
		for range keys {
			innerPlaceholders = append(innerPlaceholders, loader.placeholderFn(argCounter))
			argCounter++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(innerPlaceholders, ", ")))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", loader.Table, strings.Join(keys, ", "), strings.Join(placeholders, ", "))
	return loader.Db.Prepare(query)
}

// StreamLoad consumes records from the provided channel, groups them into batches,
// and concurrently executes INSERT statements.
func (loader *StreamingSQLLoader) StreamLoad(ctx context.Context, recordsCh <-chan utils.Record) error {
	batchesCh := make(chan []utils.Record, loader.workerCount)
	var wg sync.WaitGroup

	// Batch collector goroutine.
	go func() {
		var batch []utils.Record
		for rec := range recordsCh {
			batch = append(batch, rec)
			if len(batch) >= loader.batchSize {
				batchesCh <- batch
				batch = nil
			}
		}
		if len(batch) > 0 {
			batchesCh <- batch
		}
		close(batchesCh)
	}()

	// Worker function to process batches.
	worker := func() {
		defer wg.Done()
		for batch := range batchesCh {
			if len(batch) == 0 {
				continue
			}
			// Determine a consistent key order from the first record.
			keys := []string{}
			for k := range batch[0] {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			stmt, err := loader.prepareStmt(keys, len(batch))
			if err != nil {
				log.Printf("Error preparing statement: %v", err)
				continue
			}
			var args []any
			for _, rec := range batch {
				for _, key := range keys {
					args = append(args, rec[key])
				}
			}
			if _, err := stmt.ExecContext(ctx, args...); err != nil {
				log.Printf("Error executing batch insert: %v", err)
			}
			stmt.Close()
		}
	}

	// Launch concurrent workers.
	wg.Add(loader.workerCount)
	for i := 0; i < loader.workerCount; i++ {
		go worker()
	}
	wg.Wait()
	return nil
}

// Close cleans up any resources used by the streaming loader.
// In this implementation, there are no persistent resources.
func (loader *StreamingSQLLoader) Close() error {
	return nil
}

// StreamingLoaderAdapter wraps StreamingSQLLoader to implement the contract.Loader interface.
// It provides a StoreBatch method that ETL expects, by sending each record of the batch
// into an internal channel processed by the streaming loader.
type StreamingLoaderAdapter struct {
	Loader    *StreamingSQLLoader
	recordsCh chan utils.Record
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewStreamingLoaderAdapter creates a new adapter with a buffered channel.
func NewStreamingLoaderAdapter(loader *StreamingSQLLoader, bufferSize int) *StreamingLoaderAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &StreamingLoaderAdapter{
		Loader:    loader,
		recordsCh: make(chan utils.Record, bufferSize),
		ctx:       ctx,
		cancel:    cancel,
	}
	adapter.wg.Add(1)
	go func() {
		defer adapter.wg.Done()
		if err := adapter.Loader.StreamLoad(ctx, adapter.recordsCh); err != nil {
			log.Printf("Error in StreamingSQLLoader: %v", err)
		}
	}()
	return adapter
}

// Setup calls the underlying loader's Setup method.
func (sla *StreamingLoaderAdapter) Setup(ctx context.Context) error {
	return sla.Loader.Setup(ctx)
}

// StoreBatch receives a batch of records from the ETL and sends each record to the streaming channel.
func (sla *StreamingLoaderAdapter) StoreBatch(ctx context.Context, batch []utils.Record) error {
	for _, rec := range batch {
		select {
		case sla.recordsCh <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close cancels the context, closes the channel, and waits for the worker to finish.
func (sla *StreamingLoaderAdapter) Close() error {
	sla.cancel()
	close(sla.recordsCh)
	sla.wg.Wait()
	return sla.Loader.Close()
}
