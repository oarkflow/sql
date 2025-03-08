package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/etl/contract"
	"github.com/oarkflow/sql/utils"
)

// MultiTransformer allows a transformer to produce multiple output records
type MultiTransformer interface {
	TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <config.yaml>", os.Args[0])
	}
	configPath := os.Args[1]
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	var sourceDB *sql.DB
	var destDB *sql.DB
	// Open source DB if needed.
	if cfg.Source.Type == "mysql" || cfg.Source.Type == "postgresql" {
		sourceDB, err = etl.OpenDB(cfg.Source)
		if err != nil {
			log.Fatalf("Error connecting to source DB: %v", err)
		}
		defer sourceDB.Close()
	}
	// Open destination DB if needed.
	if cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql" {
		destDB, err = etl.OpenDB(cfg.Destination)
		if err != nil {
			log.Fatalf("Error connecting to destination DB: %v", err)
		}
		defer destDB.Close()
	}

	// Process each table configuration.
	for _, tableCfg := range cfg.Tables {
		// For SQL destinations, respect the migrate flag.
		if (cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql") && !tableCfg.Migrate {
			continue
		}
		log.Printf("Starting migration: %s -> %s", tableCfg.OldName, tableCfg.NewName)
		// For SQL destinations only, auto-create table if requested.
		if (cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql") && tableCfg.AutoCreateTable {
			csvFileName := tableCfg.OldName
			if csvFileName == "" {
				csvFileName = cfg.Source.File
			}
			if err := etl.CreateTableFromCSV(destDB, csvFileName, tableCfg.NewName); err != nil {
				log.Fatalf("Error creating table %s: %v", tableCfg.NewName, err)
			}
		}

		// Build the ETL job.
		etlJob := NewETL(
			WithSource(cfg.Source.Type, sourceDB, cfg.Source.File, tableCfg.OldName, tableCfg.Query),
			WithDestination(cfg.Destination.Type, destDB, cfg.Destination.File, tableCfg),
			WithCheckpoint(NewFileCheckpointStore("checkpoint.txt"), func(rec utils.Record) string {
				if name, ok := rec["name"].(string); ok {
					return name
				}
				return ""
			}),
			WithMapping(tableCfg.Mapping),
			// Add any additional transformers.
			WithTransformers(),
			// If key_value_table is true, add the key-value transformer.
			func(e *ETL) error {
				if tableCfg.KeyValueTable {
					return WithKeyValueTransformer(
						tableCfg.ExtraValues,
						tableCfg.IncludeFields,
						tableCfg.ExcludeFields,
						tableCfg.KeyField,
						tableCfg.ValueField,
					)(e)
				}
				return nil
			},
			WithWorkerCount(2),
			WithBatchSize(tableCfg.BatchSize),
			WithRawChanBuffer(50),
		)

		ctx := context.Background()
		if err := etlJob.Run(ctx); err != nil {
			log.Printf("ETL job failed: %v", err)
		}
		if err := etlJob.Close(); err != nil {
			log.Printf("Error closing ETL job: %v", err)
		}
		log.Printf("Migration for %s complete", tableCfg.OldName)
	}
	log.Println("All migrations complete.")
}

func retry(retryCount int, retryDelay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("Retry attempt %d failed: %v", i+1, err)
		jitter := 0.8 + rand.Float64()*0.4
		time.Sleep(time.Duration(float64(retryDelay) * jitter))
		retryDelay *= 2
	}
	return err
}

//
// ETL Options and Pipeline Setup
//

type Option func(*ETL) error

func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) Option {
	return func(e *ETL) error {
		var src contract.Source
		switch sourceType {
		case "mysql", "postgresql":
			if sourceDB == nil {
				return fmt.Errorf("source database is nil")
			}
			src = NewSQLSource(sourceDB, sourceTable, sourceQuery)
		case "csv", "json":
			file := sourceTable
			if file == "" {
				file = sourceFile
			}
			src = NewFileSource(file)
		default:
			return fmt.Errorf("unsupported source type: %s", sourceType)
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

func WithDestination(destType string, destDB *sql.DB, destFile string, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var destination contract.Loader
		switch destType {
		case "mysql", "postgresql":
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			destination = NewSQLLoader(destDB, destType, cfg)
		case "csv", "json":
			file := cfg.NewName
			if file == "" {
				file = destFile
			}
			appendMode := true
			if cfg.TruncateDestination {
				appendMode = false
			}
			destination = NewFileLoader(file, appendMode)
		default:
			return fmt.Errorf("unsupported destination type: %s", destType)
		}
		e.loaders = append(e.loaders, destination)
		return nil
	}
}

func WithMapping(mapping map[string]string) Option {
	return func(e *ETL) error {
		var mappersList []contract.Mapper
		if len(mapping) > 0 {
			mappersList = append(mappersList, NewFieldMapper(mapping))
		}
		mappersList = append(mappersList, &LowercaseMapper{})
		e.mappers = append(e.mappers, mappersList...)
		return nil
	}
}

func WithTransformers() Option {
	return func(e *ETL) error {
		transformersList := []contract.Transformer{
			&LookupTransformer{
				LookupData:  map[string]string{"key1": "value1"},
				Field:       "some_field",
				TargetField: "lookup_result",
			},
		}
		e.transformers = append(e.transformers, transformersList...)
		return nil
	}
}

// WithKeyValueTransformer adds a multi-transformer that converts a record into one or more key-value rows.
// It uses the following rules:
//  1. Build a base row from extra_values – for each mapping (e.g. user_id: "id"), copy the value from the source field.
//  2. Candidate fields for conversion are those in the original record that are NOT part of the extra values,
//     AND not in the include_fields list, AND not in the exclude_fields list.
//  3. For each candidate field, produce a new record that contains the base extra fields plus:
//     - key_field: set to the candidate field name,
//     - value_field: set to the candidate’s value,
//     - "value_type": a string indicating the type (e.g. "string", "int").
//
// In our sample, this will produce rows such as:
//
//	user_id: 1, key: "status", value: "banned", value_type: "string"
func WithKeyValueTransformer(extraValues map[string]interface{}, includeFields, excludeFields []string, keyField, valueField string) Option {
	return func(e *ETL) error {
		kt := &KeyValueTransformer{
			ExtraValues:   extraValues,
			IncludeFields: includeFields,
			ExcludeFields: excludeFields,
			KeyField:      keyField,
			ValueField:    valueField,
		}
		e.transformers = append(e.transformers, kt)
		return nil
	}
}

func WithWorkerCount(count int) Option {
	return func(e *ETL) error {
		e.workerCount = count
		return nil
	}
}

func WithBatchSize(size int) Option {
	return func(e *ETL) error {
		e.batchSize = size
		return nil
	}
}

func WithRawChanBuffer(buffer int) Option {
	return func(e *ETL) error {
		e.rawChanBuffer = buffer
		return nil
	}
}

func WithCheckpoint(store contract.CheckpointStore, cpFunc func(rec utils.Record) string) Option {
	return func(e *ETL) error {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
		return nil
	}
}

//
// ETL Structure and Pipeline Run
//

type ETL struct {
	sources         []contract.Source
	mappers         []contract.Mapper
	validators      []contract.Validator
	transformers    []contract.Transformer
	loaders         []contract.Loader
	workerCount     int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	loaderWorkers   int
	rawChanBuffer   int
	checkpointStore contract.CheckpointStore
	checkpointFunc  func(rec utils.Record) string
	lastCheckpoint  string
	cpMutex         sync.Mutex
}

func defaultConfig() *ETL {
	return &ETL{
		workerCount:   4,
		batchSize:     100,
		retryCount:    3,
		retryDelay:    100 * time.Millisecond,
		loaderWorkers: 2,
		rawChanBuffer: 100,
	}
}

func NewETL(opts ...Option) *ETL {
	e := defaultConfig()
	for _, opt := range opts {
		if err := opt(e); err != nil {
			log.Printf("Error applying option: %v", err)
		}
	}
	return e
}

func (e *ETL) Run(ctx context.Context) error {
	if e.checkpointStore != nil {
		if cp, err := e.checkpointStore.GetCheckpoint(ctx); err != nil {
			log.Printf("Error retrieving checkpoint: %v", err)
		} else {
			e.lastCheckpoint = cp
			log.Printf("Resuming from checkpoint: %s", e.lastCheckpoint)
		}
	}
	rawChan := make(chan utils.Record, e.rawChanBuffer)
	var srcWG sync.WaitGroup
	for _, s := range e.sources {
		if err := s.Setup(ctx); err != nil {
			return fmt.Errorf("source setup error: %v", err)
		}
		srcWG.Add(1)
		go func(src contract.Source) {
			defer srcWG.Done()
			ch, err := src.Extract(ctx)
			if err != nil {
				log.Printf("Source extraction error: %v", err)
				return
			}
			for rec := range ch {
				rawChan <- rec
			}
		}(s)
	}
	go func() {
		srcWG.Wait()
		close(rawChan)
	}()
	processedChan := make(chan utils.Record, e.workerCount*2)
	var procWG sync.WaitGroup
	for i := 0; i < e.workerCount; i++ {
		procWG.Add(1)
		go func(workerID int) {
			defer procWG.Done()
			for raw := range rawChan {
				// Apply mappers (which return one record each)
				rec, err := applyMappers(ctx, e.mappers, raw, workerID)
				if err != nil || rec == nil {
					continue
				}
				// Apply transformers – supporting multi-record output.
				outRecords, err := applyTransformers(ctx, e.transformers, rec, workerID)
				if err != nil {
					continue
				}
				for _, r := range outRecords {
					processedChan <- r
				}
			}
		}(i)
	}
	go func() {
		procWG.Wait()
		close(processedChan)
	}()
	batchChan := make(chan []utils.Record, e.loaderWorkers)
	var batchWG sync.WaitGroup
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		batch := make([]utils.Record, 0, e.batchSize)
		for rec := range processedChan {
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				batchChan <- batch
				batch = make([]utils.Record, 0, e.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()
	var loaderWG sync.WaitGroup
	for i := 0; i < e.loaderWorkers; i++ {
		loaderWG.Add(1)
		go func(workerID int) {
			defer loaderWG.Done()
			for batch := range batchChan {
				for _, loader := range e.loaders {
					if err := loader.Setup(ctx); err != nil {
						log.Printf("[Loader Worker %d] Loader setup error: %v", workerID, err)
						continue
					}
					err := retry(e.retryCount, e.retryDelay, func() error {
						return loader.LoadBatch(ctx, batch)
					})
					if err != nil {
						log.Printf("[Loader Worker %d] Error loading batch: %v", workerID, err)
						continue
					}
					if e.checkpointStore != nil && e.checkpointFunc != nil {
						cp := e.checkpointFunc(batch[len(batch)-1])
						e.cpMutex.Lock()
						if cp > e.lastCheckpoint {
							if err := e.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
								log.Printf("[Loader Worker %d] Error saving checkpoint: %v", workerID, err)
							} else {
								e.lastCheckpoint = cp
							}
						}
						e.cpMutex.Unlock()
					}
				}
			}
		}(i)
	}
	batchWG.Wait()
	loaderWG.Wait()
	return nil
}

func (e *ETL) Close() error {
	for _, src := range e.sources {
		if err := src.Close(); err != nil {
			return fmt.Errorf("error closing source: %v", err)
		}
	}
	for _, loader := range e.loaders {
		if err := loader.Close(); err != nil {
			return fmt.Errorf("error closing loader: %v", err)
		}
	}
	return nil
}

// applyMappers applies all mappers sequentially.
func applyMappers(ctx context.Context, mappers []contract.Mapper, rec utils.Record, workerID int) (utils.Record, error) {
	for _, mapper := range mappers {
		var err error
		rec, err = mapper.Map(ctx, rec)
		if err != nil {
			log.Printf("[Worker %d] Mapper (%s) error: %v", workerID, mapper.Name(), err)
			return nil, err
		}
	}
	return rec, nil
}

// applyTransformers applies all transformers and supports multi-output.
func applyTransformers(ctx context.Context, transformers []contract.Transformer, rec utils.Record, workerID int) ([]utils.Record, error) {
	records := []utils.Record{rec}
	for _, transformer := range transformers {
		var nextRecords []utils.Record
		for _, r := range records {
			// Check if transformer supports multiple outputs.
			if mt, ok := transformer.(MultiTransformer); ok {
				recs, err := mt.TransformMany(ctx, r)
				if err != nil {
					log.Printf("[Worker %d] MultiTransformer error: %v", workerID, err)
					continue
				}
				nextRecords = append(nextRecords, recs...)
			} else {
				r2, err := transformer.Transform(ctx, r)
				if err != nil {
					log.Printf("[Worker %d] Transformer error: %v", workerID, err)
					continue
				}
				nextRecords = append(nextRecords, r2)
			}
		}
		records = nextRecords
	}
	return records, nil
}

//
// Checkpoint Store Implementation
//

type FileCheckpointStore struct {
	fileName string
	mu       sync.Mutex
}

func NewFileCheckpointStore(fileName string) *FileCheckpointStore {
	return &FileCheckpointStore{
		fileName: fileName,
	}
}

func (cs *FileCheckpointStore) SaveCheckpoint(_ context.Context, cp string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return os.WriteFile(cs.fileName, []byte(cp), 0644)
}

func (cs *FileCheckpointStore) GetCheckpoint(context.Context) (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	data, err := os.ReadFile(cs.fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

//
// File Loader (for CSV/JSON destinations)
//

type FileLoader struct {
	fileName        string
	extension       string
	appendMode      bool
	file            *os.File
	bufWriter       *bufio.Writer
	csvWriter       *csv.Writer
	jsonFirstRecord bool
	csvHeader       []string
	headerWritten   bool
}

func NewFileLoader(fileName string, appendMode bool) *FileLoader {
	extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
	return &FileLoader{
		fileName:        fileName,
		extension:       extension,
		appendMode:      appendMode,
		jsonFirstRecord: true,
	}
}

func (fl *FileLoader) Setup(ctx context.Context) error {
	switch fl.extension {
	case "json":
		if fl.appendMode {
			f, err := os.OpenFile(fl.fileName, os.O_RDWR|os.O_CREATE, 0644)
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
			f, err := os.Create(fl.fileName)
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
			f, err = os.OpenFile(fl.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
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
			f, err = os.Create(fl.fileName)
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

func (fl *FileLoader) LoadBatch(ctx context.Context, records []utils.Record) error {
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
			fl.csvHeader = extractCSVHeader(records[0])
		}
		if !fl.headerWritten && len(records) > 0 {
			if err := fl.csvWriter.Write(fl.csvHeader); err != nil {
				return fmt.Errorf("failed to write CSV header: %w", err)
			}
			fl.headerWritten = true
		}
		for _, rec := range records {
			row, err := buildCSVRow(fl.csvHeader, rec)
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

func (fl *FileLoader) Close() error {
	if fl.extension == "json" {
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

func extractCSVHeader(rec utils.Record) []string {
	header := make([]string, 0, len(rec))
	for key := range rec {
		header = append(header, key)
	}
	sort.Strings(header)
	return header
}

func buildCSVRow(header []string, rec utils.Record) ([]string, error) {
	row := make([]string, len(header))
	for i, key := range header {
		val, ok := rec[key]
		if !ok {
			row[i] = ""
			continue
		}
		switch v := val.(type) {
		case string:
			row[i] = v
		case int:
			row[i] = strconv.Itoa(v)
		case int64:
			row[i] = strconv.FormatInt(v, 10)
		case float64:
			row[i] = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			row[i] = fmt.Sprintf("%v", v)
		}
	}
	return row, nil
}

//
// SQL Loader (for SQL destinations)
//

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

func (l *SQLLoader) LoadBatch(ctx context.Context, batch []utils.Record) error {
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

//
// Mappers
//

type FieldMapper struct {
	mapping map[string]string
}

func NewFieldMapper(mapping map[string]string) *FieldMapper {
	return &FieldMapper{mapping: mapping}
}

func (fm *FieldMapper) Name() string {
	return "FieldMapper"
}

func (fm *FieldMapper) Map(ctx context.Context, rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for destField, expr := range fm.mapping {
		_, val := utils.GetValue(ctx, expr, rec)
		newRec[destField] = val
	}
	return newRec, nil
}

type LowercaseMapper struct{}

func (lm *LowercaseMapper) Name() string {
	return "LowercaseMapper"
}

func (lm *LowercaseMapper) Map(ctx context.Context, rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}

//
// KeyValue Transformer (MultiTransformer)
//

type KeyValueTransformer struct {
	ExtraValues   map[string]interface{} // mapping: new field name -> source field name
	IncludeFields []string               // fields to include (will be used as base fields)
	ExcludeFields []string               // fields to exclude from conversion
	KeyField      string                 // name of key column to create
	ValueField    string                 // name of value column to create
}

func (kt *KeyValueTransformer) Name() string {
	return "KeyValueTransformer"
}

func (kt *KeyValueTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	// Fallback to single-record output if needed.
	recs, err := kt.TransformMany(ctx, rec)
	if err != nil {
		return nil, err
	}
	if len(recs) > 0 {
		return recs[0], nil
	}
	return nil, fmt.Errorf("no output from KeyValueTransformer")
}

// TransformMany splits the input record into one record per candidate field.
// It builds a base record from extra values (by copying the value from the specified source field)
// and then for each field in the input record that is not used in extra values, include_fields, or exclude_fields,
// it creates a new record with the key/value pair.
func (kt *KeyValueTransformer) TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error) {
	// Build base extra values.
	base := make(map[string]interface{})
	for newField, srcFieldRaw := range kt.ExtraValues {
		srcField := strings.ToLower(fmt.Sprintf("%v", srcFieldRaw))
		if val, ok := rec[srcField]; ok {
			base[newField] = val
		}
	}
	// Build a set of fields to ignore: fields used in extra values, include_fields, and exclude_fields.
	ignore := make(map[string]struct{})
	for _, v := range kt.ExtraValues {
		ignore[strings.ToLower(fmt.Sprintf("%v", v))] = struct{}{}
	}
	for _, f := range kt.IncludeFields {
		ignore[strings.ToLower(f)] = struct{}{}
	}
	for _, f := range kt.ExcludeFields {
		ignore[strings.ToLower(f)] = struct{}{}
	}

	// Determine candidate fields.
	var candidates []string
	for k := range rec {
		kl := strings.ToLower(k)
		if _, found := ignore[kl]; !found {
			candidates = append(candidates, kl)
		}
	}
	// If no candidate fields, return an error or empty slice.
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate fields found for key-value conversion")
	}
	// For each candidate field, create a new record.
	var out []utils.Record
	for _, cand := range candidates {
		newRec := make(utils.Record)
		// Copy base extra values.
		for k, v := range base {
			newRec[k] = v
		}
		newRec[kt.KeyField] = cand
		if val, ok := rec[cand]; ok {
			newRec[kt.ValueField] = val
			newRec["value_type"] = typeName(val)
		}
		out = append(out, newRec)
	}
	return out, nil
}

func typeName(v interface{}) string {
	switch v.(type) {
	case int, int32, int64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "bool"
	case string:
		return "string"
	default:
		return "unknown"
	}
}

//
// Sources
//

type FileSource struct {
	Filename string
}

func (fs *FileSource) Close() error {
	return nil
}

func (fs *FileSource) Setup(_ context.Context) error {
	return nil
}

func NewFileSource(filename string) *FileSource {
	return &FileSource{Filename: filename}
}

func (fs *FileSource) Extract(_ context.Context) (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		_, err := utils.ProcessFile(fs.Filename, func(record utils.Record) {
			out <- record
		})
		if err != nil {
			log.Printf("File extraction error: %v", err)
		}
	}()
	return out, nil
}

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

//
// Lookup Transformer (unchanged)
//

type LookupTransformer struct {
	LookupData  map[string]string
	Field       string
	lookupField string
	TargetField string
}

func (lt *LookupTransformer) Name() string {
	return "Lookup"
}

func (lt *LookupTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	if key, ok := rec[lt.Field]; ok {
		keyStr := fmt.Sprintf("%v", key)
		if val, exists := lt.LookupData[keyStr]; exists {
			rec[lt.TargetField] = val
		}
	}
	return rec, nil
}
