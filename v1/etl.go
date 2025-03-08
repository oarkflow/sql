package v1

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

	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/etl/contract"
	"github.com/oarkflow/sql/utils"
)

type MultiTransformer interface {
	TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error)
}

func CreateKeyValueTable(db *sql.DB, tableName string, cfg config.TableMapping) error {
	columns := []string{
		fmt.Sprintf("%s TEXT", cfg.KeyField),
	}
	for extraField := range cfg.ExtraValues {
		columns = append(columns, fmt.Sprintf("%s TEXT", extraField))
	}
	columns = append(columns, fmt.Sprintf("%s TEXT", cfg.ValueField))
	columns = append(columns, "value_type TEXT")
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))
	_, err := db.Exec(createStmt)
	if err != nil {
		return err
	}
	if cfg.TruncateDestination {
		_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateTableFromRecord(db *sql.DB, tableName string, rec utils.Record) error {
	var columns []string
	var keys []string
	for k := range rec {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sqlType := inferSQLType(rec[k])
		columns = append(columns, fmt.Sprintf("%s %s", k, sqlType))
	}
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))
	_, err := db.Exec(createStmt)
	return err
}

func inferSQLType(val interface{}) string {
	switch val.(type) {
	case int, int32, int64:
		return "INTEGER"
	case float32, float64:
		return "REAL"
	case bool:
		return "BOOLEAN"
	case string:
		return "TEXT"
	default:
		return "TEXT"
	}
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

type Option func(*ETL) error

func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) Option {
	return func(e *ETL) error {
		var src contract.Source
		if utils.IsSQLType(sourceType) {
			if sourceDB == nil {
				return fmt.Errorf("source database is nil")
			}
			src = NewSQLSource(sourceDB, sourceTable, sourceQuery)
		} else if sourceType == "csv" || sourceType == "json" {
			file := sourceTable
			if file == "" {
				file = sourceFile
			}
			src = NewFileSource(file)
		} else {
			return fmt.Errorf("unsupported source type: %s", sourceType)
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

func WithDestination(destType string, destDB *sql.DB, destFile string, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var destination contract.Loader
		if utils.IsSQLType(destType) {
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			destination = NewSQLLoader(destDB, destType, cfg)
		} else if destType == "csv" || destType == "json" {
			file := cfg.NewName
			if file == "" {
				file = destFile
			}
			appendMode := true
			if cfg.TruncateDestination {
				appendMode = false
			}
			destination = NewFileLoader(file, appendMode)
		} else {
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
				rec, err := applyMappers(ctx, e.mappers, raw, workerID)
				if err != nil || rec == nil {
					continue
				}
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
					if sqlLoader, ok := loader.(*SQLLoader); ok && sqlLoader.autoCreate && !sqlLoader.created {
						if err := CreateTableFromRecord(sqlLoader.db, sqlLoader.table, batch[0]); err != nil {
							log.Printf("[Loader Worker %d] Error auto-creating table: %v", workerID, err)
							continue
						}
						sqlLoader.created = true
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

func applyTransformers(ctx context.Context, transformers []contract.Transformer, rec utils.Record, workerID int) ([]utils.Record, error) {
	records := []utils.Record{rec}
	for _, transformer := range transformers {
		var nextRecords []utils.Record
		for _, r := range records {
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

type FileCheckpointStore struct {
	fileName string
	mu       sync.Mutex
}

func NewFileCheckpointStore(fileName string) *FileCheckpointStore {
	return &FileCheckpointStore{fileName: fileName}
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

func (fl *FileLoader) Setup(_ context.Context) error {
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

func (fl *FileLoader) LoadBatch(_ context.Context, records []utils.Record) error {
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

type SQLLoader struct {
	db             *sql.DB
	table          string
	truncate       bool
	updateSequence bool
	destType       string
	update         bool
	delete         bool
	query          string
	autoCreate     bool
	created        bool
}

func NewSQLLoader(db *sql.DB, destType string, cfg config.TableMapping) *SQLLoader {
	autoCreate := false
	if !cfg.KeyValueTable && cfg.AutoCreateTable {
		autoCreate = true
	}
	return &SQLLoader{
		db:             db,
		destType:       destType,
		table:          cfg.NewName,
		truncate:       cfg.TruncateDestination,
		updateSequence: cfg.UpdateSequence,
		update:         cfg.Update,
		delete:         cfg.Delete,
		query:          cfg.Query,
		autoCreate:     autoCreate,
		created:        false,
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
	if l.update {
		for _, rec := range batch {
			var q string
			var args []any
			if l.query != "" {
				q = l.query
			} else {
				q, args = buildUpdateStatement(l.table, rec)
			}
			if _, err := l.db.ExecContext(ctx, q, args...); err != nil {
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
				q, args = buildDeleteStatement(l.table, rec)
			}
			if _, err := l.db.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		return nil
	}
	if len(batch) == 0 {
		return nil
	}
	if l.autoCreate && !l.created {
		if err := CreateTableFromRecord(l.db, l.table, batch[0]); err != nil {
			return err
		}
		l.created = true
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

func buildUpdateStatement(table string, rec utils.Record) (string, []any) {
	var setParts []string
	var args []any
	var id any
	for k, v := range rec {
		if strings.ToLower(k) == "id" {
			id = v
			continue
		}
		args = append(args, v)
		setParts = append(setParts, fmt.Sprintf("%s = $%d", k, len(args)))
	}
	if id == nil {
		return "", nil
	}
	args = append(args, id)
	q := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", table, strings.Join(setParts, ", "), len(args))
	return q, args
}

func buildDeleteStatement(table string, rec utils.Record) (string, []any) {
	id, ok := rec["id"]
	if !ok {
		return "", nil
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1", table)
	return q, []any{id}
}

func updateSequence(db *sql.DB, table string) error {
	seqName := fmt.Sprintf("%s_seq", table)
	q := fmt.Sprintf("SELECT setval('%s', (SELECT MAX(id) FROM %s))", seqName, table)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		log.Printf("Error updating sequence %s: %v", seqName, err)
		return err
	}
	log.Printf("Sequence %s updated", seqName)
	return nil
}

func (l *SQLLoader) Close() error {
	if l.destType == "postgresql" && l.updateSequence {
		return updateSequence(l.db, l.table)
	}
	return nil
}

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

type KeyValueTransformer struct {
	ExtraValues   map[string]interface{}
	IncludeFields []string
	ExcludeFields []string
	KeyField      string
	ValueField    string
}

func (kt *KeyValueTransformer) Name() string {
	return "KeyValueTransformer"
}

func (kt *KeyValueTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	recs, err := kt.TransformMany(ctx, rec)
	if err != nil {
		return nil, err
	}
	if len(recs) > 0 {
		return recs[0], nil
	}
	return nil, fmt.Errorf("no output from KeyValueTransformer")
}

func (kt *KeyValueTransformer) TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error) {
	base := make(map[string]interface{})
	for newField, srcFieldRaw := range kt.ExtraValues {
		srcField := strings.ToLower(fmt.Sprintf("%v", srcFieldRaw))
		if val, ok := rec[srcField]; ok {
			base[newField] = val
		}
	}
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
	var candidates []string
	for k := range rec {
		kl := strings.ToLower(k)
		if _, found := ignore[kl]; !found {
			candidates = append(candidates, kl)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate fields found for key-value conversion")
	}
	var out []utils.Record
	for _, cand := range candidates {
		newRec := make(utils.Record)
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
