package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// -------------------------
// Basic Types & Interfaces
// -------------------------

// Record represents a generic data record.
type Record map[string]interface{}

// Source defines the interface for data extraction.
type Source interface {
	Setup(ctx context.Context) error
	Extract(ctx context.Context) (<-chan Record, error)
	Close() error
}

// Loader defines the interface for loading data batches.
type Loader interface {
	Setup(ctx context.Context) error
	LoadBatch(ctx context.Context, batch []Record) error
	Close() error
}

// Mapper converts one record to another.
type Mapper interface {
	Name() string
	Map(ctx context.Context, rec Record) (Record, error)
}

// Transformer converts a record, possibly outputting one or more records.
type Transformer interface {
	Name() string
	Transform(ctx context.Context, rec Record) (Record, error)
}

// MultiTransformer can output multiple records from one input.
type MultiTransformer interface {
	Transformer
	TransformMany(ctx context.Context, rec Record) ([]Record, error)
}

// CheckpointStore persists checkpoints.
type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, cp string) error
	GetCheckpoint(ctx context.Context) (string, error)
}

// -------------------------
// Helper: writeFull
// -------------------------

// writeFull writes the entire byte slice to the provided bufio.Writer.
func writeFull(w *bufio.Writer, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := w.Write(data[total:])
		if err != nil {
			return fmt.Errorf("short write: wrote %d out of %d: %w", total+n, len(data), err)
		}
		total += n
	}
	return nil
}

// -------------------------
// Circuit Breaker Implementation
// -------------------------

type CircuitBreaker struct {
	threshold    int
	failureCount int
	open         bool
	openUntil    time.Time
	resetTimeout time.Duration
	lock         sync.Mutex
}

func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	if cb.open {
		if time.Now().After(cb.openUntil) {
			cb.open = false
			cb.failureCount = 0
			return true
		}
		return false
	}
	return true
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		cb.open = true
		cb.openUntil = time.Now().Add(cb.resetTimeout)
		logrus.Warnf("Circuit breaker opened for %v after %d failures", cb.resetTimeout, cb.failureCount)
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount = 0
	cb.open = false
}

// -------------------------
// ETL Struct & Options
// -------------------------

type ETL struct {
	sources         []Source
	mappers         []Mapper
	transformers    []Transformer
	loaders         []Loader
	workerCount     int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	rawChanBuffer   int
	loaderWorkers   int
	checkpointStore CheckpointStore
	checkpointFunc  func(rec Record) string

	// Error handling and cancellation.
	maxErrorCount  int
	errorCount     int
	errorLock      sync.Mutex
	circuitBreaker *CircuitBreaker
	cancelFunc     context.CancelFunc
}

type Option func(*ETL) error

func defaultConfig() *ETL {
	return &ETL{
		workerCount:    4,
		batchSize:      100,
		retryCount:     3,
		retryDelay:     100 * time.Millisecond,
		loaderWorkers:  2,
		rawChanBuffer:  100,
		maxErrorCount:  10,
		circuitBreaker: NewCircuitBreaker(5, 5*time.Second),
	}
}

func NewETL(opts ...Option) *ETL {
	e := defaultConfig()
	for _, opt := range opts {
		if err := opt(e); err != nil {
			logrus.Errorf("Error applying option: %v", err)
		}
	}
	return e
}

func WithSource(src Source) Option {
	return func(e *ETL) error {
		e.sources = append(e.sources, src)
		return nil
	}
}

func WithLoader(loader Loader) Option {
	return func(e *ETL) error {
		e.loaders = append(e.loaders, loader)
		return nil
	}
}

func WithMapper(mapper Mapper) Option {
	return func(e *ETL) error {
		e.mappers = append(e.mappers, mapper)
		return nil
	}
}

func WithTransformer(transformer Transformer) Option {
	return func(e *ETL) error {
		e.transformers = append(e.transformers, transformer)
		return nil
	}
}

func WithCheckpoint(store CheckpointStore, cpFunc func(rec Record) string) Option {
	return func(e *ETL) error {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
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

// incrementError increases the error count and cancels the context if too many errors occur.
func (e *ETL) incrementError() {
	e.errorLock.Lock()
	defer e.errorLock.Unlock()
	e.errorCount++
	if e.errorCount >= e.maxErrorCount {
		logrus.Error("Max error count reached, cancelling ETL process")
		if e.cancelFunc != nil {
			e.cancelFunc()
		}
	}
}

// -------------------------
// ETL Run Implementation
// -------------------------

// Run launches extraction, processing, batching, and loading concurrently.
func (e *ETL) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	defer cancel()

	// Load checkpoint if available.
	if e.checkpointStore != nil && e.checkpointFunc != nil {
		if cp, err := e.checkpointStore.GetCheckpoint(ctx); err == nil && cp != "" {
			logrus.Infof("Resuming from checkpoint: %s", cp)
		} else if err != nil {
			logrus.Warnf("Error retrieving checkpoint: %v", err)
		}
	}

	rawChan := make(chan Record, e.rawChanBuffer)
	processedChan := make(chan Record, e.workerCount*2)
	batchChan := make(chan []Record, e.loaderWorkers)

	var wg sync.WaitGroup

	// Extraction stage.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var extractWG sync.WaitGroup
		for _, src := range e.sources {
			extractWG.Add(1)
			go func(src Source) {
				defer extractWG.Done()
				if err := src.Setup(ctx); err != nil {
					logrus.Errorf("Source setup error: %v", err)
					e.incrementError()
					return
				}
				ch, err := src.Extract(ctx)
				if err != nil {
					logrus.Errorf("Source extraction error: %v", err)
					e.incrementError()
					return
				}
				for rec := range ch {
					select {
					case rawChan <- rec:
					case <-ctx.Done():
						return
					}
				}
			}(src)
		}
		extractWG.Wait()
		close(rawChan)
	}()

	// Processing stage.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var processWG sync.WaitGroup
		for i := 0; i < e.workerCount; i++ {
			processWG.Add(1)
			go func(workerID int) {
				defer processWG.Done()
				for rec := range rawChan {
					mapped, err := applyMappers(ctx, e.mappers, rec, workerID)
					if err != nil {
						logrus.Warnf("[Worker %d] Mapper error: %v", workerID, err)
						e.incrementError()
						continue
					}
					outRecords, err := applyTransformers(ctx, e.transformers, mapped, workerID)
					if err != nil {
						logrus.Warnf("[Worker %d] Transformer error: %v", workerID, err)
						e.incrementError()
						continue
					}
					for _, r := range outRecords {
						select {
						case processedChan <- r:
						case <-ctx.Done():
							return
						}
					}
				}
			}(i)
		}
		processWG.Wait()
		close(processedChan)
	}()

	// Batching stage.
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := make([]Record, 0, e.batchSize)
		for rec := range processedChan {
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				batchChan <- batch
				batch = make([]Record, 0, e.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()

	// Preinitialize loaders.
	for _, loader := range e.loaders {
		if err := loader.Setup(ctx); err != nil {
			logrus.Errorf("Loader setup error: %v", err)
			e.incrementError()
			return err
		}
	}

	// Loading stage.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var loadWG sync.WaitGroup
		for i := 0; i < e.loaderWorkers; i++ {
			loadWG.Add(1)
			go func(workerID int) {
				defer loadWG.Done()
				for batch := range batchChan {
					for _, loader := range e.loaders {
						err := retryWithCircuit(ctx, e.retryCount, e.retryDelay, e.circuitBreaker, func() error {
							return loader.LoadBatch(ctx, batch)
						})
						if err != nil {
							logrus.Warnf("[Loader Worker %d] Error loading batch: %v", workerID, err)
							e.incrementError()
							continue
						}
						if e.checkpointStore != nil && e.checkpointFunc != nil {
							cp := e.checkpointFunc(batch[len(batch)-1])
							if err := e.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
								logrus.Warnf("[Loader Worker %d] Error saving checkpoint: %v", workerID, err)
								e.incrementError()
							}
						}
					}
				}
			}(i)
		}
		loadWG.Wait()
	}()

	wg.Wait()
	return nil
}

func (e *ETL) Close() error {
	var errs []string
	for _, src := range e.sources {
		if err := src.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("source close: %v", err))
		}
	}
	for _, loader := range e.loaders {
		if err := loader.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("loader close: %v", err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// -------------------------
// Retry with Circuit Breaker
// -------------------------

func retryWithCircuit(ctx context.Context, retryCount int, retryDelay time.Duration, cb *CircuitBreaker, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		if !cb.Allow() {
			return errors.New("circuit breaker is open")
		}
		err = fn()
		if err == nil {
			cb.RecordSuccess()
			return nil
		}
		cb.RecordFailure()
		logrus.Warnf("Retry attempt %d failed: %v", i+1, err)
		jitter := 0.8 + rand.Float64()*0.4
		select {
		case <-time.After(time.Duration(float64(retryDelay) * jitter)):
		case <-ctx.Done():
			return ctx.Err()
		}
		retryDelay *= 2
	}
	return err
}

// -------------------------
// Pipeline Stage Functions
// -------------------------

func applyMappers(ctx context.Context, mappers []Mapper, rec Record, workerID int) (Record, error) {
	var err error
	for _, mapper := range mappers {
		rec, err = mapper.Map(ctx, rec)
		if err != nil {
			logrus.Warnf("[Worker %d] Mapper (%s) error: %v", workerID, mapper.Name(), err)
			return nil, err
		}
	}
	return rec, nil
}

func applyTransformers(ctx context.Context, transformers []Transformer, rec Record, workerID int) ([]Record, error) {
	records := []Record{rec}
	for _, transformer := range transformers {
		var nextRecords []Record
		if mt, ok := transformer.(MultiTransformer); ok {
			for _, r := range records {
				recs, err := mt.TransformMany(ctx, r)
				if err != nil {
					logrus.Warnf("[Worker %d] MultiTransformer (%s) error: %v", workerID, transformer.Name(), err)
					continue
				}
				nextRecords = append(nextRecords, recs...)
			}
		} else {
			for _, r := range records {
				r2, err := transformer.Transform(ctx, r)
				if err != nil {
					logrus.Warnf("[Worker %d] Transformer (%s) error: %v", workerID, transformer.Name(), err)
					continue
				}
				nextRecords = append(nextRecords, r2)
			}
		}
		records = nextRecords
	}
	if len(records) == 0 {
		return nil, errors.New("no records after transformation")
	}
	return records, nil
}

// -------------------------
// Checkpoint Store: File Implementation
// -------------------------

type FileCheckpointStore struct {
	fileName string
	mu       sync.Mutex
}

func NewFileCheckpointStore(fileName string) *FileCheckpointStore {
	return &FileCheckpointStore{fileName: fileName}
}

func (cs *FileCheckpointStore) SaveCheckpoint(ctx context.Context, cp string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return os.WriteFile(cs.fileName, []byte(cp), 0644)
}

func (cs *FileCheckpointStore) GetCheckpoint(ctx context.Context) (string, error) {
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

// -------------------------
// File and SQL Sources
// -------------------------

// FileSource reads CSV or JSON files.
type FileSource struct {
	Filename string
	fileType string // "csv" or "json"
}

func NewFileSource(filename, fileType string) *FileSource {
	return &FileSource{
		Filename: filename,
		fileType: fileType,
	}
}

func (fs *FileSource) Setup(ctx context.Context) error {
	return nil
}

func (fs *FileSource) Close() error {
	return nil
}

func (fs *FileSource) Extract(ctx context.Context) (<-chan Record, error) {
	out := make(chan Record)
	go func() {
		defer close(out)
		data, err := ioutil.ReadFile(fs.Filename)
		if err != nil {
			logrus.Errorf("Error reading file in FileSource: %v", err)
			return
		}
		if fs.fileType == "json" {
			var records []Record
			if err := json.Unmarshal(data, &records); err != nil {
				logrus.Errorf("Error unmarshaling JSON file: %v", err)
				return
			}
			for _, rec := range records {
				out <- rec
			}
		} else if fs.fileType == "csv" {
			r := csv.NewReader(strings.NewReader(string(data)))
			header, err := r.Read()
			if err != nil {
				logrus.Errorf("Error reading CSV header: %v", err)
				return
			}
			for {
				row, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					logrus.Warnf("Error reading CSV row: %v", err)
					continue
				}
				rec := make(Record)
				for i, h := range header {
					rec[h] = row[i]
				}
				out <- rec
			}
		}
	}()
	return out, nil
}

// SQLSource extracts records from a SQL database.
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

func (s *SQLSource) Close() error {
	return nil
}

func (s *SQLSource) Extract(ctx context.Context) (<-chan Record, error) {
	out := make(chan Record, 100)
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
			logrus.Errorf("SQL query error: %v", err)
			return
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			logrus.Errorf("Error getting columns: %v", err)
			return
		}
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}
			if err := rows.Scan(columnPointers...); err != nil {
				logrus.Warnf("Scan error: %v", err)
				continue
			}
			rec := make(Record)
			for i, colName := range cols {
				rec[colName] = columns[i]
			}
			out <- rec
		}
	}()
	return out, nil
}

// -------------------------
// Loader Implementations
// -------------------------

// FileLoader writes data to CSV or JSON files.
// It uses sync.Once to ensure that Setup is performed only once.
type FileLoader struct {
	fileName   string
	extension  string // "csv" or "json"
	appendMode bool

	file      *os.File
	bufWriter *bufio.Writer
	csvWriter *csv.Writer

	// For JSON files:
	jsonFirstRecord bool

	// For CSV files:
	headerWritten bool
	csvHeader     []string

	initOnce sync.Once
	setupErr error

	// Added lock for concurrency safety
	lock sync.Mutex
}

func NewFileLoader(fileName string, appendMode bool) *FileLoader {
	ext := strings.TrimPrefix(filepath.Ext(fileName), ".")
	return &FileLoader{
		fileName:   fileName,
		extension:  ext,
		appendMode: appendMode,
	}
}

func (fl *FileLoader) Setup(ctx context.Context) error {
	fl.initOnce.Do(func() {
		switch fl.extension {
		case "json":
			if fl.appendMode {
				logrus.Warn("Append mode for JSON not supported; creating new file")
				fl.appendMode = false
			}
			var f *os.File
			f, fl.setupErr = os.Create(fl.fileName)
			if fl.setupErr != nil {
				fl.setupErr = fmt.Errorf("create file: %w", fl.setupErr)
				return
			}
			fl.file = f
			fl.bufWriter = bufio.NewWriter(f)
			if err := writeFull(fl.bufWriter, []byte("[\n")); err != nil {
				fl.setupErr = fmt.Errorf("failed to write JSON array start: %w", err)
				return
			}
			fl.jsonFirstRecord = true
		case "csv":
			var f *os.File
			if fl.appendMode {
				f, fl.setupErr = os.OpenFile(fl.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
				if fl.setupErr != nil {
					fl.setupErr = fmt.Errorf("open CSV file in append mode: %w", fl.setupErr)
					return
				}
				info, err := f.Stat()
				if err != nil {
					fl.setupErr = fmt.Errorf("stat CSV file: %w", err)
					return
				}
				if info.Size() > 0 {
					fl.headerWritten = true
				}
			} else {
				f, fl.setupErr = os.Create(fl.fileName)
				if fl.setupErr != nil {
					fl.setupErr = fmt.Errorf("create CSV file: %w", fl.setupErr)
					return
				}
			}
			fl.file = f
			fl.bufWriter = bufio.NewWriter(f)
			fl.csvWriter = csv.NewWriter(fl.bufWriter)
		default:
			fl.setupErr = fmt.Errorf("unsupported file extension: %s", fl.extension)
		}
	})
	return fl.setupErr
}

func (fl *FileLoader) LoadBatch(ctx context.Context, records []Record) error {
	fl.lock.Lock()
	defer fl.lock.Unlock()
	switch fl.extension {
	case "json":
		for _, rec := range records {
			if !fl.jsonFirstRecord {
				if err := writeFull(fl.bufWriter, []byte(",\n")); err != nil {
					return fmt.Errorf("failed to write comma: %w", err)
				}
			}
			data, err := json.Marshal(rec)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON record: %w", err)
			}
			if err := writeFull(fl.bufWriter, []byte("\t"+string(data))); err != nil {
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
	fl.lock.Lock()
	defer fl.lock.Unlock()
	if fl.extension == "json" {
		// Flush before writing the closing bracket
		if err := fl.bufWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush before writing JSON closing: %w", err)
		}
		if err := writeFull(fl.bufWriter, []byte("\n]\n")); err != nil {
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

func extractCSVHeader(rec Record) []string {
	header := make([]string, 0, len(rec))
	for key := range rec {
		header = append(header, key)
	}
	sort.Strings(header)
	return header
}

func buildCSVRow(header []string, rec Record) ([]string, error) {
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
		case bool:
			row[i] = strconv.FormatBool(v)
		default:
			row[i] = fmt.Sprintf("%v", v)
		}
	}
	return row, nil
}

// SQLLoader writes data into a SQL table.
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

func NewSQLLoader(db *sql.DB, destType, table string, cfg map[string]interface{}) *SQLLoader {
	return &SQLLoader{
		db:         db,
		destType:   destType,
		table:      table,
		truncate:   false,
		update:     false,
		delete:     false,
		query:      "",
		autoCreate: true,
		created:    false,
	}
}

func (l *SQLLoader) Setup(ctx context.Context) error {
	if l.truncate {
		_, err := l.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", l.table))
		if err != nil {
			logrus.Warnf("Truncate error for table %s: %v", l.table, err)
		}
	}
	return nil
}

func (l *SQLLoader) LoadBatch(ctx context.Context, batch []Record) error {
	if l.update {
		for _, rec := range batch {
			var q string
			var args []interface{}
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
			var args []interface{}
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
	sort.Strings(keys)
	var placeholders []string
	var args []interface{}
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

func (l *SQLLoader) Close() error {
	if l.destType == "postgresql" && l.updateSequence {
		return updateSequence(l.db, l.table)
	}
	return nil
}

func buildUpdateStatement(table string, rec Record) (string, []interface{}) {
	var setParts []string
	var args []interface{}
	var id interface{}
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

func buildDeleteStatement(table string, rec Record) (string, []interface{}) {
	id, ok := rec["id"]
	if !ok {
		return "", nil
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1", table)
	return q, []interface{}{id}
}

func updateSequence(db *sql.DB, table string) error {
	seqName := fmt.Sprintf("%s_seq", table)
	q := fmt.Sprintf("SELECT setval('%s', (SELECT MAX(id) FROM %s))", seqName, table)
	if _, err := db.Exec(q); err != nil {
		logrus.Warnf("Error updating sequence %s: %v", seqName, err)
		return err
	}
	logrus.Infof("Sequence %s updated", seqName)
	return nil
}

// CreateTableFromRecord creates a table based on the schema of a record.
func CreateTableFromRecord(db *sql.DB, tableName string, rec Record) error {
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
	case time.Time:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

// -------------------------
// Mapper and Transformer Implementations
// -------------------------

// FieldMapper maps fields using a provided mapping.
type FieldMapper struct {
	mapping map[string]string
}

func NewFieldMapper(mapping map[string]string) *FieldMapper {
	return &FieldMapper{mapping: mapping}
}

func (fm *FieldMapper) Name() string {
	return "FieldMapper"
}

func (fm *FieldMapper) Map(ctx context.Context, rec Record) (Record, error) {
	newRec := make(Record)
	for destField, srcField := range fm.mapping {
		if val, ok := rec[srcField]; ok {
			newRec[destField] = val
		} else {
			newRec[destField] = nil
		}
	}
	return newRec, nil
}

// LowercaseMapper converts all keys to lowercase.
type LowercaseMapper struct{}

func (lm *LowercaseMapper) Name() string {
	return "LowercaseMapper"
}

func (lm *LowercaseMapper) Map(ctx context.Context, rec Record) (Record, error) {
	newRec := make(Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}

// LookupTransformer adds a looked-up value based on a field.
type LookupTransformer struct {
	LookupData  map[string]string
	Field       string
	TargetField string
}

func (lt *LookupTransformer) Name() string {
	return "LookupTransformer"
}

func (lt *LookupTransformer) Transform(ctx context.Context, rec Record) (Record, error) {
	if key, ok := rec[lt.Field]; ok {
		keyStr := fmt.Sprintf("%v", key)
		if val, exists := lt.LookupData[keyStr]; exists {
			rec[lt.TargetField] = val
		}
	}
	return rec, nil
}

// FilterTransformer filters out records; here it drops records with even "id".
type FilterTransformer struct{}

func (ft *FilterTransformer) Name() string {
	return "FilterTransformer"
}

func (ft *FilterTransformer) Transform(ctx context.Context, rec Record) (Record, error) {
	idVal, ok := rec["id"]
	if !ok {
		return rec, nil
	}
	idInt, err := strconv.Atoi(fmt.Sprintf("%v", idVal))
	if err != nil {
		return rec, nil
	}
	if idInt%2 == 0 {
		return nil, errors.New("filtered out record with even id")
	}
	return rec, nil
}

// ConditionalTransformer conditionally adds a greeting if name is "Alice".
type ConditionalTransformer struct{}

func (ct *ConditionalTransformer) Name() string {
	return "ConditionalTransformer"
}

func (ct *ConditionalTransformer) Transform(ctx context.Context, rec Record) (Record, error) {
	if name, ok := rec["name"]; ok {
		if strings.ToLower(fmt.Sprintf("%v", name)) == "alice" {
			rec["greeting"] = "Hello, Alice!"
		}
	}
	return rec, nil
}

// -------------------------
// Main Function
// -------------------------

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	ctx := context.Background()

	// For demonstration, create an ETL job that extracts from a CSV file
	// and loads into a JSON file, applying several mappers and transformers.
	csvSource := NewFileSource("source_data.csv", "csv")
	jsonLoader := NewFileLoader("destination_data.json", false)

	fieldMap := map[string]string{
		"identifier": "id",
		"fullname":   "name",
		"mail":       "email",
	}
	fieldMapper := NewFieldMapper(fieldMap)
	lowercaseMapper := &LowercaseMapper{}
	lookupTransformer := &LookupTransformer{
		LookupData:  map[string]string{"1": "one", "3": "three"},
		Field:       "id",
		TargetField: "number_word",
	}
	filterTransformer := &FilterTransformer{}
	conditionalTransformer := &ConditionalTransformer{}

	checkpointStore := NewFileCheckpointStore("checkpoint.txt")
	checkpointFunc := func(rec Record) string {
		if id, ok := rec["id"]; ok {
			return fmt.Sprintf("%v", id)
		}
		return ""
	}

	etlJob := NewETL(
		WithSource(csvSource),
		WithLoader(jsonLoader),
		WithMapper(fieldMapper),
		WithMapper(lowercaseMapper),
		WithTransformer(lookupTransformer),
		WithTransformer(filterTransformer),
		WithTransformer(conditionalTransformer),
		WithCheckpoint(checkpointStore, checkpointFunc),
		WithWorkerCount(4),
		WithBatchSize(3), // Smaller batch size for demonstration.
		WithRawChanBuffer(50),
	)

	if err := etlJob.Run(ctx); err != nil {
		logrus.Fatalf("ETL job failed: %v", err)
	}
	if err := etlJob.Close(); err != nil {
		logrus.Errorf("Error closing ETL job: %v", err)
	}
	logrus.Info("All migrations complete.")
}
