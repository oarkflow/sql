package fileutil

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
)

// It is assumed that T is compatible with a record of type map[string]any.
type CSVAppender[T any] struct {
	filePath      string
	file          *os.File
	bufWriter     *bufio.Writer
	csvWriter     *csv.Writer
	header        []string
	headerWritten bool
	mu            sync.Mutex

	// Batch buffering fields.
	batchSize int
	pending   []T
	batchMu   sync.Mutex
}

// NewCSVAppender creates a new CSVAppender for elements of type T.
// The batchSize parameter defines how many items are accumulated before a flush.
func NewCSVAppender[T any](filePath string, appendMode bool, batchSize int) (*CSVAppender[T], error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batchSize must be greater than zero")
	}
	var f *os.File
	var err error
	var headerWritten bool
	if appendMode {
		f, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("open CSV file in append mode: %w", err)
		}
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat CSV file: %w", err)
		}
		headerWritten = info.Size() > 0
	} else {
		f, err = os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("create CSV file: %w", err)
		}
		headerWritten = false
	}
	bufWriter := bufio.NewWriter(f)
	return &CSVAppender[T]{
		filePath:      filePath,
		file:          f,
		bufWriter:     bufWriter,
		csvWriter:     csv.NewWriter(bufWriter),
		headerWritten: headerWritten,
		batchSize:     batchSize,
		pending:       make([]T, 0, batchSize),
	}, nil
}

// Append appends a single record of type T, accumulating in a batch.
func (ca *CSVAppender[T]) Append(record T) error {
	ca.batchMu.Lock()
	ca.pending = append(ca.pending, record)
	if len(ca.pending) >= ca.batchSize {
		batch := make([]T, len(ca.pending))
		copy(batch, ca.pending)
		ca.pending = nil
		ca.batchMu.Unlock()
		if err := ca.AppendBatch(batch); err != nil {
			ca.batchMu.Lock()
			ca.pending = append(batch, ca.pending...)
			ca.batchMu.Unlock()
			return err
		}
		return nil
	}
	ca.batchMu.Unlock()
	return nil
}

// AppendBatch writes a batch of records to the CSV file.
func (ca *CSVAppender[T]) AppendBatch(records []T) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if len(records) == 0 {
		return nil
	}
	if !ca.headerWritten {
		// Extract header using the first record.
		ca.header = ExtractCSVHeader(records[0])
		if err := ca.csvWriter.Write(ca.header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
		ca.headerWritten = true
	}
	for _, rec := range records {
		row, err := BuildCSVRow(ca.header, rec)
		if err != nil {
			return fmt.Errorf("failed to build CSV row: %w", err)
		}
		if err := ca.csvWriter.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}
	ca.csvWriter.Flush()
	if err := ca.csvWriter.Error(); err != nil {
		return fmt.Errorf("csv writer flush error: %w", err)
	}
	return ca.bufWriter.Flush()
}

// readAllRecords reads the entire CSV file and returns all records of type T.
// It uses ProcessCSVFile. It is assumed that T is compatible with utils.Record.
func (ca *CSVAppender[T]) readAllRecords() ([]T, error) {
	var records []T
	err := ProcessCSVFile(ca.filePath, func(rec map[string]any) {
		// Try to convert rec to type T.
		if cast, ok := any(rec).(T); ok {
			records = append(records, cast)
		}
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// DeleteRecord reads all records, removes those for which predicate returns true,
// and rewrites the CSV file.
func (ca *CSVAppender[T]) DeleteRecord(predicate func(T) bool) error {
	// Flush pending records.
	ca.batchMu.Lock()
	pendingBatch := make([]T, len(ca.pending))
	copy(pendingBatch, ca.pending)
	ca.pending = nil
	ca.batchMu.Unlock()
	if len(pendingBatch) > 0 {
		if err := ca.AppendBatch(pendingBatch); err != nil {
			return err
		}
	}

	ca.mu.Lock()
	defer ca.mu.Unlock()
	records, err := ca.readAllRecords()
	if err != nil {
		return err
	}
	filtered := make([]T, 0, len(records))
	for _, r := range records {
		if !predicate(r) {
			filtered = append(filtered, r)
		}
	}
	// Rewrite the CSV file.
	f, err := os.Create(ca.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if len(ca.header) > 0 {
		if err := w.Write(ca.header); err != nil {
			return err
		}
	}
	for _, rec := range filtered {
		row, err := BuildCSVRow(ca.header, rec)
		if err != nil {
			return err
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// SearchRecords returns all records for which predicate returns true.
func (ca *CSVAppender[T]) SearchRecords(predicate func(T) bool) ([]T, error) {
	// Flush pending records.
	ca.batchMu.Lock()
	pendingBatch := make([]T, len(ca.pending))
	copy(pendingBatch, ca.pending)
	ca.pending = nil
	ca.batchMu.Unlock()
	if len(pendingBatch) > 0 {
		if err := ca.AppendBatch(pendingBatch); err != nil {
			return nil, err
		}
	}
	ca.mu.Lock()
	defer ca.mu.Unlock()
	records, err := ca.readAllRecords()
	if err != nil {
		return nil, err
	}
	var result []T
	for _, r := range records {
		if predicate(r) {
			result = append(result, r)
		}
	}
	return result, nil
}

// ForEach iterates over each record in the CSV file (after flushing pending records)
// and calls the provided callback.
func (ca *CSVAppender[T]) ForEach(callback func(T)) error {
	// Flush pending records.
	ca.batchMu.Lock()
	pendingBatch := make([]T, len(ca.pending))
	copy(pendingBatch, ca.pending)
	ca.pending = nil
	ca.batchMu.Unlock()
	if len(pendingBatch) > 0 {
		if err := ca.AppendBatch(pendingBatch); err != nil {
			return err
		}
	}
	ca.mu.Lock()
	defer ca.mu.Unlock()
	records, err := ca.readAllRecords()
	if err != nil {
		return err
	}
	for _, r := range records {
		callback(r)
	}
	return nil
}

// Close flushes pending records and closes the underlying CSV file.
func (ca *CSVAppender[T]) Close() error {
	ca.batchMu.Lock()
	batch := make([]T, len(ca.pending))
	copy(batch, ca.pending)
	ca.pending = nil
	ca.batchMu.Unlock()
	if len(batch) > 0 {
		if err := ca.AppendBatch(batch); err != nil {
			return err
		}
	}
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.csvWriter.Flush()
	if err := ca.csvWriter.Error(); err != nil {
		return err
	}
	if ca.bufWriter != nil {
		if err := ca.bufWriter.Flush(); err != nil {
			return err
		}
	}
	if ca.file != nil {
		return ca.file.Close()
	}
	return nil
}
