package fileutil

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
)

type CSVAppender[T any] struct {
	file          *os.File
	bufWriter     *bufio.Writer
	csvWriter     *csv.Writer
	header        []string
	headerWritten bool
	mu            sync.Mutex

	// New fields for batching.
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
		file:          f,
		bufWriter:     bufWriter,
		csvWriter:     csv.NewWriter(bufWriter),
		headerWritten: headerWritten,
		batchSize:     batchSize,
		pending:       make([]T, 0, batchSize),
	}, nil
}

// Append appends a single record of type T.
// It accumulates records until the batchSize is reached, then flushes them using AppendBatch.
// In case of a flush error, the batch is requeued so that no records are lost.
func (ca *CSVAppender[T]) Append(record T) error {
	ca.batchMu.Lock()
	ca.pending = append(ca.pending, record)
	if len(ca.pending) >= ca.batchSize {
		batch := make([]T, len(ca.pending))
		copy(batch, ca.pending)
		ca.pending = nil
		ca.batchMu.Unlock()
		if err := ca.AppendBatch(batch); err != nil {
			// Requeue batch on error.
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
		// ExtractCSVHeader should be implemented to return a slice of strings representing the CSV header.
		ca.header = ExtractCSVHeader(records[0])
		if err := ca.csvWriter.Write(ca.header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
		ca.headerWritten = true
	}

	for _, rec := range records {
		// BuildCSVRow should be implemented to build a row (slice of strings) from the record and header.
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

// Close flushes any remaining pending records and closes the underlying file.
func (ca *CSVAppender[T]) Close() error {
	// Flush any remaining pending records.
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
