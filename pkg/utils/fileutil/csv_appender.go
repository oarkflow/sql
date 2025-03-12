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
}

func NewCSVAppender[T any](filePath string, appendMode bool) (*CSVAppender[T], error) {
	var f *os.File
	var err error
	if appendMode {
		f, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("open CSV file in append mode: %w", err)
		}
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat CSV file: %w", err)
		}
		headerWritten := info.Size() > 0
		bufWriter := bufio.NewWriter(f)
		return &CSVAppender[T]{
			file:          f,
			bufWriter:     bufWriter,
			csvWriter:     csv.NewWriter(bufWriter),
			headerWritten: headerWritten,
		}, nil
	}

	f, err = os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("create CSV file: %w", err)
	}
	bufWriter := bufio.NewWriter(f)
	return &CSVAppender[T]{
		file:          f,
		bufWriter:     bufWriter,
		csvWriter:     csv.NewWriter(bufWriter),
		headerWritten: false,
	}, nil
}

func (ca *CSVAppender[T]) Append(record T) error {
	return ca.AppendBatch([]T{record})
}

func (ca *CSVAppender[T]) AppendBatch(records []T) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if len(records) == 0 {
		return nil
	}

	if !ca.headerWritten {
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

func (ca *CSVAppender[T]) Close() error {
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
