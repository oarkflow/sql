package loader

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/oarkflow/sql/utils"
)

type FileLoader struct {
	fileName  string
	extension string
	file      *os.File
	bufWriter *bufio.Writer
	csvWriter *csv.Writer
	// jsonFirstRecord is used to track whether we've written the first record in the JSON array.
	jsonFirstRecord bool
	csvHeader       []string
	headerWritten   bool
}

func NewFileLoader(fileName string) *FileLoader {
	extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
	return &FileLoader{
		fileName:        fileName,
		extension:       extension,
		jsonFirstRecord: true,
	}
}

func (fl *FileLoader) Setup() error {
	f, err := os.Create(fl.fileName)
	if err != nil {
		return err
	}
	fl.file = f
	fl.bufWriter = bufio.NewWriter(f)
	switch fl.extension {
	case "json":
		// Write the opening bracket for a JSON array.
		if _, err := fl.bufWriter.WriteString("[\n"); err != nil {
			return fmt.Errorf("failed to write JSON array start: %w", err)
		}
		fl.jsonFirstRecord = true
	case "csv":
		fl.csvWriter = csv.NewWriter(fl.bufWriter)
	default:
		return fmt.Errorf("unsupported file extension: %s", fl.extension)
	}
	return nil
}

func (fl *FileLoader) LoadBatch(records []utils.Record) error {
	switch fl.extension {
	case "json":
		for _, rec := range records {
			// Add comma if not the first record.
			if !fl.jsonFirstRecord {
				if _, err := fl.bufWriter.WriteString(",\n"); err != nil {
					return fmt.Errorf("failed to write comma: %w", err)
				}
			}
			// Marshal the record.
			data, err := json.Marshal(rec)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON record: %w", err)
			}
			if _, err := fl.bufWriter.WriteString("\t"); err != nil {
				return fmt.Errorf("failed to write tab: %w", err)
			}
			if _, err := fl.bufWriter.Write(data); err != nil {
				return fmt.Errorf("failed to write JSON record: %w", err)
			}
			fl.jsonFirstRecord = false
		}
	case "csv":
		if !fl.headerWritten && len(records) > 0 {
			fl.csvHeader = extractCSVHeader(records[0])
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
	// For JSON, close the array.
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
