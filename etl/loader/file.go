package loader

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/oarkflow/sql/utils"
)

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
