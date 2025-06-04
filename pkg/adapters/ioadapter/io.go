package ioadapter

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/log"

	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type Adapter struct {
	mode   string
	reader io.Reader
	writer io.Writer
	format string
}

func NewSource(reader io.Reader, format string) *Adapter {
	return &Adapter{
		mode:   "source",
		reader: reader,
		format: strings.ToLower(format),
	}
}

func NewLoader(writer io.Writer, format string) *Adapter {
	return &Adapter{
		mode:   "loader",
		writer: writer,
		format: strings.ToLower(format),
	}
}

func (ioa *Adapter) Setup(_ context.Context) error {
	return nil
}

func (ioa *Adapter) Extract(_ context.Context, _ ...contracts.Option) (<-chan utils.Record, error) {
	if ioa.mode != "source" {
		return nil, fmt.Errorf("IOAdapter not configured as source")
	}
	out := make(chan utils.Record)
	go func() {
		switch ioa.format {
		case "csv":
			var data string
			if isInteractive(ioa.reader) {
				_, _ = fmt.Fprintln(os.Stdout, "Enter CSV data (press Enter twice to finish):")
				data = readUntilDoubleEnter(ioa.reader)
			} else {

				b, err := io.ReadAll(ioa.reader)
				if err != nil {
					log.Printf("Error reading input: %v", err)
					break
				}
				data = string(b)
			}
			csvReader := csv.NewReader(strings.NewReader(data))
			records, err := csvReader.ReadAll()
			if err != nil {
				log.Printf("CSV read error: %v", err)
				break
			}
			if len(records) < 1 {
				break
			}

			header := records[0]
			for _, row := range records[1:] {
				rec := make(utils.Record)
				for i, field := range row {
					if i < len(header) {
						rec[header[i]] = field
					}
				}
				out <- rec
			}
		case "json":
			var data string
			if isInteractive(ioa.reader) {
				_, _ = fmt.Fprintln(os.Stdout, "Enter JSON data (press Enter to finish):")
				data = readUntilDoubleEnter(ioa.reader)
			} else {
				b, err := io.ReadAll(ioa.reader)
				if err != nil {
					log.Printf("Error reading input: %v", err)
					break
				}
				data = string(b)
			}
			trimmed := strings.TrimSpace(data)
			dec := json.NewDecoder(strings.NewReader(data))
			if len(trimmed) > 0 && trimmed[0] == '[' {

				_, err := dec.Token()
				if err != nil {
					log.Printf("JSON array token error: %v", err)
					break
				}
				for dec.More() {
					var record map[string]any
					if err := dec.Decode(&record); err != nil {
						log.Printf("JSON decode error: %v", err)
						continue
					}
					out <- record
				}
				_, _ = dec.Token()
			} else {

				scanner := bufio.NewScanner(strings.NewReader(data))
				for scanner.Scan() {
					line := scanner.Text()
					if strings.TrimSpace(line) == "" {
						continue
					}
					var record map[string]any
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						log.Printf("JSON unmarshal error: %v", err)
						continue
					}
					out <- record
				}
			}
		default:

			scanner := bufio.NewScanner(ioa.reader)

			const maxCapacity = 1024 * 1024
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, maxCapacity)
			for scanner.Scan() {
				line := scanner.Text()
				rec := utils.Record{"line": line}
				out <- rec
			}
			if err := scanner.Err(); err != nil {
				log.Printf("Scanner error: %v", err)
			}
		}
		close(out)
	}()
	return out, nil
}

func (ioa *Adapter) StoreBatch(_ context.Context, records []utils.Record) error {
	switch ioa.format {
	case "csv":
		w := csv.NewWriter(ioa.writer)
		if len(records) == 0 {
			return nil
		}
		header := extractCSVHeader(records[0])
		if err := w.Write(header); err != nil {
			return err
		}
		for _, rec := range records {
			row, err := buildCSVRow(header, rec)
			if err != nil {
				return err
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	case "json":
		for _, rec := range records {
			data, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			_, err = ioa.writer.Write(data)
			if err != nil {
				return err
			}
			_, err = ioa.writer.Write([]byte("\n"))
			if err != nil {
				return err
			}
		}
	default:
		for _, rec := range records {
			line := fmt.Sprintf("%v", rec)
			_, err := ioa.writer.Write([]byte(line + "\n"))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ioa *Adapter) StoreSingle(_ context.Context, rec utils.Record) error {
	// For single record mode, simply use ioa.writer.
	switch ioa.format {
	case "csv":
		w := csv.NewWriter(ioa.writer)
		// Write header only if needed (assume already written if not interactive)
		header := extractCSVHeader(rec)
		if err := w.Write(header); err != nil {
			return err
		}
		row, err := buildCSVRow(header, rec)
		if err != nil {
			return err
		}
		if err := w.Write(row); err != nil {
			return err
		}
		w.Flush()
		return w.Error()
	case "json":
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		_, err = ioa.writer.Write(data)
		if err != nil {
			return err
		}
		_, err = ioa.writer.Write([]byte("\n"))
		return err
	default:
		line := fmt.Sprintf("%v", rec)
		_, err := ioa.writer.Write([]byte(line + "\n"))
		return err
	}
}

func (ioa *Adapter) Close() error {
	if ioa.mode == "source" {
		if closer, ok := ioa.reader.(io.Closer); ok {
			return closer.Close()
		}
	} else if ioa.mode == "loader" {
		if closer, ok := ioa.writer.(io.Closer); ok {
			return closer.Close()
		}
	}
	return nil
}

func extractCSVHeader(rec utils.Record) []string {
	var header []string
	for k := range rec {
		header = append(header, k)
	}
	sort.Strings(header)
	return header
}

func buildCSVRow(header []string, rec utils.Record) ([]string, error) {
	row := make([]string, len(header))
	for i, key := range header {
		if val, ok := rec[key]; ok {
			row[i] = fmt.Sprintf("%v", val)
		} else {
			row[i] = ""
		}
	}
	return row, nil
}

func isInteractive(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func readUntilDoubleEnter(r io.Reader) string {
	scanner := bufio.NewScanner(r)
	var lines []string
	blankCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			blankCount++
			if blankCount >= 1 {
				break
			}
		} else {
			blankCount = 0
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}
