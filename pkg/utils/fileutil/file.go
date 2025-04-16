package fileutil

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq/utils"
)

func ProcessFile(filename string, callbacks ...func(utils.Record)) ([]utils.Record, error) {
	var callback func(utils.Record)
	if len(callbacks) > 0 {
		callback = callbacks[0]
	}
	var records []utils.Record
	processCallback := func(record utils.Record) {
		if callback != nil {
			callback(record)
		} else {
			records = append(records, record)
		}
	}
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		if err := ProcessJSONStream(filename, processCallback); err != nil {
			return nil, err
		}
	case ".csv":
		if err := ProcessCSVFile(filename, processCallback); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}
	return records, nil
}

func ProcessJSONStream(filename string, callback func(utils.Record)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := json.NewDecoder(file)
	_, err = decoder.Token()
	if err != nil {
		return err
	}
	for decoder.More() {
		var obj utils.Record
		if err := decoder.Decode(&obj); err != nil {
			return err
		}
		if callback != nil {
			callback(obj)
		}
	}
	return nil
}

func ProcessCSVFile(filename string, callback func(utils.Record)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.ReuseRecord = true
	headers, err := reader.Read()
	if err != nil {
		return err
	}
	headerCopy := make([]string, len(headers))
	copy(headerCopy, headers)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		row := make(utils.Record)
		for i, header := range headerCopy {
			if i < len(record) {
				row[header] = record[i]
			} else {
				row[header] = nil
			}
		}
		if callback != nil {
			callback(row)
		}
	}
	return nil
}

// DeleteRecord removes all records from the file that satisfy the predicate.
// The file must be either a JSON array or a CSV file.
// For JSON files, it rewrites the JSON array without the matching records.
// For CSV files, it preserves the header if present and rewrites the remaining rows.
func DeleteRecord(filename string, predicate func(utils.Record) bool) error {
	// Read existing records using ProcessFile.
	records, err := ProcessFile(filename)
	if err != nil {
		return fmt.Errorf("failed to process file %s: %w", filename, err)
	}

	// Filter out records which match the predicate (i.e. records to delete).
	var keptRecords []utils.Record
	for _, record := range records {
		if !predicate(record) {
			keptRecords = append(keptRecords, record)
		}
	}

	// Determine file type by its extension.
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		// Write the filtered records back as a JSON array.
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create JSON file %s: %w", filename, err)
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ") // Pretty-print with indentation.
		if err := encoder.Encode(keptRecords); err != nil {
			return fmt.Errorf("failed to write JSON records to file %s: %w", filename, err)
		}
		return nil

	case ".csv":
		// Write the records back in CSV format.
		// Determine header: if there is at least one record, we extract the header keys.
		var header []string
		if len(keptRecords) > 0 {
			header = ExtractCSVHeader(keptRecords[0])
		}

		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create CSV file %s: %w", filename, err)
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		// Write header if available.
		if len(header) > 0 {
			if err := writer.Write(header); err != nil {
				return fmt.Errorf("failed to write CSV header: %w", err)
			}
		}

		// Write each record as a row.
		for _, rec := range keptRecords {
			row, err := BuildCSVRow(header, rec)
			if err != nil {
				return fmt.Errorf("failed to build CSV row: %w", err)
			}
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return fmt.Errorf("csv writer flush error: %w", err)
		}
		return nil

	default:
		return fmt.Errorf("unsupported file extension: %s", ext)
	}
}

// SearchRecords returns all records from the file that satisfy the predicate.
// The file must be either a JSON array or a CSV file.
func SearchRecords(filename string, predicate func(utils.Record) bool) ([]utils.Record, error) {
	// Process all records from the file.
	records, err := ProcessFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to process file %s: %w", filename, err)
	}

	// Filter the records using the predicate.
	var result []utils.Record
	for _, record := range records {
		if predicate(record) {
			result = append(result, record)
		}
	}
	return result, nil
}

// ForEach processes every record in the file by calling the callback function.
// The file must be either a JSON array or a CSV file.
func ForEach(filename string, callback func(utils.Record)) error {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		return ProcessJSONStream(filename, callback)
	case ".csv":
		return ProcessCSVFile(filename, callback)
	default:
		return fmt.Errorf("unsupported file extension: %s", ext)
	}
}
