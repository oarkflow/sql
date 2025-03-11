package fileutil

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/oarkflow/json"

	"github.com/oarkflow/etl/pkg/utils"
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
