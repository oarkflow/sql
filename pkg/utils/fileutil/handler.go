package fileutil

import (
	"fmt"
	"strconv"

	"github.com/oarkflow/etl/pkg/contract"
)

func NewAppender[T any](file, extension string, appendMode bool) (contract.Appender[T], error) {
	switch extension {
	case "json":
		return NewJSONAppender[T](file, appendMode)
	case "csv":
		return NewCSVAppender[T](file, appendMode)
	default:
		return nil, fmt.Errorf("unsupported file extension: %s", extension)
	}
}

func ExtractCSVHeader(rec any) []string {
	switch rec := rec.(type) {
	case map[string]any:
		header := make([]string, 0, len(rec))
		for key := range rec {
			header = append(header, key)
		}
		return header
	}
	return []string{}
}

func BuildCSVRow(header []string, rec any) ([]string, error) {
	row := make([]string, len(header))
	switch rec := rec.(type) {
	case map[string]any:
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
	return row, nil
}
