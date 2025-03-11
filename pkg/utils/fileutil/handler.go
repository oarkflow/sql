package fileutil

import (
	"fmt"
	"strconv"
)

// --- Helper functions ---

func ExtractCSVHeader(rec map[string]interface{}) []string {
	header := make([]string, 0, len(rec))
	for key := range rec {
		header = append(header, key)
	}
	// Optionally sort header if needed.
	return header
}

func BuildCSVRow(header []string, rec map[string]interface{}) ([]string, error) {
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
