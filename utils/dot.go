package utils

import (
	"encoding/json"
	"strings"

	"github.com/buger/jsonparser"
)

func DotGet(rec Record, dotPath string) (any, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	keys := strings.Split(dotPath, ".")
	value, dataType, _, err := jsonparser.Get(data, keys...)
	if err != nil {
		return nil, err
	}
	switch dataType {
	case jsonparser.String:
		return string(value), nil
	case jsonparser.Number:
		return string(value), nil
	case jsonparser.Boolean:
		return string(value), nil
	default:
		return value, nil
	}
}
