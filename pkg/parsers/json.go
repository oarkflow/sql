package parsers

import (
	"encoding/json"
)

// JSONParser parses JSON data
type JSONParser struct{}

// NewJSONParser creates a new JSON parser
func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

// Name returns the parser name
func (p *JSONParser) Name() string {
	return "JSON"
}

// Detect checks if the data is valid JSON
func (p *JSONParser) Detect(data []byte) bool {
	var temp interface{}
	return json.Unmarshal(data, &temp) == nil
}

// Parse parses the JSON data
func (p *JSONParser) Parse(data []byte) (interface{}, error) {
	var result interface{}
	err := json.Unmarshal(data, &result)
	return result, err
}
