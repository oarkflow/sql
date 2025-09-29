package parsers

import (
	"encoding/xml"
)

// XMLParser parses XML data
type XMLParser struct{}

// NewXMLParser creates a new XML parser
func NewXMLParser() *XMLParser {
	return &XMLParser{}
}

// Name returns the parser name
func (p *XMLParser) Name() string {
	return "XML"
}

// Detect checks if the data is valid XML
func (p *XMLParser) Detect(data []byte) bool {
	var temp any
	return xml.Unmarshal(data, &temp) == nil
}

// Parse parses the XML data
func (p *XMLParser) Parse(data []byte) (any, error) {
	var result any
	err := xml.Unmarshal(data, &result)
	return result, err
}
