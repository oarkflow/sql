package platform

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
)

// CSVParser parses CSV data
type CSVParser struct {
	delimiter rune
	hasHeader bool
}

func NewCSVParser(delimiter rune, hasHeader bool) *CSVParser {
	return &CSVParser{
		delimiter: delimiter,
		hasHeader: hasHeader,
	}
}

func (p *CSVParser) Parse(data []byte, contentType string) ([]Record, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.Comma = p.delimiter

	records := []Record{}

	// Read all records
	csvRecords, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(csvRecords) == 0 {
		return records, nil
	}

	var headers []string
	if p.hasHeader {
		headers = csvRecords[0]
		csvRecords = csvRecords[1:]
	} else {
		// Generate column names
		headers = make([]string, len(csvRecords[0]))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i+1)
		}
	}

	// Convert to records
	for _, row := range csvRecords {
		record := make(Record)
		for i, value := range row {
			if i < len(headers) {
				record[headers[i]] = value
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func (p *CSVParser) Name() string {
	return "csv"
}

// JSONParser parses JSON data
type JSONParser struct{}

func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

func (p *JSONParser) Parse(data []byte, contentType string) ([]Record, error) {
	records := []Record{}

	// Try to parse as JSON array first
	var jsonArray []map[string]interface{}
	if err := json.Unmarshal(data, &jsonArray); err == nil {
		for _, item := range jsonArray {
			record := make(Record)
			for k, v := range item {
				record[k] = v
			}
			records = append(records, record)
		}
		return records, nil
	}

	// Try to parse as single JSON object
	var jsonObject map[string]interface{}
	if err := json.Unmarshal(data, &jsonObject); err == nil {
		record := make(Record)
		for k, v := range jsonObject {
			record[k] = v
		}
		records = append(records, record)
		return records, nil
	}

	// Try to parse as newline-delimited JSON
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var item map[string]interface{}
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, fmt.Errorf("failed to parse JSON line: %w", err)
		}

		record := make(Record)
		for k, v := range item {
			record[k] = v
		}
		records = append(records, record)
	}

	return records, nil
}

func (p *JSONParser) Name() string {
	return "json"
}

// XMLParser parses XML data
type XMLParser struct{}

func NewXMLParser() *XMLParser {
	return &XMLParser{}
}

func (p *XMLParser) Parse(data []byte, contentType string) ([]Record, error) {
	// Simple XML parsing - in production, use a proper XML parser
	xmlStr := string(data)

	// This is a very basic implementation
	// For production, consider using encoding/xml or a more robust parser

	records := []Record{}

	// Split by root elements (very simplistic)
	parts := strings.Split(xmlStr, "<record>")
	for _, part := range parts[1:] {
		endIndex := strings.Index(part, "</record>")
		if endIndex == -1 {
			continue
		}

		recordXML := part[:endIndex]
		record := p.parseXMLRecord(recordXML)
		records = append(records, record)
	}

	return records, nil
}

func (p *XMLParser) parseXMLRecord(xmlStr string) Record {
	record := make(Record)

	// Very basic XML field extraction
	// This should be replaced with proper XML parsing
	fields := []string{"id", "name", "value", "timestamp"}
	for _, field := range fields {
		startTag := "<" + field + ">"
		endTag := "</" + field + ">"

		start := strings.Index(xmlStr, startTag)
		if start == -1 {
			continue
		}
		start += len(startTag)

		end := strings.Index(xmlStr[start:], endTag)
		if end == -1 {
			continue
		}

		value := xmlStr[start : start+end]
		record[field] = value
	}

	return record
}

func (p *XMLParser) Name() string {
	return "xml"
}

// HL7Parser parses HL7 messages
type HL7Parser struct{}

func NewHL7Parser() *HL7Parser {
	return &HL7Parser{}
}

func (p *HL7Parser) Parse(data []byte, contentType string) ([]Record, error) {
	hl7Str := string(data)

	// Basic HL7 parsing
	// HL7 messages are pipe-delimited with segments
	lines := strings.Split(hl7Str, "\r")

	record := make(Record)
	record["raw_message"] = hl7Str

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			continue
		}

		segmentType := parts[0]
		record["segment_"+segmentType] = strings.Join(parts[1:], "|")

		// Parse common segments
		switch segmentType {
		case "MSH":
			if len(parts) > 8 {
				record["message_type"] = parts[8]
			}
		case "PID":
			if len(parts) > 5 {
				record["patient_id"] = parts[3]
				record["patient_name"] = parts[5]
			}
		case "OBR":
			if len(parts) > 4 {
				record["observation_id"] = parts[2]
				record["observation_name"] = parts[4]
			}
		}
	}

	return []Record{record}, nil
}

func (p *HL7Parser) Name() string {
	return "hl7"
}

// TSVParser parses Tab-Separated Values
type TSVParser struct {
	hasHeader bool
}

func NewTSVParser(hasHeader bool) *TSVParser {
	return &TSVParser{hasHeader: hasHeader}
}

func (p *TSVParser) Parse(data []byte, contentType string) ([]Record, error) {
	// TSV is similar to CSV but with tab delimiter
	csvParser := NewCSVParser('\t', p.hasHeader)
	return csvParser.Parse(data, contentType)
}

func (p *TSVParser) Name() string {
	return "tsv"
}

// FixedWidthParser parses fixed-width text files
type FixedWidthParser struct {
	fieldWidths []int
	fieldNames  []string
}

func NewFixedWidthParser(fieldWidths []int, fieldNames []string) *FixedWidthParser {
	return &FixedWidthParser{
		fieldWidths: fieldWidths,
		fieldNames:  fieldNames,
	}
}

func (p *FixedWidthParser) Parse(data []byte, contentType string) ([]Record, error) {
	lines := strings.Split(string(data), "\n")
	records := []Record{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		record := make(Record)
		pos := 0

		for i, width := range p.fieldWidths {
			if pos+width > len(line) {
				break
			}

			value := strings.TrimSpace(line[pos : pos+width])
			fieldName := fmt.Sprintf("field%d", i+1)
			if i < len(p.fieldNames) {
				fieldName = p.fieldNames[i]
			}

			record[fieldName] = value
			pos += width
		}

		records = append(records, record)
	}

	return records, nil
}

func (p *FixedWidthParser) Name() string {
	return "fixed-width"
}

// StringParser parses plain text strings
type StringParser struct{}

func NewStringParser() *StringParser {
	return &StringParser{}
}

func (p *StringParser) Parse(data []byte, contentType string) ([]Record, error) {
	record := make(Record)
	record["content"] = string(data)
	record["length"] = len(data)

	return []Record{record}, nil
}

func (p *StringParser) Name() string {
	return "string"
}

// ParserFactory creates parsers based on content type
type ParserFactory struct{}

func NewParserFactory() *ParserFactory {
	return &ParserFactory{}
}

func (f *ParserFactory) CreateParser(contentType string) Parser {
	switch {
	case strings.Contains(contentType, "csv"):
		return NewCSVParser(',', true)
	case strings.Contains(contentType, "tsv") || strings.Contains(contentType, "tab-separated"):
		return NewTSVParser(true)
	case strings.Contains(contentType, "json"):
		return NewJSONParser()
	case strings.Contains(contentType, "xml"):
		return NewXMLParser()
	case strings.Contains(contentType, "hl7"):
		return NewHL7Parser()
	default:
		// Default to string parser
		return NewStringParser()
	}
}

// DetectContentType attempts to detect content type from data
func DetectContentType(data []byte) string {
	str := string(data)

	// Check for JSON
	if (strings.HasPrefix(strings.TrimSpace(str), "{") && strings.HasSuffix(strings.TrimSpace(str), "}")) ||
		(strings.HasPrefix(strings.TrimSpace(str), "[") && strings.HasSuffix(strings.TrimSpace(str), "]")) {
		return "application/json"
	}

	// Check for XML
	if strings.Contains(str, "<?xml") || strings.Contains(str, "<record>") {
		return "application/xml"
	}

	// Check for CSV (look for commas and newlines)
	lines := strings.Split(str, "\n")
	if len(lines) > 1 {
		commaCount := strings.Count(lines[0], ",")
		if commaCount > 0 && commaCount == strings.Count(lines[1], ",") {
			return "text/csv"
		}
	}

	// Check for HL7 (pipe characters and MSH segment)
	if strings.Contains(str, "MSH|") {
		return "application/hl7"
	}

	return "text/plain"
}
