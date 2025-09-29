package parsers

// PlainTextParser parses plain text data
type PlainTextParser struct{}

// NewPlainTextParser creates a new plain text parser
func NewPlainTextParser() *PlainTextParser {
	return &PlainTextParser{}
}

// Name returns the parser name
func (p *PlainTextParser) Name() string {
	return "PlainText"
}

// Detect always returns true as fallback
func (p *PlainTextParser) Detect(data []byte) bool {
	return true
}

// Parse returns the data as a string
func (p *PlainTextParser) Parse(data []byte) (any, error) {
	return string(data), nil
}
