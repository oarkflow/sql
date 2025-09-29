package parsers

// Parser defines the interface for parsing different data formats
type Parser interface {
	// Parse attempts to parse the given data and returns the parsed result or an error
	Parse(data []byte) (any, error)
	// Name returns the name of the parser
	Name() string
	// Detect checks if the data matches this parser's format
	Detect(data []byte) bool
}
