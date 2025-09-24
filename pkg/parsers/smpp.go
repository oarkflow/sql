package parsers

import (
	"encoding/binary"
	"errors"
)

// SMPPParser parses SMPP PDUs
type SMPPParser struct{}

// NewSMPPParser creates a new SMPP parser
func NewSMPPParser() *SMPPParser {
	return &SMPPParser{}
}

// Name returns the parser name
func (p *SMPPParser) Name() string {
	return "SMPP"
}

// Detect checks if the data is an SMPP PDU
func (p *SMPPParser) Detect(data []byte) bool {
	if len(data) < 16 { // Minimum SMPP PDU length
		return false
	}
	commandLength := binary.BigEndian.Uint32(data[0:4])
	return int(commandLength) == len(data)
}

// Parse parses the SMPP PDU into a map
func (p *SMPPParser) Parse(data []byte) (interface{}, error) {
	if len(data) < 16 {
		return nil, errors.New("data too short for SMPP PDU")
	}

	result := make(map[string]interface{})
	result["command_length"] = binary.BigEndian.Uint32(data[0:4])
	result["command_id"] = binary.BigEndian.Uint32(data[4:8])
	result["command_status"] = binary.BigEndian.Uint32(data[8:12])
	result["sequence_number"] = binary.BigEndian.Uint32(data[12:16])

	// Parse body based on command_id, but for simplicity, just the header
	// Add more fields as needed for specific commands

	return result, nil
}
