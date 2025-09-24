package parsers

import (
	"fmt"
	"strings"
	"time"
)

// HL7Parser represents an HL7 message parser
type HL7Parser struct {
	fieldSeparator        string
	componentSeparator    string
	subcomponentSeparator string
	repetitionSeparator   string
	escapeCharacter       string
}

// NewHL7Parser creates a new HL7 parser
func NewHL7Parser() *HL7Parser {
	return &HL7Parser{
		fieldSeparator:        "|",
		componentSeparator:    "^",
		subcomponentSeparator: "&",
		repetitionSeparator:   "~",
		escapeCharacter:       "\\",
	}
}

// Detect checks if the data is a valid HL7 message
func (p *HL7Parser) Detect(data []byte) bool {
	message := string(data)
	lines := strings.Split(strings.TrimSpace(message), "\r")
	if len(lines) == 1 {
		lines = strings.Split(strings.TrimSpace(message), "\n")
	}
	if len(lines) == 0 {
		return false
	}

	// Check if first line starts with MSH
	mshLine := strings.TrimSpace(lines[0])
	return strings.HasPrefix(mshLine, "MSH")
}

// Parse parses an HL7 message into a generic map structure
func (p *HL7Parser) Parse(message string) (map[string]interface{}, error) {
	// Try splitting on \r first, then \n if that doesn't work
	lines := strings.Split(strings.TrimSpace(message), "\r")
	if len(lines) == 1 {
		// If no \r found, try \n
		lines = strings.Split(strings.TrimSpace(message), "\n")
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	// Parse MSH segment to get delimiters
	mshLine := lines[0]
	if !strings.HasPrefix(mshLine, "MSH") {
		return nil, fmt.Errorf("message does not start with MSH segment")
	}

	// Extract delimiters from MSH
	if len(mshLine) < 8 {
		return nil, fmt.Errorf("MSH segment too short")
	}

	p.fieldSeparator = string(mshLine[3])
	p.componentSeparator = string(mshLine[4])
	p.repetitionSeparator = string(mshLine[5])
	p.escapeCharacter = string(mshLine[6])
	p.subcomponentSeparator = string(mshLine[7])

	result := make(map[string]interface{})

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		segmentName, fields := p.parseSegment(line)
		if segmentName == "" {
			continue
		}

		if _, exists := result[segmentName]; !exists {
			result[segmentName] = []map[string]interface{}{}
		}

		segmentData := p.parseFields(fields)
		result[segmentName] = append(result[segmentName].([]map[string]interface{}), segmentData)
	}

	return result, nil
}

// ParseTyped parses an HL7 message into a typed struct based on message type
func (p *HL7Parser) ParseTyped(message string) (HL7MessageInterface, error) {
	// First parse as generic to get message type
	generic, err := p.Parse(message)
	if err != nil {
		return nil, err
	}

	// Get message type from MSH
	mshSegmentsInterface, ok := generic["MSH"]
	if !ok {
		return nil, fmt.Errorf("no MSH segment found")
	}

	mshSegments, ok := mshSegmentsInterface.([]map[string]interface{})
	if !ok || len(mshSegments) == 0 {
		return nil, fmt.Errorf("invalid MSH segment format")
	}

	mshData := mshSegments[0]
	messageType := p.getMessageType(mshData)

	switch messageType {
	case "ADT^A01":
		return p.parseADT_A01(message)
	case "ADT^A02":
		return p.parseADT_A02(message)
	case "ADT^A03":
		return p.parseADT_A03(message)
	case "ADT^A04":
		return p.parseADT_A04(message)
	case "ADT^A05":
		return p.parseADT_A05(message)
	case "ADT^A06":
		return p.parseADT_A06(message)
	case "ADT^A08":
		return p.parseADT_A08(message)
	case "ADT^A11":
		return p.parseADT_A11(message)
	case "ADT^A12":
		return p.parseADT_A12(message)
	case "ADT^A13":
		return p.parseADT_A13(message)
	case "ORU^R01":
		return p.parseORU_R01(message)
	case "ORM^O01":
		return p.parseORM_O01(message)
	case "ORR^O02":
		return p.parseORR_O02(message)
	case "RAS^O17":
		return p.parseRAS_O17(message)
	case "ACK":
		return p.parseACK(message)
	case "SIU^S12":
		return p.parseSIU_S12(message)
	case "DFT^P03":
		return p.parseDFT_P03(message)
	default:
		return nil, fmt.Errorf("unsupported message type: %s", messageType)
	}
}

// parseSegment parses a single HL7 segment
func (p *HL7Parser) parseSegment(line string) (string, []string) {
	if len(line) < 3 {
		return "", nil
	}

	segmentName := line[:3]
	if len(line) <= 3 {
		return segmentName, []string{}
	}

	// Split by field separator, but skip the first field (segment name)
	fields := strings.Split(line[4:], p.fieldSeparator)
	return segmentName, fields
}

// parseFields parses the fields of a segment
func (p *HL7Parser) parseFields(fields []string) map[string]interface{} {
	result := make(map[string]interface{})

	for i, field := range fields {
		fieldName := fmt.Sprintf("%d", i+1)
		result[fieldName] = p.parseField(field)
	}

	return result
}

// parseField parses a single field, handling components and repetitions
func (p *HL7Parser) parseField(field string) interface{} {
	if field == "" {
		return field
	}

	// Check for repetitions
	if strings.Contains(field, p.repetitionSeparator) {
		repetitions := strings.Split(field, p.repetitionSeparator)
		result := make([]interface{}, len(repetitions))
		for i, rep := range repetitions {
			result[i] = p.parseComponents(rep)
		}
		return result
	}

	return p.parseComponents(field)
}

// parseComponents parses components within a field
func (p *HL7Parser) parseComponents(field string) interface{} {
	if !strings.Contains(field, p.componentSeparator) {
		return field
	}

	components := strings.Split(field, p.componentSeparator)
	if len(components) == 1 {
		return field
	}

	result := make([]interface{}, len(components))
	for i, comp := range components {
		result[i] = p.parseSubcomponents(comp)
	}

	return result
}

// parseSubcomponents parses subcomponents within a component
func (p *HL7Parser) parseSubcomponents(component string) interface{} {
	if !strings.Contains(component, p.subcomponentSeparator) {
		return component
	}

	subcomponents := strings.Split(component, p.subcomponentSeparator)
	if len(subcomponents) == 1 {
		return component
	}

	result := make([]interface{}, len(subcomponents))
	for i, subcomp := range subcomponents {
		result[i] = subcomp
	}

	return result
}

// getMessageType extracts message type from MSH data
func (p *HL7Parser) getMessageType(mshData map[string]interface{}) string {
	messageTypeField, ok := mshData["8"]
	if !ok {
		return ""
	}

	// Handle different formats of message type field
	switch mt := messageTypeField.(type) {
	case string:
		// Handle format like "ADT^A01^ADT_A01" or "ADT^A01"
		parts := strings.Split(mt, "^")
		if len(parts) >= 2 {
			return parts[0] + "^" + parts[1]
		}
		return mt
	case []interface{}:
		if len(mt) >= 2 {
			if code, ok := mt[0].(string); ok {
				if event, ok := mt[1].(string); ok {
					return code + "^" + event
				}
			}
		}
	}

	return ""
}

// parseADT_A01 parses ADT^A01 message
func (p *HL7Parser) parseADT_A01(message string) (*ADT_A01, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A01{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "NK1":
				result.NK1 = append(result.NK1, p.parseNK1(segmentData))
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			case "AL1":
				result.AL1 = append(result.AL1, p.parseAL1(segmentData))
			case "DG1":
				result.DG1 = append(result.DG1, p.parseDG1(segmentData))
			case "PR1":
				result.PR1 = append(result.PR1, p.parsePR1(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseADT_A02 parses ADT^A02 message
func (p *HL7Parser) parseADT_A02(message string) (*ADT_A02, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A02{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			}
		}
	}

	return result, nil
}

// parseADT_A03 parses ADT^A03 message
func (p *HL7Parser) parseADT_A03(message string) (*ADT_A03, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A03{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			case "AL1":
				result.AL1 = append(result.AL1, p.parseAL1(segmentData))
			case "DG1":
				result.DG1 = append(result.DG1, p.parseDG1(segmentData))
			}
		}
	}

	return result, nil
}

// parseADT_A04 parses ADT^A04 message
func (p *HL7Parser) parseADT_A04(message string) (*ADT_A04, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A04{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "NK1":
				result.NK1 = append(result.NK1, p.parseNK1(segmentData))
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			case "AL1":
				result.AL1 = append(result.AL1, p.parseAL1(segmentData))
			case "DG1":
				result.DG1 = append(result.DG1, p.parseDG1(segmentData))
			case "PR1":
				result.PR1 = append(result.PR1, p.parsePR1(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseADT_A05 parses ADT^A05 message
func (p *HL7Parser) parseADT_A05(message string) (*ADT_A05, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A05{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "NK1":
				result.NK1 = append(result.NK1, p.parseNK1(segmentData))
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			case "AL1":
				result.AL1 = append(result.AL1, p.parseAL1(segmentData))
			case "DG1":
				result.DG1 = append(result.DG1, p.parseDG1(segmentData))
			case "PR1":
				result.PR1 = append(result.PR1, p.parsePR1(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseADT_A06 parses ADT^A06 message
func (p *HL7Parser) parseADT_A06(message string) (*ADT_A06, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A06{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "NK1":
				result.NK1 = append(result.NK1, p.parseNK1(segmentData))
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			case "AL1":
				result.AL1 = append(result.AL1, p.parseAL1(segmentData))
			case "DG1":
				result.DG1 = append(result.DG1, p.parseDG1(segmentData))
			case "PR1":
				result.PR1 = append(result.PR1, p.parsePR1(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseADT_A08 parses ADT^A08 message
func (p *HL7Parser) parseADT_A08(message string) (*ADT_A08, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A08{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "NK1":
				result.NK1 = append(result.NK1, p.parseNK1(segmentData))
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			case "AL1":
				result.AL1 = append(result.AL1, p.parseAL1(segmentData))
			case "DG1":
				result.DG1 = append(result.DG1, p.parseDG1(segmentData))
			case "PR1":
				result.PR1 = append(result.PR1, p.parsePR1(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseADT_A11 parses ADT^A11 message
func (p *HL7Parser) parseADT_A11(message string) (*ADT_A11, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A11{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			}
		}
	}

	return result, nil
}

// parseADT_A12 parses ADT^A12 message
func (p *HL7Parser) parseADT_A12(message string) (*ADT_A12, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A12{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			}
		}
	}

	return result, nil
}

// parseADT_A13 parses ADT^A13 message
func (p *HL7Parser) parseADT_A13(message string) (*ADT_A13, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ADT_A13{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "PV2":
				result.PV2 = p.parsePV2(segmentData)
			}
		}
	}

	return result, nil
}

// parseORU_R01 parses ORU^R01 message
func (p *HL7Parser) parseORU_R01(message string) (*ORU_R01, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ORU_R01{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "ORC":
				result.ORC = append(result.ORC, p.parseORC(segmentData))
			case "OBR":
				result.OBR = append(result.OBR, p.parseOBR(segmentData))
			case "OBX":
				result.OBX = append(result.OBX, p.parseOBX(segmentData))
			case "NTE":
				result.NTE = append(result.NTE, p.parseNTE(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseORM_O01 parses ORM^O01 message
func (p *HL7Parser) parseORM_O01(message string) (*ORM_O01, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ORM_O01{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "ORC":
				result.ORC = p.parseORC(segmentData)
			case "RXE":
				result.RXE = p.parseRXE(segmentData)
			case "RXR":
				result.RXR = append(result.RXR, p.parseRXR(segmentData))
			case "RXC":
				result.RXC = append(result.RXC, p.parseRXC(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseORR_O02 parses ORR^O02 message
func (p *HL7Parser) parseORR_O02(message string) (*ORR_O02, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ORR_O02{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "MSA":
				result.MSA = p.parseMSA(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "ORC":
				result.ORC = p.parseORC(segmentData)
			case "RXE":
				result.RXE = p.parseRXE(segmentData)
			case "RXR":
				result.RXR = append(result.RXR, p.parseRXR(segmentData))
			case "RXC":
				result.RXC = append(result.RXC, p.parseRXC(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseRAS_O17 parses RAS^O17 message
func (p *HL7Parser) parseRAS_O17(message string) (*RAS_O17, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &RAS_O17{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "ORC":
				result.ORC = p.parseORC(segmentData)
			case "RXE":
				result.RXE = p.parseRXE(segmentData)
			case "RXR":
				result.RXR = append(result.RXR, p.parseRXR(segmentData))
			case "RXC":
				result.RXC = append(result.RXC, p.parseRXC(segmentData))
			case "RXA":
				result.RXA = append(result.RXA, p.parseRXA(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseACK parses ACK message
func (p *HL7Parser) parseACK(message string) (*ACK, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &ACK{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "MSA":
				result.MSA = p.parseMSA(segmentData)
			case "ERR":
				result.ERR = append(result.ERR, p.parseERR(segmentData))
			}
		}
	}

	return result, nil
}

// parseSIU_S12 parses SIU^S12 message
func (p *HL7Parser) parseSIU_S12(message string) (*SIU_S12, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &SIU_S12{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "SCH":
				result.SCH = p.parseSCH(segmentData)
			case "RGS":
				result.RGS = append(result.RGS, p.parseRGS(segmentData))
			case "AIG":
				result.AIG = append(result.AIG, p.parseAIG(segmentData))
			case "AIL":
				result.AIL = append(result.AIL, p.parseAIL(segmentData))
			case "AIS":
				result.AIS = append(result.AIS, p.parseAIS(segmentData))
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseDFT_P03 parses DFT^P03 message
func (p *HL7Parser) parseDFT_P03(message string) (*DFT_P03, error) {
	segments, err := p.parseSegments(message)
	if err != nil {
		return nil, err
	}

	result := &DFT_P03{}

	for segmentName, segmentList := range segments {
		for _, segmentData := range segmentList {
			switch segmentName {
			case "MSH":
				result.MSH = p.parseMSH(segmentData)
			case "EVN":
				result.EVN = p.parseEVN(segmentData)
			case "PID":
				result.PID = p.parsePID(segmentData)
			case "PV1":
				result.PV1 = p.parsePV1(segmentData)
			case "FT1":
				result.FT1 = append(result.FT1, p.parseFT1(segmentData))
			case "ROL":
				result.ROL = append(result.ROL, p.parseROL(segmentData))
			}
		}
	}

	return result, nil
}

// parseSegments is a helper function to parse all segments
func (p *HL7Parser) parseSegments(message string) (map[string][]map[string]interface{}, error) {
	// Use the same line splitting logic as Parse
	lines := strings.Split(strings.TrimSpace(message), "\r")
	if len(lines) == 1 {
		// If no \r found, try \n
		lines = strings.Split(strings.TrimSpace(message), "\n")
	}
	result := make(map[string][]map[string]interface{})

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		segmentName, fields := p.parseSegment(line)
		if segmentName == "" {
			continue
		}

		if _, exists := result[segmentName]; !exists {
			result[segmentName] = []map[string]interface{}{}
		}

		segmentData := p.parseFields(fields)
		result[segmentName] = append(result[segmentName], segmentData)
	}

	return result, nil
}

// Helper parsing functions for individual segments
// These would need to be implemented for each segment type
// For brevity, I'll implement a few key ones

func (p *HL7Parser) parseMSH(data map[string]interface{}) *MSH {
	msh := &MSH{}

	// MSH.1 (FieldSeparator) is not transmitted in HL7 messages
	// Fields start from MSH.2
	if val, ok := data["1"]; ok {
		msh.EncodingCharacters = strings.Join(p.getStringSlice(val), p.repetitionSeparator)
	}
	if val, ok := data["2"]; ok {
		msh.SendingApplication = p.parseHD(val)
	}
	if val, ok := data["3"]; ok {
		msh.SendingFacility = p.parseHD(val)
	}
	if val, ok := data["4"]; ok {
		msh.ReceivingApplication = p.parseHD(val)
	}
	if val, ok := data["5"]; ok {
		msh.ReceivingFacility = p.parseHD(val)
	}
	if val, ok := data["6"]; ok {
		msh.DateTimeOfMessage = p.parseTime(val)
	}
	if val, ok := data["7"]; ok {
		msh.Security = p.getString(val)
	}
	if val, ok := data["8"]; ok {
		msh.MessageType = p.parseMSG(val)
	}
	if val, ok := data["9"]; ok {
		msh.MessageControlID = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		msh.ProcessingID = p.parsePT(val)
	}
	if val, ok := data["11"]; ok {
		msh.VersionID = p.parseVID(val)
	}
	if val, ok := data["12"]; ok {
		msh.SequenceNumber = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		msh.ContinuationPointer = p.getString(val)
	}
	if val, ok := data["14"]; ok {
		msh.AcceptAcknowledgmentType = p.getString(val)
	}
	if val, ok := data["16"]; ok {
		msh.ApplicationAcknowledgmentType = p.getString(val)
	}
	if val, ok := data["17"]; ok {
		msh.CountryCode = p.getString(val)
	}
	if val, ok := data["18"]; ok {
		msh.CharacterSet = p.getStringSlice(val)
	}
	if val, ok := data["19"]; ok {
		msh.PrincipalLanguageOfMessage = p.parseCE(val)
	}
	if val, ok := data["20"]; ok {
		msh.AlternateCharacterSetHandlingScheme = p.getString(val)
	}
	if val, ok := data["21"]; ok {
		msh.MessageProfileIdentifier = p.parseEISlice(val)
	}
	if val, ok := data["22"]; ok {
		msh.SendingResponsibleOrganization = p.parseXON(val)
	}
	if val, ok := data["23"]; ok {
		msh.ReceivingResponsibleOrganization = p.parseXON(val)
	}
	if val, ok := data["24"]; ok {
		msh.SendingNetworkAddress = p.getString(val)
	}
	if val, ok := data["25"]; ok {
		msh.ReceivingNetworkAddress = p.getString(val)
	}

	return msh
}

func (p *HL7Parser) parseEVN(data map[string]interface{}) *EVN {
	evn := &EVN{}

	if val, ok := data["1"]; ok {
		evn.EventTypeCode = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		evn.RecordedDateTime = p.parseTime(val)
	}
	if val, ok := data["3"]; ok {
		evn.DateTimePlannedEvent = p.parseTime(val)
	}
	if val, ok := data["4"]; ok {
		evn.EventReasonCode = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		evn.OperatorID = p.parseXCNList(val)
	}
	if val, ok := data["6"]; ok {
		evn.EventOccurred = p.parseTime(val)
	}
	if val, ok := data["7"]; ok {
		evn.EventFacility = p.parseHD(val)
	}

	return evn
}

func (p *HL7Parser) parsePID(data map[string]interface{}) *PID {
	pid := &PID{}

	if val, ok := data["1"]; ok {
		pid.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		pid.PatientID = p.parseCX(val)
	}
	if val, ok := data["3"]; ok {
		pid.PatientIdentifierList = p.parseCXList(val)
	}
	if val, ok := data["4"]; ok {
		pid.AlternatePatientID = p.parseCXList(val)
	}
	if val, ok := data["5"]; ok {
		pid.PatientName = p.parseXPNList(val)
	}
	if val, ok := data["6"]; ok {
		pid.MothersMaidenName = p.parseXPNList(val)
	}
	if val, ok := data["7"]; ok {
		pid.DateTimeOfBirth = p.parseTime(val)
	}
	if val, ok := data["8"]; ok {
		pid.AdministrativeSex = p.getString(val)
	}
	if val, ok := data["9"]; ok {
		pid.PatientAlias = p.parseXPNList(val)
	}
	if val, ok := data["10"]; ok {
		pid.Race = p.parseCESlice(val)
	}
	if val, ok := data["11"]; ok {
		pid.PatientAddress = p.parseXADList(val)
	}
	if val, ok := data["12"]; ok {
		pid.CountyCode = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		pid.PhoneNumberHome = p.parseXTNList(val)
	}
	if val, ok := data["14"]; ok {
		pid.PhoneNumberBusiness = p.parseXTNList(val)
	}
	if val, ok := data["15"]; ok {
		pid.PrimaryLanguage = p.parseCE(val)
	}
	if val, ok := data["16"]; ok {
		pid.MaritalStatus = p.parseCE(val)
	}
	if val, ok := data["17"]; ok {
		pid.Religion = p.parseCE(val)
	}
	if val, ok := data["18"]; ok {
		pid.PatientAccountNumber = p.parseCX(val)
	}
	if val, ok := data["19"]; ok {
		pid.SSNNumberPatient = p.getString(val)
	}
	if val, ok := data["20"]; ok {
		pid.DriversLicenseNumber = p.parseDLN(val)
	}
	if val, ok := data["21"]; ok {
		pid.MothersIdentifier = p.parseCXList(val)
	}
	if val, ok := data["22"]; ok {
		pid.EthnicGroup = p.parseCESlice(val)
	}
	if val, ok := data["23"]; ok {
		pid.BirthPlace = p.getString(val)
	}
	if val, ok := data["24"]; ok {
		pid.MultipleBirthIndicator = p.getString(val)
	}
	if val, ok := data["25"]; ok {
		pid.BirthOrder = p.getString(val)
	}
	if val, ok := data["26"]; ok {
		pid.Citizenship = p.parseCESlice(val)
	}
	if val, ok := data["27"]; ok {
		pid.VeteransMilitaryStatus = p.parseCE(val)
	}
	if val, ok := data["28"]; ok {
		pid.Nationality = p.parseCE(val)
	}
	if val, ok := data["29"]; ok {
		pid.PatientDeathDateAndTime = p.parseTime(val)
	}
	if val, ok := data["30"]; ok {
		pid.PatientDeathIndicator = p.getString(val)
	}
	if val, ok := data["31"]; ok {
		pid.IdentityUnknownIndicator = p.getString(val)
	}
	if val, ok := data["32"]; ok {
		pid.IdentityReliabilityCode = p.parseCESlice(val)
	}
	if val, ok := data["33"]; ok {
		pid.LastUpdateDateTime = p.parseTime(val)
	}
	if val, ok := data["34"]; ok {
		pid.LastUpdateFacility = p.parseHD(val)
	}
	if val, ok := data["35"]; ok {
		pid.SpeciesCode = p.parseCE(val)
	}
	if val, ok := data["36"]; ok {
		pid.BreedCode = p.parseCE(val)
	}
	if val, ok := data["37"]; ok {
		pid.Strain = p.getString(val)
	}
	if val, ok := data["38"]; ok {
		pid.ProductionClassCode = p.parseCE(val)
	}
	if val, ok := data["39"]; ok {
		pid.TribalCitizenship = p.parseCWESlice(val)
	}

	return pid
}

// Helper functions for parsing individual data types
func (p *HL7Parser) reconstructEncodingCharacters(val interface{}) string {
	// EncodingCharacters is typically "^~\&"
	// The parsed value is complex due to component/subcomponent parsing
	// For now, return the standard value
	return "^~\\&"
}

func (p *HL7Parser) getString(val interface{}) string {
	if str, ok := val.(string); ok {
		return str
	}
	return ""
}

func (p *HL7Parser) getStringSlice(val interface{}) []string {
	switch v := val.(type) {
	case string:
		return []string{v}
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			result[i] = p.getString(item)
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseTime(val interface{}) time.Time {
	str := p.getString(val)
	if str == "" {
		return time.Time{}
	}

	// Try different HL7 timestamp formats
	formats := []string{
		"20060102150405-0700",
		"20060102150405",
		"20060102",
		time.RFC3339,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, str); err == nil {
			return t
		}
	}

	return time.Time{}
}

func (p *HL7Parser) parseHD(val interface{}) *HD {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	hd := &HD{}
	if len(components) > 0 {
		hd.NamespaceID = components[0]
	}
	if len(components) > 1 {
		hd.UniversalID = components[1]
	}
	if len(components) > 2 {
		hd.UniversalIDType = components[2]
	}

	return hd
}

func (p *HL7Parser) parseMSG(val interface{}) *MSG {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	msg := &MSG{}
	if len(components) > 0 {
		msg.MessageCode = components[0]
	}
	if len(components) > 1 {
		msg.TriggerEvent = components[1]
	}
	if len(components) > 2 {
		msg.MessageStructure = components[2]
	}

	return msg
}

func (p *HL7Parser) parsePT(val interface{}) *PT {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	pt := &PT{}
	if len(components) > 0 {
		pt.ProcessingID = components[0]
	}
	if len(components) > 1 {
		pt.ProcessingMode = components[1]
	}

	return pt
}

func (p *HL7Parser) parseVID(val interface{}) *VID {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	vid := &VID{}
	if len(components) > 0 {
		vid.VersionID = components[0]
	}
	if len(components) > 1 {
		vid.InternationalizationCode = p.parseCEFromComponents(components[1])
	}
	if len(components) > 2 {
		vid.InternationalVersionID = p.parseCEFromComponents(components[2])
	}

	return vid
}

func (p *HL7Parser) parseCE(val interface{}) *CE {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	ce := &CE{}
	if len(components) > 0 {
		ce.Identifier = components[0]
	}
	if len(components) > 1 {
		ce.Text = components[1]
	}
	if len(components) > 2 {
		ce.NameOfCodingSystem = components[2]
	}
	if len(components) > 3 {
		ce.AlternateIdentifier = components[3]
	}
	if len(components) > 4 {
		ce.AlternateText = components[4]
	}
	if len(components) > 5 {
		ce.NameOfAlternateCodingSystem = components[5]
	}

	return ce
}

func (p *HL7Parser) parseCEFromComponents(componentStr string) *CE {
	if componentStr == "" {
		return nil
	}
	subComponents := strings.Split(componentStr, p.subcomponentSeparator)
	ce := &CE{}
	if len(subComponents) > 0 {
		ce.Identifier = subComponents[0]
	}
	if len(subComponents) > 1 {
		ce.Text = subComponents[1]
	}
	if len(subComponents) > 2 {
		ce.NameOfCodingSystem = subComponents[2]
	}
	return ce
}

func (p *HL7Parser) getComponents(val interface{}) []string {
	str := p.getString(val)
	if str == "" {
		return nil
	}
	return strings.Split(str, p.componentSeparator)
}

// Placeholder implementations for other parsing functions
// These would need full implementation for a complete parser

func (p *HL7Parser) parseCX(val interface{}) *CX {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	cx := &CX{}
	if len(components) > 0 {
		cx.IDNumber = components[0]
	}
	if len(components) > 1 {
		cx.CheckDigit = components[1]
	}
	if len(components) > 2 {
		cx.CheckDigitScheme = components[2]
	}
	if len(components) > 3 {
		cx.AssigningAuthority = p.parseHD(components[3])
	}
	if len(components) > 4 {
		cx.IdentifierTypeCode = components[4]
	}
	if len(components) > 5 {
		cx.AssigningFacility = p.parseHD(components[5])
	}
	if len(components) > 6 {
		cx.EffectiveDate = p.parseTime(components[6])
	}
	if len(components) > 7 {
		cx.ExpirationDate = p.parseTime(components[7])
	}
	if len(components) > 8 {
		cx.AssigningJurisdiction = p.parseCWE(components[8])
	}
	if len(components) > 9 {
		cx.AssigningAgencyOrDepartment = p.parseCWE(components[9])
	}

	return cx
}
func (p *HL7Parser) parseCXList(val interface{}) []*CX {
	switch v := val.(type) {
	case string:
		if cx := p.parseCX(v); cx != nil {
			return []*CX{cx}
		}
	case []interface{}:
		result := make([]*CX, 0, len(v))
		for _, item := range v {
			if cx := p.parseCX(item); cx != nil {
				result = append(result, cx)
			}
		}
		return result
	}
	return nil
}
func (p *HL7Parser) parseXPNList(val interface{}) []*XPN {
	switch v := val.(type) {
	case string:
		if xpn := p.parseXPN(v); xpn != nil {
			return []*XPN{xpn}
		}
	case []interface{}:
		result := make([]*XPN, 0, len(v))
		for _, item := range v {
			if xpn := p.parseXPN(item); xpn != nil {
				result = append(result, xpn)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseXPN(val interface{}) *XPN {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	xpn := &XPN{}
	if len(components) > 0 {
		xpn.FamilyName = components[0]
	}
	if len(components) > 1 {
		xpn.GivenName = components[1]
	}
	if len(components) > 2 {
		xpn.SecondAndFurtherGivenNames = components[2]
	}
	if len(components) > 3 {
		xpn.Suffix = components[3]
	}
	if len(components) > 4 {
		xpn.Prefix = components[4]
	}
	if len(components) > 5 {
		xpn.Degree = components[5]
	}
	if len(components) > 6 {
		xpn.NameTypeCode = components[6]
	}
	if len(components) > 7 {
		xpn.NameRepresentationCode = components[7]
	}
	if len(components) > 8 {
		xpn.NameContext = p.parseCE(components[8])
	}
	if len(components) > 9 {
		xpn.NameValidityRange = components[9]
	}
	if len(components) > 10 {
		xpn.NameAssemblyOrder = components[10]
	}
	if len(components) > 11 {
		xpn.EffectiveDate = p.parseTime(components[11])
	}
	if len(components) > 12 {
		xpn.ExpirationDate = p.parseTime(components[12])
	}
	if len(components) > 13 {
		xpn.ProfessionalSuffix = components[13]
	}

	return xpn
}
func (p *HL7Parser) parseCESlice(val interface{}) []*CE {
	switch v := val.(type) {
	case string:
		if ce := p.parseCE(v); ce != nil {
			return []*CE{ce}
		}
	case []interface{}:
		result := make([]*CE, 0, len(v))
		for _, item := range v {
			if ce := p.parseCE(item); ce != nil {
				result = append(result, ce)
			}
		}
		return result
	}
	return nil
}
func (p *HL7Parser) parseXAD(val interface{}) *XAD {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	xad := &XAD{}
	if len(components) > 0 {
		xad.StreetAddress = components[0]
	}
	if len(components) > 1 {
		xad.OtherDesignation = components[1]
	}
	if len(components) > 2 {
		xad.City = components[2]
	}
	if len(components) > 3 {
		xad.StateOrProvince = components[3]
	}
	if len(components) > 4 {
		xad.ZipOrPostalCode = components[4]
	}
	if len(components) > 5 {
		xad.Country = components[5]
	}
	if len(components) > 6 {
		xad.AddressType = components[6]
	}
	if len(components) > 7 {
		xad.OtherGeographicDesignation = components[7]
	}
	if len(components) > 8 {
		xad.CountyParishCode = components[8]
	}
	if len(components) > 9 {
		xad.CensusTract = components[9]
	}
	if len(components) > 10 {
		xad.AddressRepresentationCode = components[10]
	}
	if len(components) > 11 {
		xad.AddressValidityRange = components[11]
	}
	if len(components) > 12 {
		xad.EffectiveDate = p.parseTime(components[12])
	}
	if len(components) > 13 {
		xad.ExpirationDate = p.parseTime(components[13])
	}

	return xad
}

func (p *HL7Parser) parseXADList(val interface{}) []*XAD {
	switch v := val.(type) {
	case string:
		if xad := p.parseXAD(v); xad != nil {
			return []*XAD{xad}
		}
	case []interface{}:
		result := make([]*XAD, 0, len(v))
		for _, item := range v {
			if xad := p.parseXAD(item); xad != nil {
				result = append(result, xad)
			}
		}
		return result
	}
	return nil
}
func (p *HL7Parser) parseXTN(val interface{}) *XTN {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	xtn := &XTN{}
	if len(components) > 0 {
		xtn.TelephoneNumber = components[0]
	}
	if len(components) > 1 {
		xtn.TelecommunicationUseCode = components[1]
	}
	if len(components) > 2 {
		xtn.TelecommunicationEquipmentType = components[2]
	}
	if len(components) > 3 {
		xtn.EmailAddress = components[3]
	}
	if len(components) > 4 {
		xtn.CountryCode = components[4]
	}
	if len(components) > 5 {
		xtn.AreaCityCode = components[5]
	}
	if len(components) > 6 {
		xtn.LocalNumber = components[6]
	}
	if len(components) > 7 {
		xtn.Extension = components[7]
	}
	if len(components) > 8 {
		xtn.AnyText = components[8]
	}
	if len(components) > 9 {
		xtn.ExtensionPrefix = components[9]
	}
	if len(components) > 10 {
		xtn.SpeedDialCode = components[10]
	}
	if len(components) > 11 {
		xtn.UnformattedTelephoneNumber = components[11]
	}

	return xtn
}

func (p *HL7Parser) parseXTNList(val interface{}) []*XTN {
	switch v := val.(type) {
	case string:
		if xtn := p.parseXTN(v); xtn != nil {
			return []*XTN{xtn}
		}
	case []interface{}:
		result := make([]*XTN, 0, len(v))
		for _, item := range v {
			if xtn := p.parseXTN(item); xtn != nil {
				result = append(result, xtn)
			}
		}
		return result
	}
	return nil
}
func (p *HL7Parser) parseDLN(val interface{}) *DLN {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	dln := &DLN{}
	if len(components) > 0 {
		dln.LicenseNumber = components[0]
	}
	if len(components) > 1 {
		dln.IssuingState = components[1]
	}
	if len(components) > 2 {
		dln.ExpirationDate = components[2]
	}

	return dln
}
func (p *HL7Parser) parseCWE(val interface{}) *CWE {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	cwe := &CWE{}
	if len(components) > 0 {
		cwe.Identifier = components[0]
	}
	if len(components) > 1 {
		cwe.Text = components[1]
	}
	if len(components) > 2 {
		cwe.NameOfCodingSystem = components[2]
	}
	if len(components) > 3 {
		cwe.AlternateIdentifier = components[3]
	}
	if len(components) > 4 {
		cwe.AlternateText = components[4]
	}
	if len(components) > 5 {
		cwe.NameOfAlternateCodingSystem = components[5]
	}
	if len(components) > 6 {
		cwe.CodingSystemVersionID = components[6]
	}
	if len(components) > 7 {
		cwe.AlternateCodingSystemVersionID = components[7]
	}
	if len(components) > 8 {
		cwe.OriginalText = components[8]
	}

	return cwe
}

func (p *HL7Parser) parseCWESlice(val interface{}) []*CWE {
	switch v := val.(type) {
	case string:
		if cwe := p.parseCWE(v); cwe != nil {
			return []*CWE{cwe}
		}
	case []interface{}:
		result := make([]*CWE, 0, len(v))
		for _, item := range v {
			if cwe := p.parseCWE(item); cwe != nil {
				result = append(result, cwe)
			}
		}
		return result
	}
	return nil
}
func (p *HL7Parser) parseEI(val interface{}) *EI {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	ei := &EI{}
	if len(components) > 0 {
		ei.EntityIdentifier = components[0]
	}
	if len(components) > 1 {
		ei.NamespaceID = components[1]
	}
	if len(components) > 2 {
		ei.UniversalID = components[2]
	}
	if len(components) > 3 {
		ei.UniversalIDType = components[3]
	}

	return ei
}

func (p *HL7Parser) parseEISlice(val interface{}) []*EI {
	switch v := val.(type) {
	case string:
		if ei := p.parseEI(v); ei != nil {
			return []*EI{ei}
		}
	case []interface{}:
		result := make([]*EI, 0, len(v))
		for _, item := range v {
			if ei := p.parseEI(item); ei != nil {
				result = append(result, ei)
			}
		}
		return result
	}
	return nil
}
func (p *HL7Parser) parseXON(val interface{}) *XON {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	xon := &XON{}
	if len(components) > 0 {
		xon.OrganizationName = components[0]
	}
	if len(components) > 1 {
		xon.OrganizationNameTypeCode = components[1]
	}
	if len(components) > 2 {
		xon.IDNumber = components[2]
	}
	if len(components) > 3 {
		xon.CheckDigit = components[3]
	}
	if len(components) > 4 {
		xon.CheckDigitScheme = components[4]
	}
	if len(components) > 5 {
		xon.AssigningAuthority = p.parseHD(components[5])
	}
	if len(components) > 6 {
		xon.IdentifierTypeCode = components[6]
	}
	if len(components) > 7 {
		xon.AssigningFacility = p.parseHD(components[7])
	}
	if len(components) > 8 {
		xon.NameRepresentationCode = components[8]
	}
	if len(components) > 9 {
		xon.OrganizationIdentifier = components[9]
	}

	return xon
}

func (p *HL7Parser) parseXONList(val interface{}) []*XON {
	switch v := val.(type) {
	case string:
		if xon := p.parseXON(v); xon != nil {
			return []*XON{xon}
		}
	case []interface{}:
		result := make([]*XON, 0, len(v))
		for _, item := range v {
			if xon := p.parseXON(item); xon != nil {
				result = append(result, xon)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseFC(val interface{}) *FC {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	fc := &FC{}
	if len(components) > 0 {
		fc.FinancialClassCode = components[0]
	}
	if len(components) > 1 {
		fc.EffectiveDate = p.parseTime(components[1])
	}

	return fc
}

func (p *HL7Parser) parseDLD(val interface{}) *DLD {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	dld := &DLD{}
	if len(components) > 0 {
		dld.DischargeLocation = components[0]
	}
	if len(components) > 1 {
		dld.EffectiveDate = p.parseTime(components[1])
	}

	return dld
}

func (p *HL7Parser) parseEIP(val interface{}) *EIP {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	eip := &EIP{}
	if len(components) > 0 {
		eip.PlacerAssignedIdentifier = p.parseEI(components[0])
	}
	if len(components) > 1 {
		eip.FillerAssignedIdentifier = p.parseEI(components[1])
	}

	return eip
}

func (p *HL7Parser) parseCQ(val interface{}) *CQ {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	cq := &CQ{}
	if len(components) > 0 {
		cq.Quantity = components[0]
	}
	if len(components) > 1 {
		cq.Units = p.parseCE(components[1])
	}

	return cq
}

func (p *HL7Parser) parseSPS(val interface{}) *SPS {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	sps := &SPS{}
	if len(components) > 0 {
		sps.SpecimenSourceNameOrCode = p.parseCE(components[0])
	}
	if len(components) > 1 {
		sps.Additives = components[1]
	}
	if len(components) > 2 {
		sps.SpecimenCollectionMethod = components[2]
	}
	if len(components) > 3 {
		sps.BodySite = p.parseCE(components[3])
	}
	if len(components) > 4 {
		sps.SiteModifier = p.parseCE(components[4])
	}
	if len(components) > 5 {
		sps.CollectionMethodModifierCode = p.parseCE(components[5])
	}
	if len(components) > 6 {
		sps.SpecimenRole = p.parseCWE(components[6])
	}

	return sps
}

func (p *HL7Parser) parseMOC(val interface{}) *MOC {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	moc := &MOC{}
	if len(components) > 0 {
		moc.MonetaryAmount = components[0]
	}
	if len(components) > 1 {
		moc.ChargeCode = p.parseCE(components[1])
	}

	return moc
}

func (p *HL7Parser) parsePRL(val interface{}) *PRL {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	prl := &PRL{}
	if len(components) > 0 {
		prl.ParentObservationIdentifier = p.parseCE(components[0])
	}
	if len(components) > 1 {
		prl.ParentObservationSubIdentifier = components[1]
	}
	if len(components) > 2 {
		prl.ParentObservationValueDescriptor = components[2]
	}

	return prl
}

func (p *HL7Parser) parseNDL(val interface{}) *NDL {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	ndl := &NDL{}
	if len(components) > 0 {
		ndl.Name = p.parseXCN(components[0])
	}
	if len(components) > 1 {
		ndl.StartDateTime = p.parseTime(components[1])
	}
	if len(components) > 2 {
		ndl.EndDateTime = p.parseTime(components[2])
	}
	if len(components) > 3 {
		ndl.PointOfCare = components[3]
	}
	if len(components) > 4 {
		ndl.Room = components[4]
	}
	if len(components) > 5 {
		ndl.Bed = components[5]
	}
	if len(components) > 6 {
		ndl.Facility = p.parseHD(components[6])
	}
	if len(components) > 7 {
		ndl.LocationStatus = components[7]
	}
	if len(components) > 8 {
		ndl.PatientLocationType = components[8]
	}
	if len(components) > 9 {
		ndl.Building = components[9]
	}
	if len(components) > 10 {
		ndl.Floor = components[10]
	}

	return ndl
}

func (p *HL7Parser) parseNDLSice(val interface{}) []*NDL {
	switch v := val.(type) {
	case string:
		if ndl := p.parseNDL(v); ndl != nil {
			return []*NDL{ndl}
		}
	case []interface{}:
		result := make([]*NDL, 0, len(v))
		for _, item := range v {
			if ndl := p.parseNDL(item); ndl != nil {
				result = append(result, ndl)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseELD(val interface{}) *ELD {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	eld := &ELD{}
	if len(components) > 0 {
		eld.SegmentID = components[0]
	}
	if len(components) > 1 {
		eld.SegmentSequence = components[1]
	}
	if len(components) > 2 {
		eld.FieldPosition = components[2]
	}
	if len(components) > 3 {
		eld.CodeIdentifyingError = p.parseCE(components[3])
	}

	return eld
}

func (p *HL7Parser) parseERL(val interface{}) *ERL {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	erl := &ERL{}
	if len(components) > 0 {
		erl.SegmentID = components[0]
	}
	if len(components) > 1 {
		erl.SegmentSequence = components[1]
	}
	if len(components) > 2 {
		erl.FieldPosition = components[2]
	}
	if len(components) > 3 {
		erl.FieldRepetition = components[3]
	}
	if len(components) > 4 {
		erl.ComponentNumber = components[4]
	}
	if len(components) > 5 {
		erl.SubComponentNumber = components[5]
	}

	return erl
}

func (p *HL7Parser) parseELDSlice(val interface{}) []*ELD {
	switch v := val.(type) {
	case string:
		if eld := p.parseELD(v); eld != nil {
			return []*ELD{eld}
		}
	case []interface{}:
		result := make([]*ELD, 0, len(v))
		for _, item := range v {
			if eld := p.parseELD(item); eld != nil {
				result = append(result, eld)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseERLSlice(val interface{}) []*ERL {
	switch v := val.(type) {
	case string:
		if erl := p.parseERL(v); erl != nil {
			return []*ERL{erl}
		}
	case []interface{}:
		result := make([]*ERL, 0, len(v))
		for _, item := range v {
			if erl := p.parseERL(item); erl != nil {
				result = append(result, erl)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseXADSlice(val interface{}) []*XAD {
	switch v := val.(type) {
	case string:
		if xad := p.parseXAD(v); xad != nil {
			return []*XAD{xad}
		}
	case []interface{}:
		result := make([]*XAD, 0, len(v))
		for _, item := range v {
			if xad := p.parseXAD(item); xad != nil {
				result = append(result, xad)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parsePLSlice(val interface{}) []*PL {
	switch v := val.(type) {
	case string:
		if pl := p.parsePL(v); pl != nil {
			return []*PL{pl}
		}
	case []interface{}:
		result := make([]*PL, 0, len(v))
		for _, item := range v {
			if pl := p.parsePL(item); pl != nil {
				result = append(result, pl)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseFCSlice(val interface{}) []*FC {
	switch v := val.(type) {
	case string:
		if fc := p.parseFC(v); fc != nil {
			return []*FC{fc}
		}
	case []interface{}:
		result := make([]*FC, 0, len(v))
		for _, item := range v {
			if fc := p.parseFC(item); fc != nil {
				result = append(result, fc)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseTimeSlice(val interface{}) []time.Time {
	switch v := val.(type) {
	case string:
		if t := p.parseTime(v); !t.IsZero() {
			return []time.Time{t}
		}
	case []interface{}:
		result := make([]time.Time, 0, len(v))
		for _, item := range v {
			if t := p.parseTime(item); !t.IsZero() {
				result = append(result, t)
			}
		}
		return result
	}
	return nil
}

func (p *HL7Parser) parseNK1(data map[string]interface{}) *NK1 {
	nk1 := &NK1{}

	if val, ok := data["1"]; ok {
		nk1.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		nk1.Name = p.parseXPNList(val)
	}
	if val, ok := data["3"]; ok {
		nk1.Relationship = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		nk1.Address = p.parseXADList(val)
	}
	if val, ok := data["5"]; ok {
		nk1.PhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["6"]; ok {
		nk1.BusinessPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["7"]; ok {
		nk1.ContactRole = p.parseCE(val)
	}
	if val, ok := data["8"]; ok {
		nk1.StartDate = p.parseTime(val)
	}
	if val, ok := data["9"]; ok {
		nk1.EndDate = p.parseTime(val)
	}
	if val, ok := data["10"]; ok {
		nk1.NextOfKinAssociatedPartiesJobTitle = p.getString(val)
	}
	// Add more fields as needed, but for brevity, implement key ones

	return nk1
}
func (p *HL7Parser) parsePL(val interface{}) *PL {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	pl := &PL{}
	if len(components) > 0 {
		pl.PointOfCare = components[0]
	}
	if len(components) > 1 {
		pl.Room = components[1]
	}
	if len(components) > 2 {
		pl.Bed = components[2]
	}
	if len(components) > 3 {
		pl.Facility = p.parseHD(components[3])
	}
	if len(components) > 4 {
		pl.LocationStatus = components[4]
	}
	if len(components) > 5 {
		pl.PersonLocationType = components[5]
	}
	if len(components) > 6 {
		pl.Building = components[6]
	}
	if len(components) > 7 {
		pl.Floor = components[7]
	}
	if len(components) > 8 {
		pl.LocationDescription = components[8]
	}
	if len(components) > 9 {
		pl.ComprehensiveLocationIdentifier = p.parseEI(components[9])
	}
	if len(components) > 10 {
		pl.AssigningAuthorityForLocation = p.parseHD(components[10])
	}

	return pl
}
func (p *HL7Parser) parsePV1(data map[string]interface{}) *PV1 {
	pv1 := &PV1{}

	if val, ok := data["1"]; ok {
		pv1.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		pv1.PatientClass = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		pv1.AssignedPatientLocation = p.parsePL(val)
	}
	if val, ok := data["4"]; ok {
		pv1.AdmissionType = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		pv1.PreadmitNumber = p.parseCX(val)
	}
	if val, ok := data["6"]; ok {
		pv1.PriorPatientLocation = p.parsePL(val)
	}
	if val, ok := data["7"]; ok {
		pv1.AttendingDoctor = p.parseXCNList(val)
	}
	if val, ok := data["8"]; ok {
		pv1.ReferringDoctor = p.parseXCNList(val)
	}
	if val, ok := data["9"]; ok {
		pv1.ConsultingDoctor = p.parseXCNList(val)
	}
	if val, ok := data["10"]; ok {
		pv1.HospitalService = p.getString(val)
	}
	if val, ok := data["11"]; ok {
		pv1.TemporaryLocation = p.parsePL(val)
	}
	if val, ok := data["12"]; ok {
		pv1.PreadmitTestIndicator = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		pv1.ReAdmissionIndicator = p.getString(val)
	}
	if val, ok := data["14"]; ok {
		pv1.AdmitSource = p.getString(val)
	}
	if val, ok := data["15"]; ok {
		pv1.AmbulatoryStatus = p.getStringSlice(val)
	}
	if val, ok := data["16"]; ok {
		pv1.VIPIndicator = p.getString(val)
	}
	if val, ok := data["17"]; ok {
		pv1.AdmittingDoctor = p.parseXCNList(val)
	}
	if val, ok := data["18"]; ok {
		pv1.PatientType = p.getString(val)
	}
	if val, ok := data["19"]; ok {
		pv1.VisitNumber = p.parseCX(val)
	}
	// Add more fields as needed

	return pv1
}
func (p *HL7Parser) parsePV2(data map[string]interface{}) *PV2 {
	pv2 := &PV2{}

	if val, ok := data["1"]; ok {
		pv2.PriorPendingLocation = p.parsePL(val)
	}
	if val, ok := data["2"]; ok {
		pv2.AccommodationCode = p.parseCE(val)
	}
	if val, ok := data["3"]; ok {
		pv2.AdmitReason = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		pv2.TransferReason = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		pv2.PatientValuables = p.getStringSlice(val)
	}
	if val, ok := data["6"]; ok {
		pv2.PatientValuablesLocation = p.getString(val)
	}
	if val, ok := data["7"]; ok {
		pv2.VisitUserCode = p.getStringSlice(val)
	}
	if val, ok := data["8"]; ok {
		pv2.ExpectedAdmitDateTime = p.parseTime(val)
	}
	if val, ok := data["9"]; ok {
		pv2.ExpectedDischargeDateTime = p.parseTime(val)
	}
	if val, ok := data["10"]; ok {
		pv2.EstimatedLengthOfInpatientStay = p.getString(val)
	}
	if val, ok := data["11"]; ok {
		pv2.ActualLengthOfInpatientStay = p.getString(val)
	}
	if val, ok := data["12"]; ok {
		pv2.VisitDescription = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		pv2.ReferralSourceCode = p.parseXCNList(val)
	}
	if val, ok := data["14"]; ok {
		pv2.PreviousServiceDate = p.parseTime(val)
	}
	if val, ok := data["15"]; ok {
		pv2.EmploymentIllnessRelatedIndicator = p.getString(val)
	}
	if val, ok := data["16"]; ok {
		pv2.PurgeStatusCode = p.getString(val)
	}
	if val, ok := data["17"]; ok {
		pv2.PurgeStatusDate = p.parseTime(val)
	}
	if val, ok := data["18"]; ok {
		pv2.SpecialProgramCode = p.parseCE(val)
	}
	if val, ok := data["19"]; ok {
		pv2.RetentionIndicator = p.getString(val)
	}
	if val, ok := data["20"]; ok {
		pv2.ExpectedNumberOfInsurancePlans = p.getString(val)
	}
	if val, ok := data["21"]; ok {
		pv2.VisitPublicityCode = p.getString(val)
	}
	if val, ok := data["22"]; ok {
		pv2.VisitProtectionIndicator = p.getString(val)
	}
	if val, ok := data["23"]; ok {
		pv2.ClinicOrganizationName = p.parseXONList(val)
	}
	if val, ok := data["24"]; ok {
		pv2.PatientStatusCode = p.getString(val)
	}
	if val, ok := data["25"]; ok {
		pv2.VisitReasonCode = p.parseCE(val)
	}
	if val, ok := data["26"]; ok {
		pv2.PreadmitTestIndicator = p.parseCESlice(val)
	}
	if val, ok := data["27"]; ok {
		pv2.ReAdmissionIndicator = p.getString(val)
	}
	if val, ok := data["28"]; ok {
		pv2.AdmitSource = p.parseCE(val)
	}
	if val, ok := data["29"]; ok {
		pv2.AmbulatoryStatus = p.parseCESlice(val)
	}
	if val, ok := data["30"]; ok {
		pv2.VIPIndicator = p.parseCE(val)
	}
	if val, ok := data["31"]; ok {
		pv2.AdmittingDoctor = p.parseXCNList(val)
	}
	if val, ok := data["32"]; ok {
		pv2.PatientType = p.parseCE(val)
	}
	if val, ok := data["33"]; ok {
		pv2.VisitNumber = p.parseCX(val)
	}
	if val, ok := data["34"]; ok {
		pv2.FinancialClass = p.parseFCSlice(val)
	}
	if val, ok := data["35"]; ok {
		pv2.ChargePriceIndicator = p.parseCE(val)
	}
	if val, ok := data["36"]; ok {
		pv2.CourtesyCode = p.parseCE(val)
	}
	if val, ok := data["37"]; ok {
		pv2.CreditRating = p.parseCE(val)
	}
	if val, ok := data["38"]; ok {
		pv2.ContractCode = p.parseCESlice(val)
	}
	if val, ok := data["39"]; ok {
		pv2.ContractEffectiveDate = p.parseTimeSlice(val)
	}
	if val, ok := data["40"]; ok {
		pv2.ContractAmount = p.getStringSlice(val)
	}
	if val, ok := data["41"]; ok {
		pv2.ContractPeriod = p.getStringSlice(val)
	}
	if val, ok := data["42"]; ok {
		pv2.InterestCode = p.parseCE(val)
	}
	if val, ok := data["43"]; ok {
		pv2.TransferToBadDebtCode = p.parseCE(val)
	}
	if val, ok := data["44"]; ok {
		pv2.TransferToBadDebtDate = p.parseTime(val)
	}
	if val, ok := data["45"]; ok {
		pv2.BadDebtAgencyCode = p.parseCE(val)
	}
	if val, ok := data["46"]; ok {
		pv2.BadDebtTransferAmount = p.getString(val)
	}
	if val, ok := data["47"]; ok {
		pv2.BadDebtRecoveryAmount = p.getString(val)
	}
	if val, ok := data["48"]; ok {
		pv2.DeleteAccountIndicator = p.parseCE(val)
	}
	if val, ok := data["49"]; ok {
		pv2.DeleteAccountDate = p.parseTime(val)
	}
	if val, ok := data["50"]; ok {
		pv2.DischargeDisposition = p.parseCE(val)
	}
	if val, ok := data["51"]; ok {
		pv2.DischargedToLocation = p.parseDLD(val)
	}
	if val, ok := data["52"]; ok {
		pv2.DietType = p.parseCE(val)
	}
	if val, ok := data["53"]; ok {
		pv2.ServicingFacility = p.parseHD(val)
	}
	if val, ok := data["54"]; ok {
		pv2.BedStatus = p.getString(val)
	}
	if val, ok := data["55"]; ok {
		pv2.AccountStatus = p.parseCE(val)
	}
	if val, ok := data["56"]; ok {
		pv2.PendingLocation = p.parsePL(val)
	}
	if val, ok := data["57"]; ok {
		pv2.PriorTemporaryLocation = p.parsePL(val)
	}
	if val, ok := data["58"]; ok {
		pv2.AdmitDateTime = p.parseTime(val)
	}
	if val, ok := data["59"]; ok {
		pv2.DischargeDateTime = p.parseTime(val)
	}
	if val, ok := data["60"]; ok {
		pv2.CurrentPatientBalance = p.getString(val)
	}
	if val, ok := data["61"]; ok {
		pv2.TotalCharges = p.getString(val)
	}
	if val, ok := data["62"]; ok {
		pv2.TotalAdjustments = p.getString(val)
	}
	if val, ok := data["63"]; ok {
		pv2.TotalPayments = p.getString(val)
	}
	if val, ok := data["64"]; ok {
		pv2.AlternateVisitID = p.parseCX(val)
	}
	if val, ok := data["65"]; ok {
		pv2.VisitIndicator = p.getString(val)
	}
	if val, ok := data["66"]; ok {
		pv2.OtherHealthcareProvider = p.parseXCNList(val)
	}
	if val, ok := data["67"]; ok {
		pv2.ServiceEpisodeDescription = p.getString(val)
	}
	if val, ok := data["68"]; ok {
		pv2.ServiceEpisodeIdentifier = p.parseCX(val)
	}

	return pv2
}
func (p *HL7Parser) parseAL1(data map[string]interface{}) *AL1 {
	al1 := &AL1{}

	if val, ok := data["1"]; ok {
		al1.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		al1.AllergenTypeCode = p.parseCE(val)
	}
	if val, ok := data["3"]; ok {
		al1.AllergenCodeMnemonicDescription = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		al1.AllergySeverityCode = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		al1.AllergyReactionCode = p.getStringSlice(val)
	}
	if val, ok := data["6"]; ok {
		al1.IdentificationDate = p.parseTime(val)
	}

	return al1
}
func (p *HL7Parser) parseDG1(data map[string]interface{}) *DG1 {
	dg1 := &DG1{}

	if val, ok := data["1"]; ok {
		dg1.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		dg1.DiagnosisCodingMethod = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		dg1.DiagnosisCodeDG1 = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		dg1.DiagnosisDescription = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		dg1.DiagnosisDateTime = p.parseTime(val)
	}

	return dg1
}
func (p *HL7Parser) parsePR1(data map[string]interface{}) *PR1 {
	pr1 := &PR1{}

	if val, ok := data["1"]; ok {
		pr1.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		pr1.ProcedureCodingMethod = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		pr1.ProcedureCode = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		pr1.ProcedureDescription = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		pr1.ProcedureDateTime = p.parseTime(val)
	}
	if val, ok := data["6"]; ok {
		pr1.ProcedureFunctionalType = p.getString(val)
	}
	if val, ok := data["7"]; ok {
		pr1.ProcedureMinutes = p.getString(val)
	}
	if val, ok := data["8"]; ok {
		pr1.Anesthesiologist = p.parseXCNList(val)
	}
	if val, ok := data["9"]; ok {
		pr1.AnesthesiaCode = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		pr1.AnesthesiaMinutes = p.getString(val)
	}
	if val, ok := data["11"]; ok {
		pr1.Surgeon = p.parseXCNList(val)
	}
	if val, ok := data["12"]; ok {
		pr1.ProcedurePractitioner = p.parseXCNList(val)
	}
	if val, ok := data["13"]; ok {
		pr1.ConsentCode = p.parseCE(val)
	}
	if val, ok := data["14"]; ok {
		pr1.ProcedurePriority = p.getString(val)
	}
	if val, ok := data["15"]; ok {
		pr1.AssociatedDiagnosisCode = p.parseCE(val)
	}
	if val, ok := data["16"]; ok {
		pr1.ProcedureCodeModifier = p.parseCESlice(val)
	}
	if val, ok := data["17"]; ok {
		pr1.ProcedureDRGType = p.getString(val)
	}
	if val, ok := data["18"]; ok {
		pr1.TissueTypeCode = p.parseCESlice(val)
	}

	return pr1
}
func (p *HL7Parser) parseROL(data map[string]interface{}) *ROL {
	rol := &ROL{}

	if val, ok := data["1"]; ok {
		rol.RoleInstanceID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		rol.ActionCode = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		rol.RoleROL = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		rol.RolePerson = p.parseXCNList(val)
	}
	if val, ok := data["5"]; ok {
		rol.RoleBeginDateTime = p.parseTime(val)
	}
	if val, ok := data["6"]; ok {
		rol.RoleEndDateTime = p.parseTime(val)
	}
	if val, ok := data["7"]; ok {
		rol.RoleDuration = p.parseCE(val)
	}
	if val, ok := data["8"]; ok {
		rol.RoleActionReason = p.parseCE(val)
	}
	if val, ok := data["9"]; ok {
		rol.ProviderType = p.parseCESlice(val)
	}
	if val, ok := data["10"]; ok {
		rol.OrganizationUnitType = p.parseCE(val)
	}
	if val, ok := data["11"]; ok {
		rol.OfficeHomeAddressBirthplace = p.parseXAD(val)
	}
	if val, ok := data["12"]; ok {
		rol.Phone = p.parseXTNList(val)
	}

	return rol
}
func (p *HL7Parser) parseORC(data map[string]interface{}) *ORC {
	orc := &ORC{}

	if val, ok := data["1"]; ok {
		orc.OrderControl = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		orc.PlacerOrderNumber = p.parseEI(val)
	}
	if val, ok := data["3"]; ok {
		orc.FillerOrderNumber = p.parseEI(val)
	}
	if val, ok := data["4"]; ok {
		orc.PlacerGroupNumber = p.parseEI(val)
	}
	if val, ok := data["5"]; ok {
		orc.OrderStatus = p.getString(val)
	}
	if val, ok := data["6"]; ok {
		orc.ResponseFlag = p.getString(val)
	}
	if val, ok := data["7"]; ok {
		orc.QuantityTiming = p.getStringSlice(val)
	}
	if val, ok := data["8"]; ok {
		orc.ParentOrder = p.parseEIP(val)
	}
	if val, ok := data["9"]; ok {
		orc.DateTimeOfTransaction = p.parseTime(val)
	}
	if val, ok := data["10"]; ok {
		orc.EnteredBy = p.parseXCNList(val)
	}
	if val, ok := data["11"]; ok {
		orc.VerifiedBy = p.parseXCNList(val)
	}
	if val, ok := data["12"]; ok {
		orc.OrderingProvider = p.parseXCNList(val)
	}
	if val, ok := data["13"]; ok {
		orc.EnterersLocation = p.parsePL(val)
	}
	if val, ok := data["14"]; ok {
		orc.CallBackPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["15"]; ok {
		orc.OrderEffectiveDateTime = p.parseTime(val)
	}
	if val, ok := data["16"]; ok {
		orc.OrderControlCodeReason = p.parseCE(val)
	}
	if val, ok := data["17"]; ok {
		orc.EnteringOrganization = p.parseCE(val)
	}
	if val, ok := data["18"]; ok {
		orc.EnteringDevice = p.parseCE(val)
	}
	if val, ok := data["19"]; ok {
		orc.ActionBy = p.parseXCNList(val)
	}
	if val, ok := data["20"]; ok {
		orc.AdvancedBeneficiaryNoticeCode = p.parseCE(val)
	}
	if val, ok := data["21"]; ok {
		orc.OrderingFacilityName = p.parseXONList(val)
	}
	if val, ok := data["22"]; ok {
		orc.OrderingFacilityAddress = p.parseXADList(val)
	}
	if val, ok := data["23"]; ok {
		orc.OrderingFacilityPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["24"]; ok {
		orc.OrderingProviderAddress = p.parseXADList(val)
	}
	if val, ok := data["25"]; ok {
		orc.OrderStatusModifier = p.parseCWE(val)
	}
	if val, ok := data["26"]; ok {
		orc.AdvancedBeneficiaryNoticeOverrideReason = p.parseCWE(val)
	}
	if val, ok := data["27"]; ok {
		orc.FillersExpectedAvailabilityDateTime = p.parseTime(val)
	}
	if val, ok := data["28"]; ok {
		orc.ConfidentialityCode = p.parseCWE(val)
	}
	if val, ok := data["29"]; ok {
		orc.OrderType = p.parseCWE(val)
	}
	if val, ok := data["30"]; ok {
		orc.EntererAuthorizationCode = p.getString(val)
	}

	return orc
}
func (p *HL7Parser) parseOBR(data map[string]interface{}) *OBR {
	obr := &OBR{}

	if val, ok := data["1"]; ok {
		obr.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		obr.PlacerOrderNumber = p.parseEI(val)
	}
	if val, ok := data["3"]; ok {
		obr.FillerOrderNumber = p.parseEI(val)
	}
	if val, ok := data["4"]; ok {
		obr.UniversalServiceIdentifier = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		obr.PriorityOBR = p.getString(val)
	}
	if val, ok := data["6"]; ok {
		obr.RequestedDateTime = p.parseTime(val)
	}
	if val, ok := data["7"]; ok {
		obr.ObservationDateTime = p.parseTime(val)
	}
	if val, ok := data["8"]; ok {
		obr.ObservationEndDateTime = p.parseTime(val)
	}
	if val, ok := data["9"]; ok {
		obr.CollectionVolume = p.parseCQ(val)
	}
	if val, ok := data["10"]; ok {
		obr.CollectorIdentifier = p.parseXCNList(val)
	}
	if val, ok := data["11"]; ok {
		obr.SpecimenActionCode = p.getString(val)
	}
	if val, ok := data["12"]; ok {
		obr.DangerCode = p.parseCE(val)
	}
	if val, ok := data["13"]; ok {
		obr.RelevantClinicalInfo = p.getString(val)
	}
	if val, ok := data["14"]; ok {
		obr.SpecimenReceivedDateTime = p.parseTime(val)
	}
	if val, ok := data["15"]; ok {
		obr.SpecimenSource = p.parseSPS(val)
	}
	if val, ok := data["16"]; ok {
		obr.OrderingProvider = p.parseXCNList(val)
	}
	if val, ok := data["17"]; ok {
		obr.OrderCallbackPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["18"]; ok {
		obr.PlacerField1 = p.getString(val)
	}
	if val, ok := data["19"]; ok {
		obr.PlacerField2 = p.getString(val)
	}
	if val, ok := data["20"]; ok {
		obr.FillerField1 = p.getString(val)
	}
	if val, ok := data["21"]; ok {
		obr.FillerField2 = p.getString(val)
	}
	if val, ok := data["22"]; ok {
		obr.ResultsRptStatusChngDateTime = p.parseTime(val)
	}
	if val, ok := data["23"]; ok {
		obr.ChargeToPractice = p.parseMOC(val)
	}
	if val, ok := data["24"]; ok {
		obr.DiagnosticServSectID = p.getString(val)
	}
	if val, ok := data["25"]; ok {
		obr.ResultStatus = p.getString(val)
	}
	if val, ok := data["26"]; ok {
		obr.ParentResult = p.parsePRL(val)
	}
	if val, ok := data["27"]; ok {
		obr.QuantityTiming = p.getStringSlice(val)
	}
	if val, ok := data["28"]; ok {
		obr.ResultCopiesTo = p.parseXCNList(val)
	}
	if val, ok := data["29"]; ok {
		obr.ParentNumber = p.parseEIP(val)
	}
	if val, ok := data["30"]; ok {
		obr.TransportationMode = p.getString(val)
	}
	if val, ok := data["31"]; ok {
		obr.ReasonForStudy = p.parseCESlice(val)
	}
	if val, ok := data["32"]; ok {
		obr.PrincipalResultInterpreter = p.parseNDL(val)
	}
	if val, ok := data["33"]; ok {
		obr.AssistantResultInterpreter = p.parseNDLSice(val)
	}
	if val, ok := data["34"]; ok {
		obr.Technician = p.parseNDLSice(val)
	}
	if val, ok := data["35"]; ok {
		obr.Transcriptionist = p.parseNDLSice(val)
	}
	if val, ok := data["36"]; ok {
		obr.ScheduledDateTime = p.parseTime(val)
	}
	if val, ok := data["37"]; ok {
		obr.NumberOfSampleContainers = p.getString(val)
	}
	if val, ok := data["38"]; ok {
		obr.TransportLogisticsOfCollectedSample = p.getStringSlice(val)
	}
	if val, ok := data["39"]; ok {
		obr.CollectorsComment = p.parseCESlice(val)
	}
	if val, ok := data["40"]; ok {
		obr.TransportArrangementResponsibility = p.getString(val)
	}
	if val, ok := data["41"]; ok {
		obr.TransportArranged = p.getString(val)
	}
	if val, ok := data["42"]; ok {
		obr.EscortRequired = p.getString(val)
	}
	if val, ok := data["43"]; ok {
		obr.PlannedPatientTransportComment = p.parseCESlice(val)
	}
	if val, ok := data["44"]; ok {
		obr.ProcedureCode = p.parseCE(val)
	}
	if val, ok := data["45"]; ok {
		obr.ProcedureCodeModifier = p.parseCESlice(val)
	}
	if val, ok := data["46"]; ok {
		obr.PlacerSupplementalServiceInformation = p.parseCESlice(val)
	}
	if val, ok := data["47"]; ok {
		obr.FillerSupplementalServiceInformation = p.parseCESlice(val)
	}
	if val, ok := data["48"]; ok {
		obr.MedicallyNecessaryDuplicateProcedureReason = p.parseCWE(val)
	}
	if val, ok := data["49"]; ok {
		obr.ResultHandling = p.getString(val)
	}
	if val, ok := data["50"]; ok {
		obr.ParentUniversalServiceIdentifier = p.parseCWE(val)
	}

	return obr
}
func (p *HL7Parser) parseOBX(data map[string]interface{}) *OBX {
	obx := &OBX{}

	if val, ok := data["1"]; ok {
		obx.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		obx.ValueType = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		obx.ObservationIdentifier = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		obx.ObservationSubID = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		obx.ObservationValue = val
	}
	if val, ok := data["6"]; ok {
		obx.Units = p.parseCE(val)
	}
	if val, ok := data["7"]; ok {
		obx.ReferencesRange = p.getString(val)
	}
	if val, ok := data["8"]; ok {
		obx.AbnormalFlags = p.getStringSlice(val)
	}
	if val, ok := data["9"]; ok {
		obx.Probability = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		obx.NatureOfAbnormalTest = p.getStringSlice(val)
	}
	if val, ok := data["11"]; ok {
		obx.ObservationResultStatus = p.getString(val)
	}
	if val, ok := data["12"]; ok {
		obx.EffectiveDateOfReferenceRange = p.parseTime(val)
	}
	if val, ok := data["13"]; ok {
		obx.UserDefinedAccessChecks = p.getString(val)
	}
	if val, ok := data["14"]; ok {
		obx.DateTimeOfTheObservation = p.parseTime(val)
	}
	if val, ok := data["15"]; ok {
		obx.ProducersID = p.parseCE(val)
	}
	if val, ok := data["16"]; ok {
		obx.ResponsibleObserver = p.parseXCNList(val)
	}
	if val, ok := data["17"]; ok {
		obx.ObservationMethod = p.parseCESlice(val)
	}
	if val, ok := data["18"]; ok {
		obx.EquipmentInstanceIdentifier = p.parseEI(val)
	}
	if val, ok := data["19"]; ok {
		obx.DateTimeOfTheAnalysis = p.parseTime(val)
	}
	if val, ok := data["20"]; ok {
		obx.ObservationSite = p.parseCESlice(val)
	}
	if val, ok := data["21"]; ok {
		obx.ObservationInstanceIdentifier = p.parseEI(val)
	}
	if val, ok := data["22"]; ok {
		obx.MoodCode = p.getString(val)
	}
	if val, ok := data["23"]; ok {
		obx.PerformingOrganizationName = p.parseXON(val)
	}
	if val, ok := data["24"]; ok {
		obx.PerformingOrganizationAddress = p.parseXAD(val)
	}
	if val, ok := data["25"]; ok {
		obx.PerformingOrganizationMedicalDirector = p.parseXCNList(val)
	}
	if val, ok := data["26"]; ok {
		obx.PatientResultsReleaseCategory = p.getString(val)
	}
	if val, ok := data["27"]; ok {
		obx.RootCause = p.getString(val)
	}
	if val, ok := data["28"]; ok {
		obx.LocalProcessControl = p.parseCESlice(val)
	}

	return obx
}
func (p *HL7Parser) parseNTE(data map[string]interface{}) *NTE {
	nte := &NTE{}

	if val, ok := data["1"]; ok {
		nte.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		nte.SourceOfComment = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		nte.Comment = p.getStringSlice(val)
	}
	if val, ok := data["4"]; ok {
		nte.CommentType = p.parseCE(val)
	}

	return nte
}
func (p *HL7Parser) parseRXE(data map[string]interface{}) *RXE {
	rxe := &RXE{}

	if val, ok := data["1"]; ok {
		rxe.QuantityTiming = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		rxe.GiveCode = p.parseCE(val)
	}
	if val, ok := data["3"]; ok {
		rxe.GiveAmountMinimum = p.getString(val)
	}
	if val, ok := data["4"]; ok {
		rxe.GiveAmountMaximum = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		rxe.GiveUnits = p.parseCE(val)
	}
	if val, ok := data["6"]; ok {
		rxe.GiveDosageForm = p.parseCE(val)
	}
	if val, ok := data["7"]; ok {
		rxe.ProvidersAdministrationInstructions = p.parseCESlice(val)
	}
	if val, ok := data["8"]; ok {
		rxe.DeliverToLocation = p.parsePL(val)
	}
	if val, ok := data["9"]; ok {
		rxe.SubstitutionStatus = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		rxe.DispenseAmount = p.getString(val)
	}
	if val, ok := data["11"]; ok {
		rxe.DispenseUnits = p.parseCE(val)
	}
	if val, ok := data["12"]; ok {
		rxe.NumberOfRefills = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		rxe.OrderingProvidersOrderNumber = p.parseXCNList(val)
	}
	if val, ok := data["14"]; ok {
		rxe.NumberOfRefillsRemaining = p.getString(val)
	}
	if val, ok := data["15"]; ok {
		rxe.NumberOfRefillsDosesDispensed = p.getString(val)
	}
	if val, ok := data["16"]; ok {
		rxe.DateTimeOfMostRecentRefillOrDoseDispensed = p.parseTime(val)
	}
	if val, ok := data["17"]; ok {
		rxe.TotalDailyDose = p.getString(val)
	}
	if val, ok := data["18"]; ok {
		rxe.NeedsHumanReview = p.getString(val)
	}
	if val, ok := data["19"]; ok {
		rxe.PharmacyTreatmentSuppliersSpecialDispensingInstructions = p.parseCESlice(val)
	}
	if val, ok := data["20"]; ok {
		rxe.GivePerTimeUnit = p.getString(val)
	}
	if val, ok := data["21"]; ok {
		rxe.GiveRateAmount = p.getString(val)
	}
	if val, ok := data["22"]; ok {
		rxe.GiveRateUnits = p.parseCE(val)
	}
	if val, ok := data["23"]; ok {
		rxe.GiveStrength = p.getString(val)
	}
	if val, ok := data["24"]; ok {
		rxe.GiveStrengthUnits = p.parseCE(val)
	}
	if val, ok := data["25"]; ok {
		rxe.GiveIndication = p.parseCESlice(val)
	}
	if val, ok := data["26"]; ok {
		rxe.DispensePackageSize = p.getString(val)
	}
	if val, ok := data["27"]; ok {
		rxe.DispensePackageSizeUnit = p.parseCE(val)
	}
	if val, ok := data["28"]; ok {
		rxe.DispensePackageMethod = p.getString(val)
	}
	if val, ok := data["29"]; ok {
		rxe.SupplementaryCode = p.parseCESlice(val)
	}
	if val, ok := data["30"]; ok {
		rxe.OriginalOrderDateTime = p.parseTime(val)
	}
	if val, ok := data["31"]; ok {
		rxe.GiveDrugStrengthVolume = p.getString(val)
	}
	if val, ok := data["32"]; ok {
		rxe.GiveDrugStrengthVolumeUnits = p.parseCE(val)
	}
	if val, ok := data["33"]; ok {
		rxe.ControlledSubstanceSchedule = p.parseCE(val)
	}
	if val, ok := data["34"]; ok {
		rxe.FormularyStatus = p.getString(val)
	}
	if val, ok := data["35"]; ok {
		rxe.PharmaceuticalSubstanceAlternative = p.parseCESlice(val)
	}
	if val, ok := data["36"]; ok {
		rxe.PharmacyOfMostRecentFill = p.parseCE(val)
	}
	if val, ok := data["37"]; ok {
		rxe.InitialDispenseAmount = p.getString(val)
	}
	if val, ok := data["38"]; ok {
		rxe.DispensingPharmacy = p.parseCE(val)
	}
	if val, ok := data["39"]; ok {
		rxe.DispensingPharmacyAddress = p.parseXAD(val)
	}
	if val, ok := data["40"]; ok {
		rxe.DeliverToPatientLocation = p.parsePL(val)
	}
	if val, ok := data["41"]; ok {
		rxe.DeliverToAddress = p.parseXAD(val)
	}

	return rxe
}
func (p *HL7Parser) parseRXR(data map[string]interface{}) *RXR {
	rxr := &RXR{}

	if val, ok := data["1"]; ok {
		rxr.Route = p.parseCE(val)
	}
	if val, ok := data["2"]; ok {
		rxr.AdministrationSite = p.parseCE(val)
	}
	if val, ok := data["3"]; ok {
		rxr.AdministrationDevice = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		rxr.AdministrationMethod = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		rxr.RoutingInstruction = p.parseCE(val)
	}
	if val, ok := data["6"]; ok {
		rxr.AdministrationSiteModifier = p.parseCE(val)
	}

	return rxr
}
func (p *HL7Parser) parseRXC(data map[string]interface{}) *RXC {
	rxc := &RXC{}

	if val, ok := data["1"]; ok {
		rxc.RXComponentType = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		rxc.ComponentCode = p.parseCE(val)
	}
	if val, ok := data["3"]; ok {
		rxc.ComponentAmount = p.getString(val)
	}
	if val, ok := data["4"]; ok {
		rxc.ComponentUnits = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		rxc.ComponentStrength = p.getString(val)
	}
	if val, ok := data["6"]; ok {
		rxc.ComponentStrengthUnits = p.parseCE(val)
	}
	if val, ok := data["7"]; ok {
		rxc.SupplementaryCode = p.parseCESlice(val)
	}
	if val, ok := data["8"]; ok {
		rxc.ComponentDrugStrengthVolume = p.getString(val)
	}
	if val, ok := data["9"]; ok {
		rxc.ComponentDrugStrengthVolumeUnits = p.parseCE(val)
	}

	return rxc
}
func (p *HL7Parser) parseRXA(data map[string]interface{}) *RXA {
	rxa := &RXA{}

	if val, ok := data["1"]; ok {
		rxa.GiveSubIDCounter = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		rxa.AdministrationSubIDCounter = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		rxa.DateTimeStartOfAdministration = p.parseTime(val)
	}
	if val, ok := data["4"]; ok {
		rxa.DateTimeEndOfAdministration = p.parseTime(val)
	}
	if val, ok := data["5"]; ok {
		rxa.AdministeredCode = p.parseCE(val)
	}
	if val, ok := data["6"]; ok {
		rxa.AdministeredAmount = p.getString(val)
	}
	if val, ok := data["7"]; ok {
		rxa.AdministeredUnits = p.parseCE(val)
	}
	if val, ok := data["8"]; ok {
		rxa.AdministeredDosageForm = p.parseCE(val)
	}
	if val, ok := data["9"]; ok {
		rxa.AdministrationNotes = p.parseCESlice(val)
	}
	if val, ok := data["10"]; ok {
		rxa.AdministeringProvider = p.parseXCNList(val)
	}
	if val, ok := data["11"]; ok {
		rxa.AdministeredAtLocation = p.parsePL(val)
	}
	if val, ok := data["12"]; ok {
		rxa.AdministeredPerTimeUnit = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		rxa.AdministeredStrength = p.getString(val)
	}
	if val, ok := data["14"]; ok {
		rxa.AdministeredStrengthUnits = p.parseCE(val)
	}
	if val, ok := data["15"]; ok {
		rxa.SubstanceLotNumber = p.getStringSlice(val)
	}
	if val, ok := data["16"]; ok {
		rxa.SubstanceExpirationDate = p.parseTimeSlice(val)
	}
	if val, ok := data["17"]; ok {
		rxa.SubstanceManufacturerName = p.parseCESlice(val)
	}
	if val, ok := data["18"]; ok {
		rxa.SubstanceRefusalReason = p.parseCESlice(val)
	}
	if val, ok := data["19"]; ok {
		rxa.Indication = p.parseCESlice(val)
	}
	if val, ok := data["20"]; ok {
		rxa.CompletionStatus = p.getString(val)
	}
	if val, ok := data["21"]; ok {
		rxa.ActionCodeRXA = p.getString(val)
	}
	if val, ok := data["22"]; ok {
		rxa.SystemEntryDateTime = p.parseTime(val)
	}
	if val, ok := data["23"]; ok {
		rxa.AdministeredDrugStrengthVolume = p.getString(val)
	}
	if val, ok := data["24"]; ok {
		rxa.AdministeredDrugStrengthVolumeUnits = p.parseCE(val)
	}
	if val, ok := data["25"]; ok {
		rxa.AdministeredBarcodeIdentifier = p.getString(val)
	}
	if val, ok := data["26"]; ok {
		rxa.PharmacyOrderType = p.getString(val)
	}

	return rxa
}
func (p *HL7Parser) parseMSA(data map[string]interface{}) *MSA {
	msa := &MSA{}

	if val, ok := data["1"]; ok {
		msa.AcknowledgmentCode = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		msa.MessageControlID = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		msa.TextMessage = p.getString(val)
	}
	if val, ok := data["4"]; ok {
		msa.ExpectedSequenceNumber = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		msa.DelayedAcknowledgmentType = p.getString(val)
	}
	if val, ok := data["6"]; ok {
		msa.ErrorCondition = p.parseCE(val)
	}

	return msa
}
func (p *HL7Parser) parseERR(data map[string]interface{}) *ERR {
	err := &ERR{}

	if val, ok := data["1"]; ok {
		err.ErrorCodeAndLocation = p.parseELDSlice(val)
	}
	if val, ok := data["2"]; ok {
		err.ErrorLocation = p.parseERLSlice(val)
	}
	if val, ok := data["3"]; ok {
		err.HL7ErrorCode = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		err.Severity = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		err.ApplicationErrorCode = p.parseCE(val)
	}
	if val, ok := data["6"]; ok {
		err.ApplicationErrorParameter = p.getStringSlice(val)
	}
	if val, ok := data["7"]; ok {
		err.DiagnosticInformation = p.getString(val)
	}
	if val, ok := data["8"]; ok {
		err.UserMessage = p.getString(val)
	}
	if val, ok := data["9"]; ok {
		err.InformPersonIndicator = p.getStringSlice(val)
	}
	if val, ok := data["10"]; ok {
		err.OverrideType = p.parseCE(val)
	}
	if val, ok := data["11"]; ok {
		err.OverrideReasonCode = p.parseCESlice(val)
	}
	if val, ok := data["12"]; ok {
		err.HelpDeskContactPoint = p.parseXTNList(val)
	}

	return err
}
func (p *HL7Parser) parseSCH(data map[string]interface{}) *SCH {
	sch := &SCH{}

	if val, ok := data["1"]; ok {
		sch.PlacerAppointmentID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		sch.FillerAppointmentID = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		sch.OccurrenceNumber = p.getString(val)
	}
	if val, ok := data["4"]; ok {
		sch.PlacerGroupNumber = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		sch.ScheduleID = p.getString(val)
	}
	if val, ok := data["6"]; ok {
		sch.EventReason = p.parseCE(val)
	}
	if val, ok := data["7"]; ok {
		sch.AppointmentReason = p.parseCE(val)
	}
	if val, ok := data["8"]; ok {
		sch.AppointmentType = p.parseCE(val)
	}
	if val, ok := data["9"]; ok {
		sch.AppointmentDuration = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		sch.AppointmentDurationUnits = p.parseCE(val)
	}
	if val, ok := data["11"]; ok {
		sch.AppointmentTimingQuantity = p.getStringSlice(val)
	}
	if val, ok := data["12"]; ok {
		sch.PlacerContactPerson = p.parseXCNList(val)
	}
	if val, ok := data["13"]; ok {
		sch.PlacerContactPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["14"]; ok {
		sch.PlacerContactAddress = p.parseXADSlice(val)
	}
	if val, ok := data["15"]; ok {
		sch.PlacerContactLocation = p.parsePL(val)
	}
	if val, ok := data["16"]; ok {
		sch.FillerContactPerson = p.parseXCNList(val)
	}
	if val, ok := data["17"]; ok {
		sch.FillerContactPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["18"]; ok {
		sch.FillerContactAddress = p.parseXADSlice(val)
	}
	if val, ok := data["19"]; ok {
		sch.FillerContactLocation = p.parsePL(val)
	}
	if val, ok := data["20"]; ok {
		sch.EnteredByPerson = p.parseXCNList(val)
	}
	if val, ok := data["21"]; ok {
		sch.EnteredByPhoneNumber = p.parseXTNList(val)
	}
	if val, ok := data["22"]; ok {
		sch.EnteredByLocation = p.parsePL(val)
	}
	if val, ok := data["23"]; ok {
		sch.ParentPlacerAppointmentID = p.getString(val)
	}
	if val, ok := data["24"]; ok {
		sch.ParentFillerAppointmentID = p.getString(val)
	}
	if val, ok := data["25"]; ok {
		sch.FillerStatusCode = p.parseCE(val)
	}
	if val, ok := data["26"]; ok {
		sch.PlacerStatusCode = p.parseCE(val)
	}
	if val, ok := data["27"]; ok {
		sch.FillerSupplementalServiceInformation = p.parseCESlice(val)
	}
	if val, ok := data["28"]; ok {
		sch.PlacerSupplementalServiceInformation = p.parseCESlice(val)
	}
	if val, ok := data["29"]; ok {
		sch.RequestedNewAppointmentBookingHandle = p.getString(val)
	}
	if val, ok := data["30"]; ok {
		sch.ReferencedBy = p.getString(val)
	}

	return sch
}
func (p *HL7Parser) parseRGS(data map[string]interface{}) *RGS {
	rgs := &RGS{}

	if val, ok := data["1"]; ok {
		rgs.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		rgs.SegmentActionCode = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		rgs.ResourceGroupID = p.getString(val)
	}
	if val, ok := data["4"]; ok {
		rgs.ResourceGroupName = p.getString(val)
	}
	if val, ok := data["5"]; ok {
		rgs.CoordinatedResourceGroupScheduleIDList = p.getStringSlice(val)
	}

	return rgs
}
func (p *HL7Parser) parseAIG(data map[string]interface{}) *AIG {
	aig := &AIG{}

	if val, ok := data["1"]; ok {
		aig.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		aig.SegmentActionCode = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		aig.ResourceID = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		aig.ResourceType = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		aig.ResourceGroup = p.parseCESlice(val)
	}
	if val, ok := data["6"]; ok {
		aig.ResourceQuantity = p.getString(val)
	}
	if val, ok := data["7"]; ok {
		aig.ResourceQuantityUnits = p.parseCE(val)
	}
	if val, ok := data["8"]; ok {
		aig.StartDateTime = p.parseTime(val)
	}
	if val, ok := data["9"]; ok {
		aig.StartDateTimeOffset = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		aig.StartDateTimeOffsetUnits = p.parseCE(val)
	}
	if val, ok := data["11"]; ok {
		aig.Duration = p.getString(val)
	}
	if val, ok := data["12"]; ok {
		aig.DurationUnits = p.parseCE(val)
	}
	if val, ok := data["13"]; ok {
		aig.AllowSubstitutionCode = p.getString(val)
	}
	if val, ok := data["14"]; ok {
		aig.FillerStatusCode = p.parseCE(val)
	}

	return aig
}
func (p *HL7Parser) parseAIL(data map[string]interface{}) *AIL {
	ail := &AIL{}

	if val, ok := data["1"]; ok {
		ail.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		ail.SegmentActionCode = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		ail.LocationResourceID = p.parsePLSlice(val)
	}
	if val, ok := data["4"]; ok {
		ail.LocationTypeAIL = p.parseCE(val)
	}
	if val, ok := data["5"]; ok {
		ail.LocationGroup = p.parseCE(val)
	}
	if val, ok := data["6"]; ok {
		ail.StartDateTime = p.parseTime(val)
	}
	if val, ok := data["7"]; ok {
		ail.StartDateTimeOffset = p.getString(val)
	}
	if val, ok := data["8"]; ok {
		ail.StartDateTimeOffsetUnits = p.parseCE(val)
	}
	if val, ok := data["9"]; ok {
		ail.Duration = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		ail.DurationUnits = p.parseCE(val)
	}
	if val, ok := data["11"]; ok {
		ail.AllowSubstitutionCode = p.getString(val)
	}
	if val, ok := data["12"]; ok {
		ail.FillerStatusCode = p.parseCE(val)
	}

	return ail
}
func (p *HL7Parser) parseAIS(data map[string]interface{}) *AIS {
	ais := &AIS{}

	if val, ok := data["1"]; ok {
		ais.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		ais.SegmentActionCode = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		ais.UniversalServiceIdentifier = p.parseCE(val)
	}
	if val, ok := data["4"]; ok {
		ais.StartDateTime = p.parseTime(val)
	}
	if val, ok := data["5"]; ok {
		ais.StartDateTimeOffset = p.getString(val)
	}
	if val, ok := data["6"]; ok {
		ais.StartDateTimeOffsetUnits = p.parseCE(val)
	}
	if val, ok := data["7"]; ok {
		ais.Duration = p.getString(val)
	}
	if val, ok := data["8"]; ok {
		ais.DurationUnits = p.parseCE(val)
	}
	if val, ok := data["9"]; ok {
		ais.AllowSubstitutionCode = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		ais.FillerStatusCode = p.parseCE(val)
	}

	return ais
}
func (p *HL7Parser) parseFT1(data map[string]interface{}) *FT1 {
	ft1 := &FT1{}

	if val, ok := data["1"]; ok {
		ft1.SetID = p.getString(val)
	}
	if val, ok := data["2"]; ok {
		ft1.TransactionID = p.getString(val)
	}
	if val, ok := data["3"]; ok {
		ft1.TransactionBatchID = p.getString(val)
	}
	if val, ok := data["4"]; ok {
		ft1.TransactionDate = p.parseTime(val)
	}
	if val, ok := data["5"]; ok {
		ft1.TransactionPostingDate = p.parseTime(val)
	}
	if val, ok := data["6"]; ok {
		ft1.TransactionType = p.getString(val)
	}
	if val, ok := data["7"]; ok {
		ft1.TransactionCode = p.parseCE(val)
	}
	if val, ok := data["8"]; ok {
		ft1.TransactionDescription = p.getString(val)
	}
	if val, ok := data["9"]; ok {
		ft1.TransactionDescriptionAlt = p.getString(val)
	}
	if val, ok := data["10"]; ok {
		ft1.TransactionQuantity = p.getString(val)
	}
	if val, ok := data["11"]; ok {
		ft1.TransactionAmountExtended = p.getString(val)
	}
	if val, ok := data["12"]; ok {
		ft1.TransactionAmountUnit = p.getString(val)
	}
	if val, ok := data["13"]; ok {
		ft1.DepartmentCode = p.parseCE(val)
	}
	if val, ok := data["14"]; ok {
		ft1.InsurancePlanID = p.parseCE(val)
	}
	if val, ok := data["15"]; ok {
		ft1.InsuranceAmount = p.getString(val)
	}
	if val, ok := data["16"]; ok {
		ft1.AssignedPatientLocation = p.parsePL(val)
	}
	if val, ok := data["17"]; ok {
		ft1.FeeSchedule = p.getString(val)
	}
	if val, ok := data["18"]; ok {
		ft1.PatientType = p.getString(val)
	}
	if val, ok := data["19"]; ok {
		ft1.DiagnosisCodeFT1 = p.parseCESlice(val)
	}
	if val, ok := data["20"]; ok {
		ft1.PerformedByCode = p.parseXCNList(val)
	}
	if val, ok := data["21"]; ok {
		ft1.OrderedByCode = p.parseXCNList(val)
	}
	if val, ok := data["22"]; ok {
		ft1.UnitCost = p.getString(val)
	}
	if val, ok := data["23"]; ok {
		ft1.FillerOrderNumber = p.getString(val)
	}
	if val, ok := data["24"]; ok {
		ft1.EnteredByCode = p.parseXCNList(val)
	}
	if val, ok := data["25"]; ok {
		ft1.ProcedureCode = p.parseCE(val)
	}
	if val, ok := data["26"]; ok {
		ft1.ProcedureCodeModifier = p.parseCESlice(val)
	}

	return ft1
}
func (p *HL7Parser) parseXCN(val interface{}) *XCN {
	components := p.getComponents(val)
	if len(components) == 0 {
		return nil
	}

	xcn := &XCN{}
	if len(components) > 0 {
		xcn.IDNumber = components[0]
	}
	if len(components) > 1 {
		xcn.FamilyName = components[1]
	}
	if len(components) > 2 {
		xcn.GivenName = components[2]
	}
	if len(components) > 3 {
		xcn.SecondAndFurtherGivenNames = components[3]
	}
	if len(components) > 4 {
		xcn.Suffix = components[4]
	}
	if len(components) > 5 {
		xcn.Prefix = components[5]
	}
	if len(components) > 6 {
		xcn.Degree = components[6]
	}
	if len(components) > 7 {
		xcn.SourceTable = components[7]
	}
	if len(components) > 8 {
		xcn.AssigningAuthority = p.parseHD(components[8])
	}
	if len(components) > 9 {
		xcn.NameTypeCode = components[9]
	}
	if len(components) > 10 {
		xcn.IdentifierCheckDigit = components[10]
	}
	if len(components) > 11 {
		xcn.CheckDigitScheme = components[11]
	}
	if len(components) > 12 {
		xcn.IdentifierTypeCode = components[12]
	}
	if len(components) > 13 {
		xcn.AssigningFacility = p.parseHD(components[13])
	}
	if len(components) > 14 {
		xcn.NameRepresentationCode = components[14]
	}
	if len(components) > 15 {
		xcn.NameContext = p.parseCE(components[15])
	}
	if len(components) > 16 {
		xcn.NameValidityRange = components[16]
	}
	if len(components) > 17 {
		xcn.NameAssemblyOrder = components[17]
	}
	if len(components) > 18 {
		xcn.EffectiveDate = p.parseTime(components[18])
	}
	if len(components) > 19 {
		xcn.ExpirationDate = p.parseTime(components[19])
	}
	if len(components) > 20 {
		xcn.ProfessionalSuffix = components[20]
	}
	if len(components) > 21 {
		xcn.AssigningJurisdiction = p.parseCWE(components[21])
	}
	if len(components) > 22 {
		xcn.AssigningAgencyOrDepartment = p.parseCWE(components[22])
	}

	return xcn
}

func (p *HL7Parser) parseXCNList(val interface{}) []*XCN {
	switch v := val.(type) {
	case string:
		if xcn := p.parseXCN(v); xcn != nil {
			return []*XCN{xcn}
		}
	case []interface{}:
		result := make([]*XCN, 0, len(v))
		for _, item := range v {
			if xcn := p.parseXCN(item); xcn != nil {
				result = append(result, xcn)
			}
		}
		return result
	}
	return nil
}
