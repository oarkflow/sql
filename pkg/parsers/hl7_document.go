package parsers

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/oarkflow/json"
)

// HL7Document represents a parsed HL7 message with multiple renderings.
type HL7Document struct {
	MessageType string              `json:"message_type"`
	ControlID   string              `json:"control_id"`
	Timestamp   time.Time           `json:"timestamp"`
	RawSegments map[string]any      `json:"raw_segments"`
	Typed       HL7MessageInterface `json:"-"`
	JSON        []byte              `json:"-"`
	XML         []byte              `json:"-"`
	TypedJSON   []byte              `json:"-"`
	TypedXML    []byte              `json:"-"`
}

// ParseDocument parses the HL7 message and prepares JSON/XML renderings.
func (p *HL7Parser) ParseDocument(message string) (*HL7Document, error) {
	parsed, err := p.ParseString(message)
	if err != nil {
		return nil, err
	}
	named := p.toNamedSegments(parsed)

	typed, err := p.ParseTyped(message)
	if err != nil && !strings.Contains(err.Error(), "unsupported message type") {
		return nil, err
	}

	var typedJSON []byte
	var typedXML []byte
	if typed != nil {
		if data, err := json.MarshalIndent(typed, "", "  "); err == nil {
			typedJSON = data
		}
		if data, err := xml.MarshalIndent(typed, "", "  "); err == nil {
			typedXML = append([]byte(xml.Header), data...)
		}
	}

	jsonBytes := typedJSON
	if len(jsonBytes) == 0 {
		data, err := json.MarshalIndent(named, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to marshal HL7 JSON: %w", err)
		}
		jsonBytes = data
	}

	xmlBytes := typedXML
	if len(xmlBytes) == 0 {
		data, err := buildHL7XML(named)
		if err != nil {
			return nil, fmt.Errorf("failed to build HL7 XML: %w", err)
		}
		xmlBytes = data
	}

	doc := &HL7Document{
		RawSegments: named,
		Typed:       typed,
		JSON:        jsonBytes,
		XML:         xmlBytes,
		TypedJSON:   typedJSON,
		TypedXML:    typedXML,
	}

	if typed != nil {
		doc.MessageType = typed.GetMessageType()
		doc.ControlID = typed.GetMessageControlID()
		doc.Timestamp = typed.GetTimestamp()
	} else {
		if mshList, ok := parsed["MSH"].([]map[string]any); ok && len(mshList) > 0 {
			msh := mshList[0]
			if mt, ok := msh["8"].(string); ok {
				doc.MessageType = mt
			}
			if control, ok := msh["10"].(string); ok {
				doc.ControlID = control
			}
			if ts, ok := msh["7"].(string); ok {
				if parsed, err := parseHL7Timestamp(ts); err == nil {
					doc.Timestamp = parsed
				}
			}
		}
	}

	return doc, nil
}

// ToJSON renders an HL7 message directly to JSON bytes.
func (p *HL7Parser) ToJSON(message string) ([]byte, error) {
	doc, err := p.ParseDocument(message)
	if err != nil {
		return nil, err
	}
	return doc.JSON, nil
}

// ToXML renders an HL7 message to XML bytes.
func (p *HL7Parser) ToXML(message string) ([]byte, error) {
	doc, err := p.ParseDocument(message)
	if err != nil {
		return nil, err
	}
	return doc.XML, nil
}

func buildHL7XML(data map[string]any) ([]byte, error) {
	buf := &bytes.Buffer{}
	encoder := xml.NewEncoder(buf)
	encoder.Indent("", "  ")

	root := xml.StartElement{Name: xml.Name{Local: "HL7Message"}}
	if err := encoder.EncodeToken(root); err != nil {
		return nil, err
	}

	segmentNames := make([]string, 0, len(data))
	for name := range data {
		segmentNames = append(segmentNames, name)
	}
	sort.Strings(segmentNames)

	for _, segmentName := range segmentNames {
		entries, ok := data[segmentName].([]map[string]any)
		if !ok {
			continue
		}
		for _, entry := range entries {
			if err := encodeHL7Segment(encoder, segmentName, entry); err != nil {
				return nil, err
			}
		}
	}

	if err := encoder.EncodeToken(root.End()); err != nil {
		return nil, err
	}
	if err := encoder.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeHL7Segment(encoder *xml.Encoder, name string, fields map[string]any) error {
	start := xml.StartElement{Name: xml.Name{Local: sanitizeXMLName(name)}}
	if err := encoder.EncodeToken(start); err != nil {
		return err
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		fieldName := key
		if _, err := strconv.Atoi(key); err == nil {
			fieldName = fmt.Sprintf("Field%s", key)
		}
		if err := encodeHL7Value(encoder, fieldName, fields[key]); err != nil {
			return err
		}
	}

	return encoder.EncodeToken(start.End())
}

func encodeHL7Value(encoder *xml.Encoder, name string, value any) error {
	switch v := value.(type) {
	case string:
		return encodeSimpleElement(encoder, name, v)
	case []any:
		for idx, item := range v {
			itemName := name
			if len(v) > 1 {
				itemName = fmt.Sprintf("%s_%d", name, idx+1)
			}
			if err := encodeHL7Value(encoder, itemName, item); err != nil {
				return err
			}
		}
	case map[string]any:
		start := xml.StartElement{Name: xml.Name{Local: sanitizeXMLName(name)}}
		if err := encoder.EncodeToken(start); err != nil {
			return err
		}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			innerName := key
			if _, err := strconv.Atoi(key); err == nil {
				innerName = fmt.Sprintf("Component%s", key)
			}
			if err := encodeHL7Value(encoder, innerName, v[key]); err != nil {
				return err
			}
		}
		return encoder.EncodeToken(start.End())
	case nil:
		return nil
	default:
		return encodeSimpleElement(encoder, name, fmt.Sprintf("%v", v))
	}
	return nil
}

func encodeSimpleElement(encoder *xml.Encoder, name, value string) error {
	start := xml.StartElement{Name: xml.Name{Local: sanitizeXMLName(name)}}
	if err := encoder.EncodeToken(start); err != nil {
		return err
	}
	if err := encoder.EncodeToken(xml.CharData([]byte(value))); err != nil {
		return err
	}
	return encoder.EncodeToken(start.End())
}

func sanitizeXMLName(name string) string {
	if name == "" {
		return "Field"
	}
	var builder strings.Builder
	runes := []rune(name)
	first := runes[0]
	if !isXMLNameStart(first) {
		builder.WriteRune('_')
	}
	for _, r := range runes {
		if isXMLNameChar(r) {
			builder.WriteRune(r)
		} else {
			builder.WriteRune('_')
		}
	}
	return builder.String()
}

func isXMLNameStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_' || r == ':'
}

func isXMLNameChar(r rune) bool {
	return isXMLNameStart(r) || unicode.IsDigit(r) || r == '-' || r == '.'
}

func parseHL7Timestamp(value string) (time.Time, error) {
	formats := []string{
		"20060102150405Z07:00",
		"20060102150405-0700",
		"20060102150405",
		"200601021504",
		"20060102",
	}
	for _, layout := range formats {
		if len(value) < len(layout) {
			continue
		}
		parsed, err := time.Parse(layout, value[:len(layout)])
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported HL7 timestamp: %s", value)
}
