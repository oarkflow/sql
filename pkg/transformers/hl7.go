package transformers

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/sql/pkg/parsers"
	"github.com/oarkflow/sql/pkg/utils"
)

// HL7TransformerOptions controls how HL7 transformations populate records.
type HL7TransformerOptions struct {
	InputField           string
	OutputJSONField      string
	OutputXMLField       string
	OutputSegmentsField  string
	OutputTypedField     string
	OutputDocumentField  string
	MessageTypeField     string
	ControlIDField       string
	TimestampField       string
	OutputTypedJSONField string
	OutputTypedXMLField  string
}

// HL7Transformer parses HL7 payloads and generates JSON/XML/metadata outputs.
type HL7Transformer struct {
	parser *parsers.HL7Parser
	opts   HL7TransformerOptions
}

// NewHL7Transformer builds a transformer with sane defaults.
func NewHL7Transformer(opts HL7TransformerOptions) *HL7Transformer {
	if opts.InputField == "" {
		opts.InputField = "raw_message"
	}
	if opts.OutputJSONField == "" {
		opts.OutputJSONField = "hl7_json"
	}
	if opts.OutputXMLField == "" {
		opts.OutputXMLField = "hl7_xml"
	}
	if opts.OutputTypedJSONField == "" {
		opts.OutputTypedJSONField = "hl7_json_typed"
	}
	if opts.OutputTypedXMLField == "" {
		opts.OutputTypedXMLField = "hl7_xml_typed"
	}
	if opts.OutputSegmentsField == "" {
		opts.OutputSegmentsField = "hl7_segments"
	}
	if opts.OutputDocumentField == "" {
		opts.OutputDocumentField = "hl7_document"
	}
	if opts.MessageTypeField == "" {
		opts.MessageTypeField = "hl7_message_type"
	}
	if opts.ControlIDField == "" {
		opts.ControlIDField = "hl7_control_id"
	}
	if opts.TimestampField == "" {
		opts.TimestampField = "hl7_timestamp"
	}
	return &HL7Transformer{
		parser: parsers.NewHL7Parser(),
		opts:   opts,
	}
}

// Name returns the human friendly transformer name.
func (t *HL7Transformer) Name() string {
	return "HL7Transformer"
}

// Transform parses the HL7 message stored in InputField and enriches the record.
func (t *HL7Transformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	rawValue, ok := rec[t.opts.InputField]
	if !ok {
		return rec, fmt.Errorf("hl7 transformer: missing input field %s", t.opts.InputField)
	}
	rawMessage, err := toString(rawValue)
	if err != nil {
		return rec, fmt.Errorf("hl7 transformer: %w", err)
	}
	if strings.TrimSpace(rawMessage) == "" {
		return rec, fmt.Errorf("hl7 transformer: input field %s is empty", t.opts.InputField)
	}

	doc, err := t.parser.ParseDocument(rawMessage)
	if err != nil {
		return rec, fmt.Errorf("hl7 transformer: %w", err)
	}

	if t.opts.OutputJSONField != "" {
		rec[t.opts.OutputJSONField] = string(doc.JSON)
	}
	if t.opts.OutputXMLField != "" {
		rec[t.opts.OutputXMLField] = string(doc.XML)
	}
	if t.opts.OutputTypedJSONField != "" {
		data := doc.TypedJSON
		if len(data) == 0 {
			data = doc.JSON
		}
		rec[t.opts.OutputTypedJSONField] = string(data)
	}
	if t.opts.OutputTypedXMLField != "" {
		data := doc.TypedXML
		if len(data) == 0 {
			data = doc.XML
		}
		rec[t.opts.OutputTypedXMLField] = string(data)
	}
	if t.opts.OutputSegmentsField != "" {
		rec[t.opts.OutputSegmentsField] = doc.RawSegments
	}
	if t.opts.OutputDocumentField != "" {
		rec[t.opts.OutputDocumentField] = doc
	}
	if t.opts.OutputTypedField != "" && doc.Typed != nil {
		rec[t.opts.OutputTypedField] = doc.Typed
	}
	if t.opts.MessageTypeField != "" {
		rec[t.opts.MessageTypeField] = doc.MessageType
	}
	if t.opts.ControlIDField != "" {
		rec[t.opts.ControlIDField] = doc.ControlID
	}
	if t.opts.TimestampField != "" {
		rec[t.opts.TimestampField] = doc.Timestamp
	}

	return rec, nil
}

func toString(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}
