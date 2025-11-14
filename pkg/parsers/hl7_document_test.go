package parsers

import (
	"strings"
	"testing"
)

const sampleHL7Message = "MSH|^~\\&|SENDING|FACILITY|RECEIVER|RECEIVER|20220301120000||ADT^A01|MSG0001|P|2.5\r" +
	"PID|1||123456^^^HOSP^MR||DOE^JANE||19800101|F|||123 MAIN ST^^CITY^ST^12345||5551234567|||S" +
	"\rPV1|1|I|WARD^101^1^^HOSP||||1234^PHYSICIAN^PRIMARY||SUR|||||||1234567"

func TestParseDocumentProducesJSONAndXML(t *testing.T) {
	parser := NewHL7Parser()
	doc, err := parser.ParseDocument(sampleHL7Message)
	if err != nil {
		t.Fatalf("ParseDocument returned error: %v", err)
	}

	if doc.MessageType == "" {
		t.Fatalf("expected message type to be populated")
	}
	mshSegments, ok := doc.RawSegments["MSH"].([]map[string]any)
	if !ok || len(mshSegments) == 0 {
		t.Fatalf("expected MSH segment in raw segments")
	}
	if _, ok := mshSegments[0]["sending_application"]; !ok {
		t.Fatalf("expected friendly field names in raw segments, got: %#v", mshSegments[0])
	}
	if len(doc.JSON) == 0 {
		t.Fatalf("expected JSON payload")
	}
	if !strings.Contains(string(doc.JSON), "\"msh\"") {
		t.Fatalf("expected typed JSON payload, got: %s", string(doc.JSON))
	}
	if len(doc.XML) == 0 {
		t.Fatalf("expected XML payload")
	}
	if !strings.Contains(string(doc.XML), "<MSH>") {
		t.Fatalf("XML payload missing MSH segment: %s", string(doc.XML))
	}
}

func TestToJSONAndToXML(t *testing.T) {
	parser := NewHL7Parser()
	jsonPayload, err := parser.ToJSON(sampleHL7Message)
	if err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}
	if !strings.Contains(string(jsonPayload), "\"msh\"") {
		t.Fatalf("JSON payload missing typed MSH segment: %s", string(jsonPayload))
	}
	if !strings.Contains(string(jsonPayload), "sending_application") {
		t.Fatalf("JSON payload missing friendly field names: %s", string(jsonPayload))
	}

	xmlPayload, err := parser.ToXML(sampleHL7Message)
	if err != nil {
		t.Fatalf("ToXML error: %v", err)
	}
	if !strings.Contains(string(xmlPayload), "<PID>") {
		t.Fatalf("XML payload missing PID segment")
	}
}
