package bridglink

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
)

func TestRunPipelineHL7ToJSON(t *testing.T) {
	tempDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current working directory: %v", err)
	}
	inputPath := filepath.Join(tempDir, "input.hl7")
	outputPath := filepath.Join(tempDir, "output.json")

	if err := os.Remove(outputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed to clean output file: %v", err)
	}

	hl7Message := "MSH|^~\\&|SEND|FAC|RCV|RCV|20220301120000||ADT^A01|MSG0001|P|2.5\r" +
		"PID|1||123456^^^HOSP^MR||DOE^JOHN||19800101|M|||123 MAIN ST^^CITY^ST^12345||5551234567\r"
	if err := os.WriteFile(inputPath, []byte(hl7Message), 0o644); err != nil {
		t.Fatalf("failed to write hl7 file: %v", err)
	}

	cfg := &config.BridgLinkConfig{
		Version: "1.0",
		Connectors: map[string]config.ConnectorSpec{
			"hl7_file": {
				Protocol: "hl7",
				Format:   "hl7",
				Config: map[string]any{
					"file": inputPath,
				},
			},
			"json_file": {
				Protocol: "json",
				Format:   "json",
				Config: map[string]any{
					"file": outputPath,
				},
			},
		},
		Pipelines: []config.PipelineSpec{
			{
				ID:           "hl7_to_json",
				Name:         "hl7_to_json",
				Source:       "hl7_file",
				Destinations: []string{"json_file"},
				Parsers: []config.ParserSpec{
					{
						Name: "hl7",
						Options: map[string]any{
							"input_field": "raw_message",
							"json_field":  "payload_json",
							"xml_field":   "payload_xml",
						},
					},
				},
			},
		},
	}

	manager, err := NewManager(cfg, etl.NewManager())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	etlCfg, err := manager.BuildETLConfig("hl7_to_json")
	if err != nil {
		t.Fatalf("BuildETLConfig error: %v", err)
	}
	if len(etlCfg.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(etlCfg.Tables))
	}
	if len(etlCfg.Tables[0].Transformers) == 0 {
		t.Fatalf("expected transformers to be configured")
	}

	if _, err := manager.RunPipeline(context.Background(), "hl7_to_json"); err != nil {
		t.Fatalf("RunPipeline failed: %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	var parsed []map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to parse json output: %v", err)
	}
	if len(parsed) == 0 {
		t.Fatalf("expected at least one record in output")
	}
	payload, ok := parsed[0]["payload_json"]
	if !ok {
		t.Fatalf("payload_json field missing: %#v", parsed[0])
	}
	payloadStr, ok := payload.(string)
	if !ok || payloadStr == "" {
		t.Fatalf("payload_json field empty or not string: %#v", payload)
	}

	var payloadJSON map[string]any
	if err := json.Unmarshal([]byte(payloadStr), &payloadJSON); err != nil {
		t.Fatalf("failed to unmarshal payload_json: %v", err)
	}

	msh, ok := payloadJSON["msh"].(map[string]any)
	if !ok {
		t.Fatalf("typed payload missing MSH segment: %#v", payloadJSON)
	}

	messageType, ok := msh["message_type"].(map[string]any)
	if !ok {
		t.Fatalf("typed payload missing message_type: %#v", msh)
	}
	if code, _ := messageType["message_code"].(string); code != "ADT" {
		t.Fatalf("unexpected message_code: %#v", messageType)
	}
	if trigger, _ := messageType["trigger_event"].(string); trigger != "A01" {
		t.Fatalf("unexpected trigger_event: %#v", messageType)
	}

	sendingApp, ok := msh["sending_application"].(map[string]any)
	if !ok {
		t.Fatalf("typed payload missing sending_application: %#v", msh)
	}
	if namespace, _ := sendingApp["namespace_id"].(string); namespace != "SEND" {
		t.Fatalf("unexpected sending_application namespace_id: %#v", sendingApp)
	}
}
