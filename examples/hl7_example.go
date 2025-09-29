package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/adapters/ioadapter"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

// Sample HL7 messages for testing idempotency and deduplication
var sampleHL7Messages = `MSH|^~\&|SEND_APP|SEND_FAC|REC_APP|REC_FAC|20220301120000||ADT^A01|12345|P|2.5
EVN||200310010800|||^KOWALSKI^ROBERT^^^^B|200310010800
PID|1||123456789||DOE^JOHN^MIDDLE||19850215|M||W|123 MAIN ST^APT 1^CITY^ST^12345||(555)123-4567||(555)123-9999||S||12345|||||||||||||||||
PV1|1|I|ICU^201^A|EL||||||SUR||||19|V|||||||||||||||||||||||||200310010800

MSH|^~\&|LAB_SYS|LAB|REC_APP|REC_FAC|20220301130000||ORU^R01|67890|P|2.5
PID|1||987654321||SMITH^JANE^||19900412|F||B|456 OAK AVE^UNIT 2^TOWN^ST^67890||(555)987-6543||||||||||||||||||||||
OBR|1|12345^LAB|67890^LAB|CBC^COMPLETE BLOOD COUNT|R|20220301120000|20220301130000||||||||DOC123^DOCTOR^FIRST||||20220301130000||F
OBX|1|NM|WBC^WHITE BLOOD COUNT||7.2|10*3/uL|4.5-11.0|N|||F

MSH|^~\&|SEND_APP|SEND_FAC|REC_APP|REC_FAC|20220301120000||ADT^A01|12345|P|2.5
EVN||200310010800|||^KOWALSKI^ROBERT^^^^B|200310010800
PID|1||123456789||DOE^JOHN^MIDDLE||19850215|M||W|123 MAIN ST^APT 1^CITY^ST^12345||(555)123-4567||(555)123-9999||S||12345|||||||||||||||||
PV1|1|I|ICU^201^A|EL||||||SUR||||19|V|||||||||||||||||||||||||200310010800`

func main() {
	ctx := context.Background()

	// Create idempotency manager for deduplication
	idempotencyManager := etl.NewIdempotencyManager(
		"/tmp/hl7_idempotency.json", // persist file
		24*time.Hour,                // TTL - 24 hours
		10000,                       // max keys
	)
	defer idempotencyManager.Stop()

	// Create HL7 source adapter
	reader := strings.NewReader(sampleHL7Messages)
	source := ioadapter.NewSource(reader, "hl7")

	// Create output file for JSON results (not used in this example)
	// outputFile := "/tmp/hl7_output.json"

	// Use WithDestination with stdout for now since we need a simpler approach
	destConfig := config.DataConfig{
		Type:   "stdout",
		Format: "json",
	}

	// Create ETL pipeline with idempotency and deduplication
	pipeline := etl.NewETL(
		"hl7-pipeline-001",
		"HL7 Processing Pipeline with Idempotency",
		etl.WithSources(source),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithIdempotencyManager(idempotencyManager),
		etl.WithDeduplication("idempotency_key"), // Use our idempotency key for deduplication
		etl.WithBatchSize(10),
		etl.WithWorkerCount(2),
		// Add HL7-specific transformers and mappers
		etl.WithTransformers(&HL7MessageTransformer{}),
		etl.WithMappers(&HL7PatientMapper{}),
	)

	// Run the pipeline
	fmt.Println("=== Running HL7 Processing Pipeline with Idempotency ===")
	if err := pipeline.Run(ctx); err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	// Display idempotency stats
	stats := idempotencyManager.GetStats()
	fmt.Println("\n=== Idempotency Statistics (First Run) ===")
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	// Test deduplication by running the same messages again
	fmt.Println("\n\n=== Testing Deduplication (Running same messages again) ===")
	reader2 := strings.NewReader(sampleHL7Messages)
	source2 := ioadapter.NewSource(reader2, "hl7")

	pipeline2 := etl.NewETL(
		"hl7-dedup-test-002",
		"HL7 Deduplication Test",
		etl.WithSources(source2),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithIdempotencyManager(idempotencyManager),
		etl.WithDeduplication("idempotency_key"),
		etl.WithBatchSize(10),
		etl.WithWorkerCount(2),
		etl.WithTransformers(&HL7MessageTransformer{}),
		etl.WithMappers(&HL7PatientMapper{}),
	)

	if err := pipeline2.Run(ctx); err != nil {
		log.Fatalf("Second pipeline failed: %v", err)
	}

	// Final idempotency stats
	finalStats := idempotencyManager.GetStats()
	fmt.Println("\n=== Final Idempotency Statistics (After Deduplication Test) ===")
	for key, value := range finalStats {
		fmt.Printf("%s: %v\n", key, value)
	}

	// Display some idempotency key information
	fmt.Println("\n=== Sample Idempotency Key Information ===")
	sampleKeys := []string{"msh_12345", "pid_123456789", "pid_987654321", "obr_67890^LAB"}
	for _, key := range sampleKeys {
		if info, exists := idempotencyManager.GetKeyInfo(key); exists {
			fmt.Printf("Key: %s\n", info.Key)
			fmt.Printf("  Status: %s\n", info.Status)
			fmt.Printf("  Count: %d\n", info.Count)
			fmt.Printf("  First Seen: %s\n", info.FirstSeen.Format("15:04:05"))
			fmt.Printf("  Last Seen: %s\n", info.LastSeen.Format("15:04:05"))
			fmt.Println()
		}
	}

	// Show example of processing individual records with idempotency
	fmt.Println("=== Example: Processing Individual Records with Idempotency ===")
	testIndividualRecordProcessing(idempotencyManager)
}

// testIndividualRecordProcessing demonstrates using the idempotency manager directly
func testIndividualRecordProcessing(im *etl.IdempotencyManager) {
	// Sample HL7 record
	record := utils.Record{
		"segment":         "PID",
		"field3":          "999888777",
		"field5":          "TEST^PATIENT^MIDDLE",
		"message_type":    "patient",
		"idempotency_key": "pid_999888777",
	}

	// Define key fields for idempotency
	keyFields := []string{"idempotency_key"}

	// Process the record with idempotency
	processor := func(rec utils.Record) error {
		fmt.Printf("Processing record: %v\n", rec["idempotency_key"])
		// Simulate some processing
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// First processing attempt
	fmt.Println("First processing attempt:")
	if err := im.ProcessWithIdempotency(context.Background(), record, keyFields, processor); err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Second processing attempt (should be skipped due to idempotency)
	fmt.Println("Second processing attempt (should be skipped):")
	if err := im.ProcessWithIdempotency(context.Background(), record, keyFields, processor); err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Show key info
	if info, exists := im.GetKeyInfo("pid_999888777"); exists {
		fmt.Printf("Idempotency info - Status: %s, Count: %d\n", info.Status, info.Count)
	}
}

// HL7MessageTransformer normalizes HL7 message data and adds idempotency keys
type HL7MessageTransformer struct{}

func (t *HL7MessageTransformer) Name() string {
	return "HL7MessageTransformer"
}

func (t *HL7MessageTransformer) Transform(ctx context.Context, record utils.Record) (utils.Record, error) {
	// Extract message segment type for classification
	segment, ok := record["segment"].(string)
	if !ok {
		return record, nil
	}

	// Add message type classification and generate idempotency keys
	switch segment {
	case "MSH":
		record["message_type"] = "header"
		// Extract control ID from field9 for idempotency
		if controlID, exists := record["field9"]; exists {
			record["control_id"] = controlID
			record["idempotency_key"] = fmt.Sprintf("msh_%v", controlID)
		}
		// Extract message type from field8
		if msgType, exists := record["field8"]; exists {
			record["hl7_message_type"] = msgType
		}

	case "PID":
		record["message_type"] = "patient"
		// Extract patient ID from field3 for idempotency
		if patientID, exists := record["field3"]; exists {
			record["patient_id"] = patientID
			record["idempotency_key"] = fmt.Sprintf("pid_%v", patientID)
		}

	case "PV1":
		record["message_type"] = "visit"
		// Use patient location and timestamp for idempotency
		location := getFieldAsString(record, "field3")
		timestamp := getFieldAsString(record, "field44")
		record["idempotency_key"] = fmt.Sprintf("pv1_%s_%s", location, timestamp)

	case "OBR":
		record["message_type"] = "order"
		if orderID, exists := record["field3"]; exists {
			record["order_id"] = orderID
			record["idempotency_key"] = fmt.Sprintf("obr_%v", orderID)
		}

	case "OBX":
		record["message_type"] = "observation"
		// Combine observation sequence, type, and timestamp for uniqueness
		obsSeq := getFieldAsString(record, "field1")
		obsType := getFieldAsString(record, "field4")
		obsTimestamp := getFieldAsString(record, "field14")
		record["idempotency_key"] = fmt.Sprintf("obx_%s_%s_%s", obsSeq, obsType, obsTimestamp)

	default:
		record["message_type"] = "unknown"
		field1 := getFieldAsString(record, "field1")
		record["idempotency_key"] = fmt.Sprintf("unknown_%s_%s", segment, field1)
	}

	// Add processing timestamp
	record["processed_at"] = time.Now().Format(time.RFC3339)

	// Add validation flags
	record["is_valid"] = t.validateHL7Record(record)

	return record, nil
}

// Helper function to safely get field as string
func getFieldAsString(record utils.Record, fieldName string) string {
	if val, exists := record[fieldName]; exists {
		return fmt.Sprintf("%v", val)
	}
	return "unknown"
}

// validateHL7Record performs basic HL7 validation
func (t *HL7MessageTransformer) validateHL7Record(record utils.Record) bool {
	segment, ok := record["segment"].(string)
	if !ok {
		return false
	}

	// Basic validation rules per segment type
	switch segment {
	case "MSH":
		// MSH must have message type and control ID
		return record["field8"] != nil && record["field9"] != nil
	case "PID":
		// PID must have patient ID and name
		return record["field3"] != nil && record["field5"] != nil
	case "PV1", "OBR", "OBX":
		// These segments should have at least sequence number
		return record["field1"] != nil
	default:
		return true // Unknown segments are considered valid
	}
}

// HL7PatientMapper maps HL7 patient data to standardized format
type HL7PatientMapper struct{}

func (m *HL7PatientMapper) Name() string {
	return "HL7PatientMapper"
}

func (m *HL7PatientMapper) Map(ctx context.Context, record utils.Record) (utils.Record, error) {
	messageType, ok := record["message_type"].(string)
	if !ok || messageType != "patient" {
		return record, nil // Only process patient records
	}

	mapped := make(utils.Record)

	// Copy all original fields
	for k, v := range record {
		mapped[k] = v
	}

	// Parse patient name from field5 (format: LAST^FIRST^MIDDLE)
	if nameField, exists := record["field5"]; exists {
		if nameStr, ok := nameField.(string); ok {
			nameParts := strings.Split(nameStr, "^")
			if len(nameParts) >= 2 {
				mapped["patient_last_name"] = strings.TrimSpace(nameParts[0])
				mapped["patient_first_name"] = strings.TrimSpace(nameParts[1])

				if len(nameParts) >= 3 && strings.TrimSpace(nameParts[2]) != "" {
					mapped["patient_middle_name"] = strings.TrimSpace(nameParts[2])
					mapped["patient_full_name"] = fmt.Sprintf("%s, %s %s",
						nameParts[0], nameParts[1], nameParts[2])
				} else {
					mapped["patient_full_name"] = fmt.Sprintf("%s, %s",
						nameParts[0], nameParts[1])
				}
			}
		}
	}

	// Parse date of birth from field7 (format: YYYYMMDD)
	if dobField, exists := record["field7"]; exists {
		if dobStr, ok := dobField.(string); ok && len(dobStr) == 8 {
			if parsed, err := time.Parse("20060102", dobStr); err == nil {
				mapped["patient_dob"] = parsed.Format("2006-01-02")
				mapped["patient_dob_formatted"] = parsed.Format("January 2, 2006")

				// Calculate age
				now := time.Now()
				age := now.Year() - parsed.Year()
				if now.YearDay() < parsed.YearDay() {
					age--
				}
				mapped["patient_age"] = age
			}
		}
	}

	// Map gender from field8
	if genderField, exists := record["field8"]; exists {
		switch strings.ToUpper(fmt.Sprintf("%v", genderField)) {
		case "M":
			mapped["patient_gender"] = "Male"
			mapped["patient_gender_code"] = "M"
		case "F":
			mapped["patient_gender"] = "Female"
			mapped["patient_gender_code"] = "F"
		case "O":
			mapped["patient_gender"] = "Other"
			mapped["patient_gender_code"] = "O"
		default:
			mapped["patient_gender"] = "Unknown"
			mapped["patient_gender_code"] = "U"
		}
	}

	// Parse address from field11 (format: STREET^UNIT^CITY^STATE^ZIP)
	if addrField, exists := record["field11"]; exists {
		if addrStr, ok := addrField.(string); ok {
			addrParts := strings.Split(addrStr, "^")
			if len(addrParts) >= 5 {
				mapped["patient_street"] = strings.TrimSpace(addrParts[0])
				mapped["patient_unit"] = strings.TrimSpace(addrParts[1])
				mapped["patient_city"] = strings.TrimSpace(addrParts[2])
				mapped["patient_state"] = strings.TrimSpace(addrParts[3])
				mapped["patient_zip"] = strings.TrimSpace(addrParts[4])

				// Build full address
				unit := mapped["patient_unit"].(string)
				fullAddr := mapped["patient_street"].(string)
				if unit != "" {
					fullAddr += " " + unit
				}
				fullAddr += ", " + mapped["patient_city"].(string) + ", " +
					mapped["patient_state"].(string) + " " + mapped["patient_zip"].(string)
				mapped["patient_full_address"] = fullAddr
			}
		}
	}

	// Parse phone numbers from field13 and field14
	if phoneField, exists := record["field13"]; exists {
		if phoneStr, ok := phoneField.(string); ok {
			mapped["patient_home_phone"] = strings.TrimSpace(phoneStr)
		}
	}

	if businessPhoneField, exists := record["field14"]; exists {
		if phoneStr, ok := businessPhoneField.(string); ok {
			mapped["patient_business_phone"] = strings.TrimSpace(phoneStr)
		}
	}

	return mapped, nil
}
