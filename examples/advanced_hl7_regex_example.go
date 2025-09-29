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

// Regex pattern example for parsing log-like data
var regexPatternData = `127.0.0.1|"User login successful"
192.168.1.100|"Failed authentication attempt"
10.0.0.5|"Database query executed"
127.0.0.1|"User logout"
192.168.1.100|"Failed authentication attempt"
127.0.0.1|"User login successful"`

func main() {
	ctx := context.Background()

	fmt.Println("=== Testing Regex Pattern Parser with Idempotency ===")
	testRegexPatternParser(ctx)

	fmt.Println("\n=== Testing Enhanced HL7 Processing with Direct Idempotency ===")
	testHL7WithDirectIdempotency(ctx)
}

// testRegexPatternParser demonstrates regex pattern parsing with <ip>|<msg> format
func testRegexPatternParser(ctx context.Context) {
	// Create idempotency manager
	idempotencyManager := etl.NewIdempotencyManager(
		"/tmp/regex_idempotency.json",
		1*time.Hour,
		1000,
	)
	defer idempotencyManager.Stop()

	// Create regex source adapter
	reader := strings.NewReader(regexPatternData)
	source := ioadapter.NewSource(reader, "regex")
	source.SetPattern("<ip>|<msg>") // Set the regex pattern

	destConfig := config.DataConfig{
		Type:   "stdout",
		Format: "json",
	}

	// Create pipeline with regex transformer
	pipeline := etl.NewETL(
		"regex-pipeline-001",
		"Regex Pattern Pipeline with Idempotency",
		etl.WithSources(source),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithIdempotencyManager(idempotencyManager),
		etl.WithBatchSize(5),
		etl.WithWorkerCount(1),
		etl.WithTransformers(&RegexLogTransformer{IdempotencyMgr: idempotencyManager}),
	)

	// Run the pipeline
	if err := pipeline.Run(ctx); err != nil {
		log.Fatalf("Regex pipeline failed: %v", err)
	}

	// Show idempotency stats
	stats := idempotencyManager.GetStats()
	fmt.Println("\n=== Regex Pipeline Idempotency Statistics ===")
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
}

// testHL7WithDirectIdempotency shows direct integration with idempotency manager
func testHL7WithDirectIdempotency(ctx context.Context) {
	// Sample HL7 message with duplicates
	hl7Data := `MSH|^~\&|SYS1|FAC1|REC|REC_FAC|20220301140000||ADT^A01|11111|P|2.5
PID|1||AAA111||TEST^PATIENT^ONE||19900101|M||W|100 TEST ST^APT 1^CITY^ST^11111||(555)111-1111
PV1|1|I|ROOM^101^A|EL||||||MED||||1|V|||||||||||||||||||||||20220301140000

MSH|^~\&|SYS2|FAC2|REC|REC_FAC|20220301140000||ORU^R01|22222|P|2.5
PID|1||BBB222||ANOTHER^PATIENT^TWO||19950202|F||B|200 TEST AVE^UNIT 2^TOWN^ST^22222||(555)222-2222
OBR|1|ORDER123|LAB456|CBC^COMPLETE BLOOD COUNT|R|20220301140000|||||||||||||||20220301140000||F
OBX|1|NM|WBC||8.5|10^9/L|4.0-11.0|N|||F|||20220301140000

MSH|^~\&|SYS1|FAC1|REC|REC_FAC|20220301140000||ADT^A01|11111|P|2.5
PID|1||AAA111||TEST^PATIENT^ONE||19900101|M||W|100 TEST ST^APT 1^CITY^ST^11111||(555)111-1111
PV1|1|I|ROOM^101^A|EL||||||MED||||1|V|||||||||||||||||||||||20220301140000`

	// Create idempotency manager
	idempotencyManager := etl.NewIdempotencyManager(
		"/tmp/hl7_enhanced_idempotency.json",
		2*time.Hour,
		5000,
	)
	defer idempotencyManager.Stop()

	reader := strings.NewReader(hl7Data)
	source := ioadapter.NewSource(reader, "hl7")

	destConfig := config.DataConfig{
		Type:   "stdout",
		Format: "json",
	}

	// Create pipeline with enhanced HL7 transformer that uses idempotency manager directly
	pipeline := etl.NewETL(
		"hl7-enhanced-001",
		"Enhanced HL7 Pipeline with Direct Idempotency",
		etl.WithSources(source),
		etl.WithDestination(destConfig, nil, config.TableMapping{}),
		etl.WithBatchSize(5),
		etl.WithWorkerCount(1),
		etl.WithTransformers(&EnhancedHL7Transformer{IdempotencyMgr: idempotencyManager}),
		etl.WithMappers(&EnhancedHL7Mapper{}),
	)

	// Run the pipeline
	if err := pipeline.Run(ctx); err != nil {
		log.Fatalf("Enhanced HL7 pipeline failed: %v", err)
	}

	// Show comprehensive idempotency statistics
	stats := idempotencyManager.GetStats()
	fmt.Println("\n=== Enhanced HL7 Pipeline Idempotency Statistics ===")
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	// Show detailed key information
	fmt.Println("\n=== Detailed Idempotency Key Information ===")
	sampleKeys := []string{"msh_11111", "pid_AAA111", "msh_22222", "pid_BBB222"}
	for _, key := range sampleKeys {
		if info, exists := idempotencyManager.GetKeyInfo(key); exists {
			fmt.Printf("Key: %s\n", info.Key)
			fmt.Printf("  Status: %s, Count: %d\n", info.Status, info.Count)
			fmt.Printf("  First: %s, Last: %s\n", 
				info.FirstSeen.Format("15:04:05.000"), 
				info.LastSeen.Format("15:04:05.000"))
			fmt.Println()
		}
	}
}

// RegexLogTransformer processes regex-parsed log data with idempotency
type RegexLogTransformer struct {
	IdempotencyMgr *etl.IdempotencyManager
}

func (t *RegexLogTransformer) Name() string {
	return "RegexLogTransformer"
}

func (t *RegexLogTransformer) Transform(ctx context.Context, record utils.Record) (utils.Record, error) {
	// Generate idempotency key from IP and message content
	ip := getFieldAsString(record, "ip")
	msg := getFieldAsString(record, "msg")
	
	// Create idempotency key
	idempotencyKey := fmt.Sprintf("log_%s_%s", ip, hashString(msg))
	record["idempotency_key"] = idempotencyKey
	
	// Check if this record was already processed
	processed, err := t.IdempotencyMgr.IsProcessed(idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("idempotency check failed: %w", err)
	}
	
	if processed {
		// Skip processing this record
		record["skipped"] = true
		record["skip_reason"] = "duplicate_detected"
		return record, nil
	}
	
	// Mark as processing
	if err := t.IdempotencyMgr.MarkProcessing(idempotencyKey); err != nil {
		log.Printf("Failed to mark as processing: %v", err)
	}
	
	// Process the record
	record["log_type"] = classifyLogMessage(msg)
	record["ip_class"] = classifyIP(ip)
	record["processed_at"] = time.Now().Format(time.RFC3339)
	record["skipped"] = false
	
	// Mark as completed
	if err := t.IdempotencyMgr.MarkCompleted(idempotencyKey); err != nil {
		log.Printf("Failed to mark as completed: %v", err)
	}
	
	return record, nil
}

// EnhancedHL7Transformer processes HL7 data with direct idempotency management
type EnhancedHL7Transformer struct {
	IdempotencyMgr *etl.IdempotencyManager
}

func (t *EnhancedHL7Transformer) Name() string {
	return "EnhancedHL7Transformer"
}

func (t *EnhancedHL7Transformer) Transform(ctx context.Context, record utils.Record) (utils.Record, error) {
	segment := getFieldAsString(record, "segment")
	
	// Generate idempotency key based on segment type
	var idempotencyKey string
	switch segment {
	case "MSH":
		controlID := getFieldAsString(record, "field9")
		idempotencyKey = fmt.Sprintf("msh_%s", controlID)
		record["message_type"] = "header"
		record["control_id"] = controlID
	case "PID":
		patientID := getFieldAsString(record, "field3")
		idempotencyKey = fmt.Sprintf("pid_%s", patientID)
		record["message_type"] = "patient"
		record["patient_id"] = patientID
	case "PV1":
		location := getFieldAsString(record, "field3")
		timestamp := getFieldAsString(record, "field44")
		idempotencyKey = fmt.Sprintf("pv1_%s_%s", location, timestamp)
		record["message_type"] = "visit"
	case "OBR":
		orderID := getFieldAsString(record, "field3")
		idempotencyKey = fmt.Sprintf("obr_%s", orderID)
		record["message_type"] = "order"
	case "OBX":
		obsSeq := getFieldAsString(record, "field1")
		obsType := getFieldAsString(record, "field4")
		idempotencyKey = fmt.Sprintf("obx_%s_%s", obsSeq, obsType)
		record["message_type"] = "observation"
	default:
		idempotencyKey = fmt.Sprintf("unknown_%s_%s", segment, getFieldAsString(record, "field1"))
		record["message_type"] = "unknown"
	}
	
	record["idempotency_key"] = idempotencyKey
	
	// Check if already processed
	processed, err := t.IdempotencyMgr.IsProcessed(idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("idempotency check failed: %w", err)
	}
	
	if processed {
		record["skipped"] = true
		record["skip_reason"] = "already_processed"
		return record, nil
	}
	
	// Mark as processing
	if err := t.IdempotencyMgr.MarkProcessing(idempotencyKey); err != nil {
		log.Printf("Failed to mark as processing: %v", err)
	}
	
	// Process the record
	record["processed_at"] = time.Now().Format(time.RFC3339)
	record["is_valid"] = validateHL7Segment(segment, record)
	record["skipped"] = false
	
	// Mark as completed
	if err := t.IdempotencyMgr.MarkCompleted(idempotencyKey); err != nil {
		log.Printf("Failed to mark as completed: %v", err)
	}
	
	return record, nil
}

// EnhancedHL7Mapper provides enhanced mapping for HL7 patient data
type EnhancedHL7Mapper struct{}

func (m *EnhancedHL7Mapper) Name() string {
	return "EnhancedHL7Mapper"
}

func (m *EnhancedHL7Mapper) Map(ctx context.Context, record utils.Record) (utils.Record, error) {
	messageType := getFieldAsString(record, "message_type")
	
	// Only process patient records that weren't skipped
	if messageType != "patient" || record["skipped"] == true {
		return record, nil
	}
	
	mapped := make(utils.Record)
	
	// Copy all original fields
	for k, v := range record {
		mapped[k] = v
	}
	
	// Enhanced patient name parsing
	if nameField := getFieldAsString(record, "field5"); nameField != "" {
		nameParts := strings.Split(nameField, "^")
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
	
	// Enhanced date of birth processing
	if dobField := getFieldAsString(record, "field7"); len(dobField) == 8 {
		if parsed, err := time.Parse("20060102", dobField); err == nil {
			mapped["patient_dob"] = parsed.Format("2006-01-02")
			mapped["patient_dob_formatted"] = parsed.Format("January 2, 2006")
			
			// Calculate precise age
			now := time.Now()
			age := now.Year() - parsed.Year()
			if now.YearDay() < parsed.YearDay() {
				age--
			}
			mapped["patient_age"] = age
			mapped["patient_age_group"] = getAgeGroup(age)
		}
	}
	
	// Enhanced gender mapping
	if genderField := getFieldAsString(record, "field8"); genderField != "" {
		switch strings.ToUpper(genderField) {
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
	
	return mapped, nil
}

// Helper functions
func getFieldAsString(record utils.Record, fieldName string) string {
	if val, exists := record[fieldName]; exists {
		return fmt.Sprintf("%v", val)
	}
	return ""
}

func hashString(s string) string {
	// Simple hash for demonstration - in production use crypto/sha256
	hash := 0
	for _, char := range s {
		hash = hash*31 + int(char)
	}
	return fmt.Sprintf("%x", hash)
}

func classifyLogMessage(msg string) string {
	msg = strings.ToLower(msg)
	if strings.Contains(msg, "login") {
		return "authentication"
	} else if strings.Contains(msg, "failed") || strings.Contains(msg, "error") {
		return "error"
	} else if strings.Contains(msg, "database") || strings.Contains(msg, "query") {
		return "database"
	}
	return "general"
}

func classifyIP(ip string) string {
	if strings.HasPrefix(ip, "127.") {
		return "localhost"
	} else if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "10.") {
		return "private"
	}
	return "public"
}

func validateHL7Segment(segment string, record utils.Record) bool {
	switch segment {
	case "MSH":
		return record["field8"] != nil && record["field9"] != nil
	case "PID":
		return record["field3"] != nil && record["field5"] != nil
	case "PV1", "OBR", "OBX":
		return record["field1"] != nil
	default:
		return true
	}
}

func getAgeGroup(age int) string {
	switch {
	case age < 18:
		return "minor"
	case age < 65:
		return "adult"
	default:
		return "senior"
	}
}
