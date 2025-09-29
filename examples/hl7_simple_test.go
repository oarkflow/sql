package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/adapters/ioadapter"
	"github.com/oarkflow/sql/pkg/utils"
)

// Simple test for HL7 parsing and idempotency
func main() {
	fmt.Println("=== Testing HL7 Parsing with ioadapter ===")

	// Sample HL7 messages
	hl7Data := `MSH|^~\&|SEND_APP|SEND_FAC|REC_APP|REC_FAC|20220301120000||ADT^A01|12345|P|2.5
EVN||200310010800|||^KOWALSKI^ROBERT^^^^B|200310010800
PID|1||123456789||DOE^JOHN^MIDDLE||19850215|M||W|123 MAIN ST^APT 1^CITY^ST^12345||(555)123-4567||(555)123-9999||S||12345|||||||||||||||||
PV1|1|I|ICU^201^A|EL||||||SUR||||19|V|||||||||||||||||||||||||200310010800

MSH|^~\&|LAB_SYS|LAB|REC_APP|REC_FAC|20220301130000||ORU^R01|67890|P|2.5
PID|1||987654321||SMITH^JANE^||19900412|F||B|456 OAK AVE^UNIT 2^TOWN^ST^67890||(555)987-6543||||||||||||||||||||||`

	// Test HL7 parsing with ioadapter
	reader := strings.NewReader(hl7Data)
	source := ioadapter.NewSource(reader, "hl7")

	// Extract records
	records, err := source.Extract(nil)
	if err != nil {
		fmt.Printf("Error extracting records: %v\n", err)
		return
	}

	fmt.Println("\n=== Parsed HL7 Records ===")
	count := 0
	for record := range records {
		count++
		fmt.Printf("Record %d:\n", count)
		for k, v := range record {
			fmt.Printf("  %s: %v\n", k, v)
		}
		fmt.Println()
	}

	// Test idempotency manager
	fmt.Println("=== Testing Idempotency Manager ===")

	idempotencyMgr := etl.NewIdempotencyManager(
		"/tmp/test_idempotency.json",
		1*time.Hour,
		1000,
	)
	defer idempotencyMgr.Stop()

	// Test record with idempotency key
	testRecord := utils.Record{
		"segment": "PID",
		"field3":  "TEST123",
		"field5":  "TEST^PATIENT^",
	}

	// Generate idempotency key
	key := idempotencyMgr.GenerateKey(testRecord, []string{"segment", "field3"})
	fmt.Printf("Generated idempotency key: %s\n", key)

	// Check if processed (should be false)
	processed, _ := idempotencyMgr.IsProcessed(key)
	fmt.Printf("Is processed (first check): %v\n", processed)

	// Mark as processing
	idempotencyMgr.MarkProcessing(key)

	// Mark as completed
	idempotencyMgr.MarkCompleted(key)

	// Check if processed (should be true now)
	processed, _ = idempotencyMgr.IsProcessed(key)
	fmt.Printf("Is processed (after completion): %v\n", processed)

	// Get stats
	stats := idempotencyMgr.GetStats()
	fmt.Println("\n=== Idempotency Stats ===")
	for k, v := range stats {
		fmt.Printf("%s: %v\n", k, v)
	}
}
