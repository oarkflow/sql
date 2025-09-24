package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/sql/pkg/parsers"
)

func main() {
	parser := parsers.NewHL7Parser()

	// Sample ADT^A01 message
	adtMessage := `MSH|^~\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20230924120000||ADT^A01|123456|P|2.5|||AL|NE
EVN|A01|20230924120000
PID|1||123456^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|I|WARD1^BED1||||DRSMITH|||MED|||||ADM||SELF||||||20230924120000`

	fmt.Println("=== ADT^A01 Example ===")
	testMessage(parser, adtMessage, "ADT^A01")

	// Sample ORU^R01 message (Lab Results)
	oruMessage := `MSH|^~\&|LabSystem|Lab|ReceivingApp|ReceivingFac|20230924130000||ORU^R01|789012|P|2.5|||AL|NE
PID|1||123456^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|I|WARD1^BED1||||DRSMITH|||MED|||||ADM||SELF||||||20230924120000
ORC|RE|12345^LIS||56789^LIS|||||20230924130000|||DRSMITH||||LAB
OBR|1|12345^LIS||CBC^Complete Blood Count^L|||20230924130000|||||||||DRSMITH||||||20230924130000|||F
OBX|1|NM|WBC^White Blood Cell Count^L||7.5|10^9/L|4.0-11.0|N|||F|||20230924130000
OBX|2|NM|RBC^Red Blood Cell Count^L||4.8|10^12/L|4.2-5.4|N|||F|||20230924130000
OBX|3|NM|HGB^Hemoglobin^L||14.2|g/dL|12.0-16.0|N|||F|||20230924130000
OBX|4|NM|HCT^Hematocrit^L||42.0|%|36.0-46.0|N|||F|||20230924130000
NTE|1|L|Normal CBC results`

	fmt.Println("\n=== ORU^R01 Example ===")
	testMessage(parser, oruMessage, "ORU^R01")

	// Sample ORM^O01 message (Pharmacy Order)
	ormMessage := `MSH|^~\&|PharmSystem|Pharm|ReceivingApp|ReceivingFac|20230924140000||ORM^O01|345678|P|2.5|||AL|NE
PID|1||123456^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|I|WARD1^BED1||||DRSMITH|||MED|||||ADM||SELF||||||20230924120000
ORC|NW|98765^Pharm||87654^Pharm|||||20230924140000|||DRSMITH||||PHARM
RXE|1^tab^PO^QD^7^^^20230924140000|123^Acetaminophen^L||500^mg^tab|||||10^tab||||||||DRSMITH
RXR|PO^Oral^L
RXC|B|123^Acetaminophen^L|1^tab`

	fmt.Println("\n=== ORM^O01 Example ===")
	testMessage(parser, ormMessage, "ORM^O01")

	// Sample DFT^P03 message (Financial Transaction)
	dftMessage := `MSH|^~\&|BillingSystem|Hospital|ReceivingApp|ReceivingFac|20230924150000||DFT^P03|456789|P|2.5|||AL|NE
EVN|P03|20230924150000
PID|1||123456^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|I|WARD1^BED1||||DRSMITH|||MED|||||ADM||SELF||||||20230924120000
FT1|1|||20230924150000||CG|100.00|USD|Patient Payment|Credit|||||DRSMITH`

	fmt.Println("\n=== DFT^P03 Example ===")
	testMessage(parser, dftMessage, "DFT^P03")

	// Sample SIU^S12 message (New Appointment Booking)
	siuMessage := `MSH|^~\&|SchedulingSystem|Hospital|ReceivingApp|ReceivingFac|20230924160000||SIU^S12|567890|P|2.5|||AL|NE
SCH|12345|20230925100000^20230925110000|CONFIRMED|APPT^Appointment^L|||30|MINUTES
RGS|1|GENRES
AIG|1|DRSMITH^Smith^John^MD^L|ATTENDING
AIL|1|ROOM101^Main Building^L
AIS|1|CARDIO^Cardiology^L
PID|1||123456^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|O|CLINIC||||DRSMITH|||CARDIO|||||AMB||SELF||||||20230924160000`

	fmt.Println("\n=== SIU^S12 Example ===")
	testMessage(parser, siuMessage, "SIU^S12")
}

func testMessage(parser *parsers.HL7Parser, message, expectedType string) {
	// Parse as generic
	generic, err := parser.Parse(message)
	if err != nil {
		log.Fatalf("Error parsing message: %v", err)
	}

	fmt.Println("Generic parse successful")
	fmt.Printf("Segments: %v\n", generic)

	// Parse as typed
	typed, err := parser.ParseTyped(message)
	if err != nil {
		log.Fatalf("Error parsing typed message: %v", err)
	}

	fmt.Println("Typed parse successful")
	fmt.Printf("Message Type: %s\n", typed.GetMessageType())
	fmt.Printf("Control ID: %s\n", typed.GetMessageControlID())

	switch expectedType {
	case "ADT^A01":
		if adt, ok := typed.(*parsers.ADT_A01); ok {
			fmt.Printf("PID: %+v\n", adt.PID)
		}
	case "ORU^R01":
		if oru, ok := typed.(*parsers.ORU_R01); ok {
			fmt.Printf("PID: %+v\n", oru.PID)
			if len(oru.OBX) > 0 {
				fmt.Printf("First OBX: %+v\n", oru.OBX[0])
			}
		}
	case "ORM^O01":
		if orm, ok := typed.(*parsers.ORM_O01); ok {
			fmt.Printf("PID: %+v\n", orm.PID)
			if orm.RXE != nil {
				fmt.Printf("RXE: %+v\n", orm.RXE)
			}
		}
	case "DFT^P03":
		if dft, ok := typed.(*parsers.DFT_P03); ok {
			fmt.Printf("PID: %+v\n", dft.PID)
			if len(dft.FT1) > 0 {
				fmt.Printf("First FT1: %+v\n", dft.FT1[0])
			}
		}
	case "SIU^S12":
		if siu, ok := typed.(*parsers.SIU_S12); ok {
			fmt.Printf("PID: %+v\n", siu.PID)
			if siu.SCH != nil {
				fmt.Printf("SCH: %+v\n", siu.SCH)
			}
		}
	}
}
