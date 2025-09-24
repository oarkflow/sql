package main

import (
	"encoding/hex"
	"fmt"

	"github.com/oarkflow/sql/pkg/parsers"
)

func main() {
	// Create the same SMPP PDU as in the test script
	systemID := []byte("test\x00")     // 5 bytes
	password := []byte("password\x00") // 9 bytes
	systemType := []byte("sms\x00")    // 4 bytes
	interfaceVersion := byte(0x34)     // 1 byte
	addrTon := byte(0)                 // 1 byte
	addrNpi := byte(0)                 // 1 byte
	addressRange := []byte("\x00")     // 1 byte

	body := append(systemID, password...)
	body = append(body, systemType...)
	body = append(body, interfaceVersion, addrTon, addrNpi)
	body = append(body, addressRange...)

	commandLength := uint32(16 + len(body)) // 16 header + body
	commandID := uint32(0x00000009)         // bind_transceiver
	commandStatus := uint32(0)
	sequenceNumber := uint32(1)

	fmt.Printf("Command length: %d\n", commandLength)
	fmt.Printf("Body length: %d\n", len(body))
	fmt.Printf("Total PDU length: %d\n", 16+len(body))
	fmt.Printf("Hex dump: %s\n", hex.EncodeToString(body))

	// Create the full PDU
	pdu := make([]byte, 16+len(body))
	// Header
	pdu[0] = byte(commandLength >> 24)
	pdu[1] = byte(commandLength >> 16)
	pdu[2] = byte(commandLength >> 8)
	pdu[3] = byte(commandLength)
	pdu[4] = byte(commandID >> 24)
	pdu[5] = byte(commandID >> 16)
	pdu[6] = byte(commandID >> 8)
	pdu[7] = byte(commandID)
	pdu[8] = byte(commandStatus >> 24)
	pdu[9] = byte(commandStatus >> 16)
	pdu[10] = byte(commandStatus >> 8)
	pdu[11] = byte(commandStatus)
	pdu[12] = byte(sequenceNumber >> 24)
	pdu[13] = byte(sequenceNumber >> 16)
	pdu[14] = byte(sequenceNumber >> 8)
	pdu[15] = byte(sequenceNumber)
	// Body
	copy(pdu[16:], body)

	fmt.Printf("Full PDU hex: %s\n", hex.EncodeToString(pdu))
	fmt.Printf("PDU length: %d\n", len(pdu))

	// Test the parser
	smmpParser := parsers.NewSMPPParser()
	detected := smmpParser.Detect(pdu)
	fmt.Printf("SMPP detected: %t\n", detected)

	if detected {
		result, err := smmpParser.ParseString(pdu)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
		} else {
			fmt.Printf("Parsed result: %+v\n", result)
		}
	}
}
