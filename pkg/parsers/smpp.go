package parsers

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// SMPPParser parses SMPP PDUs
type SMPPParser struct{}

// NewSMPPParser creates a new SMPP parser
func NewSMPPParser() *SMPPParser {
	return &SMPPParser{}
}

// Name returns the parser name
func (p *SMPPParser) Name() string {
	return "smpp"
}

// Detect checks if the data is an SMPP PDU
func (p *SMPPParser) Detect(data []byte) bool {
	if len(data) < 16 { // Minimum SMPP PDU length
		return false
	}

	commandLength := binary.BigEndian.Uint32(data[0:4])

	// Validate command length
	if commandLength < 16 || commandLength > 0x7FFFFFFF {
		return false
	}

	// Check if declared length matches actual data length
	if int(commandLength) != len(data) {
		return false
	}

	// Validate command ID (should be a known SMPP command)
	commandID := binary.BigEndian.Uint32(data[4:8])
	return p.isValidCommandID(commandID)
}

// isValidCommandID checks if the command ID is a known SMPP command
func (p *SMPPParser) isValidCommandID(commandID uint32) bool {
	validCommands := map[uint32]bool{
		0x00000001: true, // bind_receiver
		0x00000002: true, // bind_transmitter
		0x00000003: true, // query_sm
		0x00000004: true, // submit_sm
		0x00000005: true, // deliver_sm
		0x00000006: true, // unbind
		0x00000007: true, // replace_sm
		0x00000008: true, // cancel_sm
		0x00000009: true, // bind_transceiver
		0x0000000B: true, // outbind
		0x00000015: true, // enquire_link
		0x00000021: true, // submit_multi
		0x00000102: true, // alert_notification
		0x80000001: true, // bind_receiver_resp
		0x80000002: true, // bind_transmitter_resp
		0x80000003: true, // query_sm_resp
		0x80000004: true, // submit_sm_resp
		0x80000005: true, // deliver_sm_resp
		0x80000006: true, // unbind_resp
		0x80000007: true, // replace_sm_resp
		0x80000008: true, // cancel_sm_resp
		0x80000009: true, // bind_transceiver_resp
		0x80000015: true, // enquire_link_resp
		0x80000021: true, // submit_multi_resp
	}

	return validCommands[commandID]
}

// Parse parses the SMPP PDU into a map
func (p *SMPPParser) Parse(data []byte) (any, error) {
	return p.ParseString(data)
}

// ParseString parses SMPP PDU data into a structured map
func (p *SMPPParser) ParseString(data []byte) (map[string]any, error) {
	if len(data) < 16 {
		return nil, errors.New("data too short for SMPP PDU")
	}

	result := make(map[string]any)

	// Parse header (all PDUs have this)
	commandLength := binary.BigEndian.Uint32(data[0:4])
	commandID := binary.BigEndian.Uint32(data[4:8])
	commandStatus := binary.BigEndian.Uint32(data[8:12])
	sequenceNumber := binary.BigEndian.Uint32(data[12:16])

	// Validate header
	if commandLength < 16 || commandLength > 0x7FFFFFFF {
		return nil, fmt.Errorf("invalid command length: %d", commandLength)
	}

	if int(commandLength) != len(data) {
		return nil, fmt.Errorf("command length mismatch: expected %d, got %d", commandLength, len(data))
	}

	result["command_length"] = commandLength
	result["command_id"] = commandID
	result["command_status"] = commandStatus
	result["sequence_number"] = sequenceNumber

	// Add human-readable command name
	result["command_name"] = p.getCommandName(commandID)

	// Add human-readable status name
	result["status_name"] = p.getStatusName(commandStatus)

	// Parse body based on command type
	body := data[16:]
	if len(body) > 0 {
		bodyData, err := p.parseCommandBody(commandID, body)
		if err != nil {
			result["body_parse_error"] = err.Error()
			// Still include raw body for debugging
			result["raw_body_hex"] = fmt.Sprintf("%X", body)
		} else {
			// Merge body data into result
			for k, v := range bodyData {
				result[k] = v
			}
		}
	}

	return result, nil
}

// getCommandName returns human-readable name for command ID
func (p *SMPPParser) getCommandName(commandID uint32) string {
	switch commandID {
	case 0x00000001:
		return "bind_receiver"
	case 0x00000002:
		return "bind_transmitter"
	case 0x00000003:
		return "query_sm"
	case 0x00000004:
		return "submit_sm"
	case 0x00000005:
		return "deliver_sm"
	case 0x00000006:
		return "unbind"
	case 0x00000007:
		return "replace_sm"
	case 0x00000008:
		return "cancel_sm"
	case 0x00000009:
		return "bind_transceiver"
	case 0x0000000B:
		return "outbind"
	case 0x00000015:
		return "enquire_link"
	case 0x00000021:
		return "submit_multi"
	case 0x00000102:
		return "alert_notification"
	case 0x80000001:
		return "bind_receiver_resp"
	case 0x80000002:
		return "bind_transmitter_resp"
	case 0x80000003:
		return "query_sm_resp"
	case 0x80000004:
		return "submit_sm_resp"
	case 0x80000005:
		return "deliver_sm_resp"
	case 0x80000006:
		return "unbind_resp"
	case 0x80000007:
		return "replace_sm_resp"
	case 0x80000008:
		return "cancel_sm_resp"
	case 0x80000009:
		return "bind_transceiver_resp"
	case 0x80000015:
		return "enquire_link_resp"
	case 0x80000021:
		return "submit_multi_resp"
	default:
		return fmt.Sprintf("unknown_0x%08X", commandID)
	}
}

// getStatusName returns human-readable name for status code
func (p *SMPPParser) getStatusName(status uint32) string {
	switch status {
	case 0x00000000:
		return "ESME_ROK"
	case 0x00000001:
		return "ESME_RINVMSGLEN"
	case 0x00000002:
		return "ESME_RINVCMDLEN"
	case 0x00000003:
		return "ESME_RINVCMDID"
	case 0x00000004:
		return "ESME_RINVBNDSTS"
	case 0x00000005:
		return "ESME_RALYBND"
	case 0x00000006:
		return "ESME_RINVPRTFLG"
	case 0x00000007:
		return "ESME_RINVREGDLVFLG"
	case 0x00000008:
		return "ESME_RSYSERR"
	case 0x0000000A:
		return "ESME_RINVSRCADR"
	case 0x0000000B:
		return "ESME_RINVDSTADR"
	case 0x0000000C:
		return "ESME_RINVMSGID"
	case 0x0000000D:
		return "ESME_RBINDFAIL"
	case 0x0000000E:
		return "ESME_RINVPASWD"
	case 0x0000000F:
		return "ESME_RINVSYSID"
	case 0x00000011:
		return "ESME_RCANCELFAIL"
	case 0x00000013:
		return "ESME_RREPLACEFAIL"
	case 0x00000014:
		return "ESME_RMSGQFUL"
	case 0x00000015:
		return "ESME_RINVSERTYP"
	case 0x00000033:
		return "ESME_RINVNUMDESTS"
	case 0x00000034:
		return "ESME_RINVDLNAME"
	case 0x00000040:
		return "ESME_RINVDESTFLAG"
	case 0x00000042:
		return "ESME_RINVSUBREP"
	case 0x00000043:
		return "ESME_RINVESMCLASS"
	case 0x00000044:
		return "ESME_RCNTSUBDL"
	case 0x00000045:
		return "ESME_RSUBMITFAIL"
	case 0x00000048:
		return "ESME_RINVSRCTON"
	case 0x00000049:
		return "ESME_RINVSRCNPI"
	case 0x00000050:
		return "ESME_RINVDSTTON"
	case 0x00000051:
		return "ESME_RINVDSTNPI"
	case 0x00000053:
		return "ESME_RINVSYSTYP"
	case 0x00000054:
		return "ESME_RINVREPFLAG"
	case 0x00000055:
		return "ESME_RINVNUMMSGS"
	case 0x00000058:
		return "ESME_RTHROTTLED"
	case 0x00000061:
		return "ESME_RINVSCHED"
	case 0x00000062:
		return "ESME_RINVEXPIRY"
	case 0x00000063:
		return "ESME_RINVDFTMSGID"
	case 0x00000064:
		return "ESME_RX_T_APPN"
	case 0x00000065:
		return "ESME_RX_P_APPN"
	case 0x00000066:
		return "ESME_RX_R_APPN"
	case 0x00000067:
		return "ESME_RQUERYFAIL"
	case 0x000000C0:
		return "ESME_RINVOPTPARSTREAM"
	case 0x000000C1:
		return "ESME_ROPTPARNOTALLWD"
	case 0x000000C2:
		return "ESME_RINVPARLEN"
	case 0x000000C3:
		return "ESME_RMISSINGOPTPARAM"
	case 0x000000C4:
		return "ESME_RINVOPTPARAMVAL"
	case 0x000000FE:
		return "ESME_RDELIVERYFAILURE"
	case 0x000000FF:
		return "ESME_RUNKNOWNERR"
	default:
		return fmt.Sprintf("unknown_0x%08X", status)
	}
}

// parseCommandBody parses the PDU body based on command type
func (p *SMPPParser) parseCommandBody(commandID uint32, body []byte) (map[string]any, error) {
	result := make(map[string]any)

	switch commandID {
	case 0x00000001, 0x00000002, 0x00000009: // bind_receiver, bind_transmitter, bind_transceiver
		return p.parseBindBody(body)
	case 0x00000004: // submit_sm
		return p.parseSubmitSmBody(body)
	case 0x00000005: // deliver_sm
		return p.parseDeliverSmBody(body)
	case 0x00000015: // enquire_link
		return map[string]any{}, nil // No body
	case 0x80000001, 0x80000002, 0x80000009: // bind responses
		return p.parseBindRespBody(body)
	case 0x80000004: // submit_sm_resp
		return p.parseSubmitSmRespBody(body)
	case 0x80000005: // deliver_sm_resp
		return p.parseDeliverSmRespBody(body)
	case 0x80000015: // enquire_link_resp
		return map[string]any{}, nil // No body
	default:
		// For unknown commands, store raw body
		result["raw_body"] = body
		result["raw_body_hex"] = fmt.Sprintf("%X", body)
	}

	return result, nil
}

// parseBindBody parses bind request body
func (p *SMPPParser) parseBindBody(body []byte) (map[string]any, error) {
	result := make(map[string]any)

	if len(body) < 7 { // Minimum: system_id(1) + password(1) + system_type(1) + interface_version(1) + addr_ton(1) + addr_npi(1) + address_range(1)
		return nil, errors.New("bind body too short")
	}

	offset := 0

	// system_id (COctet String)
	systemID, newOffset := p.readCOctetString(body, offset)
	result["system_id"] = systemID
	offset = newOffset

	// password (COctet String)
	password, newOffset := p.readCOctetString(body, offset)
	result["password"] = password
	offset = newOffset

	// system_type (COctet String)
	systemType, newOffset := p.readCOctetString(body, offset)
	result["system_type"] = systemType
	offset = newOffset

	// interface_version (1 byte)
	if offset < len(body) {
		result["interface_version"] = body[offset]
		offset++
	}

	// addr_ton (1 byte)
	if offset < len(body) {
		result["addr_ton"] = body[offset]
		offset++
	}

	// addr_npi (1 byte)
	if offset < len(body) {
		result["addr_npi"] = body[offset]
		offset++
	}

	// address_range (COctet String)
	if offset < len(body) {
		addressRange, _ := p.readCOctetString(body, offset)
		result["address_range"] = addressRange
	}

	return result, nil
}

// parseSubmitSmBody parses submit_sm body
func (p *SMPPParser) parseSubmitSmBody(body []byte) (map[string]any, error) {
	result := make(map[string]any)

	offset := 0

	// service_type
	serviceType, newOffset := p.readCOctetString(body, offset)
	result["service_type"] = serviceType
	offset = newOffset

	// source_addr_ton (1 byte)
	if offset < len(body) {
		result["source_addr_ton"] = body[offset]
		offset++
	}

	// source_addr_npi (1 byte)
	if offset < len(body) {
		result["source_addr_npi"] = body[offset]
		offset++
	}

	// source_addr (COctet String)
	sourceAddr, newOffset := p.readCOctetString(body, offset)
	result["source_addr"] = sourceAddr
	offset = newOffset

	// dest_addr_ton (1 byte)
	if offset < len(body) {
		result["dest_addr_ton"] = body[offset]
		offset++
	}

	// dest_addr_npi (1 byte)
	if offset < len(body) {
		result["dest_addr_npi"] = body[offset]
		offset++
	}

	// destination_addr (COctet String)
	destAddr, newOffset := p.readCOctetString(body, offset)
	result["destination_addr"] = destAddr
	offset = newOffset

	// esm_class (1 byte)
	if offset < len(body) {
		result["esm_class"] = body[offset]
		offset++
	}

	// protocol_id (1 byte)
	if offset < len(body) {
		result["protocol_id"] = body[offset]
		offset++
	}

	// priority_flag (1 byte)
	if offset < len(body) {
		result["priority_flag"] = body[offset]
		offset++
	}

	// schedule_delivery_time (COctet String)
	scheduleDeliveryTime, newOffset := p.readCOctetString(body, offset)
	result["schedule_delivery_time"] = scheduleDeliveryTime
	offset = newOffset

	// validity_period (COctet String)
	validityPeriod, newOffset := p.readCOctetString(body, offset)
	result["validity_period"] = validityPeriod
	offset = newOffset

	// registered_delivery (1 byte)
	if offset < len(body) {
		result["registered_delivery"] = body[offset]
		offset++
	}

	// replace_if_present_flag (1 byte)
	if offset < len(body) {
		result["replace_if_present_flag"] = body[offset]
		offset++
	}

	// data_coding (1 byte)
	if offset < len(body) {
		result["data_coding"] = body[offset]
		offset++
	}

	// sm_default_msg_id (1 byte)
	if offset < len(body) {
		result["sm_default_msg_id"] = body[offset]
		offset++
	}

	// sm_length (1 byte)
	if offset < len(body) {
		smLength := body[offset]
		result["sm_length"] = smLength
		offset++

		// short_message (sm_length bytes)
		if offset+int(smLength) <= len(body) {
			result["short_message"] = string(body[offset : offset+int(smLength)])
			offset += int(smLength)
		}
	}

	return result, nil
}

// parseDeliverSmBody parses deliver_sm body (similar to submit_sm)
func (p *SMPPParser) parseDeliverSmBody(body []byte) (map[string]any, error) {
	// deliver_sm has the same format as submit_sm
	return p.parseSubmitSmBody(body)
}

// parseBindRespBody parses bind response body
func (p *SMPPParser) parseBindRespBody(body []byte) (map[string]any, error) {
	result := make(map[string]any)

	if len(body) == 0 {
		return result, nil
	}

	// system_id (COctet String)
	systemID, _ := p.readCOctetString(body, 0)
	result["system_id"] = systemID

	return result, nil
}

// parseSubmitSmRespBody parses submit_sm response body
func (p *SMPPParser) parseSubmitSmRespBody(body []byte) (map[string]any, error) {
	result := make(map[string]any)

	if len(body) == 0 {
		return result, nil
	}

	// message_id (COctet String)
	messageID, _ := p.readCOctetString(body, 0)
	result["message_id"] = messageID

	return result, nil
}

// parseDeliverSmRespBody parses deliver_sm response body
func (p *SMPPParser) parseDeliverSmRespBody(body []byte) (map[string]any, error) {
	// Same as submit_sm_resp
	return p.parseSubmitSmRespBody(body)
}

// readCOctetString reads a COctet String (null-terminated string)
func (p *SMPPParser) readCOctetString(data []byte, offset int) (string, int) {
	if offset >= len(data) {
		return "", offset
	}

	// Find null terminator
	end := offset
	for end < len(data) && data[end] != 0 {
		end++
	}

	if end >= len(data) {
		// No null terminator found, take rest of data
		return string(data[offset:]), len(data)
	}

	// Include null terminator in offset calculation
	return string(data[offset:end]), end + 1
}
