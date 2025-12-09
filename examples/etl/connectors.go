package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// HL7 MLLP (Minimum Lower Layer Protocol) constants
const (
	START_BLOCK     byte = 0x0B // Vertical Tab
	END_BLOCK       byte = 0x1C // File Separator
	CARRIAGE_RETURN byte = 0x0D // Carriage Return
)

// HL7MLLPSource implements MLLP listener for HL7 messages
type HL7MLLPSource struct {
	host     string
	port     int
	listener net.Listener
	msgChan  chan Message
}

func NewHL7MLLPSource() *HL7MLLPSource {
	return &HL7MLLPSource{}
}

func (s *HL7MLLPSource) Initialize(config map[string]interface{}) error {
	host, ok := config["host"].(string)
	if !ok {
		host = "0.0.0.0"
	}
	s.host = host

	port, ok := config["port"].(float64)
	if !ok {
		port = 2575
	}
	s.port = int(port)

	s.msgChan = make(chan Message, 100)
	return nil
}

func (s *HL7MLLPSource) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start MLLP listener: %w", err)
	}
	s.listener = listener

	log.Printf("HL7 MLLP listener started on %s", addr)

	go s.acceptConnections(ctx)
	return nil
}

func (s *HL7MLLPSource) acceptConnections(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				log.Printf("Accept error: %v", err)
				continue
			}
			go s.handleConnection(ctx, conn)
		}
	}
}

func (s *HL7MLLPSource) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	log.Printf("New connection from %s", conn.RemoteAddr())
	reader := bufio.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Read start block
			startByte, err := reader.ReadByte()
			if err != nil {
				if err != io.EOF {
					log.Printf("Read error: %v", err)
				}
				return
			}

			if startByte != START_BLOCK {
				log.Printf("Invalid start block: %02x", startByte)
				continue
			}

			// Read until end block
			var msgData []byte
			for {
				b, err := reader.ReadByte()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				if b == END_BLOCK {
					// Read carriage return
					cr, _ := reader.ReadByte()
					if cr != CARRIAGE_RETURN {
						log.Printf("Expected CR after EB, got %02x", cr)
					}
					break
				}
				msgData = append(msgData, b)
			}

			// Create message
			msg := Message{
				ID:        uuid.New().String(),
				Timestamp: time.Now(),
				Data:      string(msgData),
				Metadata: map[string]interface{}{
					"source_ip":  conn.RemoteAddr().String(),
					"protocol":   "HL7_MLLP",
					"raw_length": len(msgData),
				},
			}

			// Send acknowledgment
			ack := s.generateACK(string(msgData))
			ackBytes := []byte{START_BLOCK}
			ackBytes = append(ackBytes, []byte(ack)...)
			ackBytes = append(ackBytes, END_BLOCK, CARRIAGE_RETURN)

			if _, err := conn.Write(ackBytes); err != nil {
				log.Printf("Failed to send ACK: %v", err)
			}

			// Send to channel
			select {
			case s.msgChan <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *HL7MLLPSource) generateACK(hl7Message string) string {
	// Parse message to extract MSH fields
	lines := strings.Split(hl7Message, "\r")
	if len(lines) == 0 {
		return ""
	}

	msh := lines[0]
	fields := strings.Split(msh, "|")

	if len(fields) < 10 {
		return ""
	}

	// Extract relevant fields
	fieldSep := "|"
	encodingChars := fields[1]
	sendingApp := fields[2]
	sendingFacility := fields[3]
	receivingApp := fields[4]
	receivingFacility := fields[5]
	messageControlID := fields[9]

	timestamp := time.Now().Format("20060102150405")

	// Build ACK message
	ack := fmt.Sprintf("MSH%s%s%s%s%s%s%s%s%s%s%s%s%sACK%s%s%sAL%s2.5",
		fieldSep, encodingChars, fieldSep,
		receivingApp, fieldSep, receivingFacility, fieldSep,
		sendingApp, fieldSep, sendingFacility, fieldSep,
		timestamp, fieldSep, messageControlID, fieldSep, fieldSep, fieldSep)

	ack += "\rMSA|AA|" + messageControlID

	return ack
}

func (s *HL7MLLPSource) Read(ctx context.Context) (<-chan Message, error) {
	return s.msgChan, nil
}

func (s *HL7MLLPSource) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *HL7MLLPSource) GetType() string {
	return "hl7_mllp"
}

// HL7ToJSONTransformer converts HL7 messages to JSON
type HL7ToJSONTransformer struct {
	prettyPrint bool
}

func NewHL7ToJSONTransformer() *HL7ToJSONTransformer {
	return &HL7ToJSONTransformer{}
}

func (t *HL7ToJSONTransformer) Initialize(config map[string]interface{}) error {
	if pretty, ok := config["pretty_print"].(bool); ok {
		t.prettyPrint = pretty
	}
	return nil
}

func (t *HL7ToJSONTransformer) Transform(ctx context.Context, msg Message) (Message, error) {
	hl7Data, ok := msg.Data.(string)
	if !ok {
		return msg, fmt.Errorf("expected string data, got %T", msg.Data)
	}

	jsonData, err := t.parseHL7ToJSON(hl7Data)
	if err != nil {
		return msg, fmt.Errorf("failed to parse HL7: %w", err)
	}

	msg.Data = jsonData
	msg.Metadata["transformation"] = "hl7_to_json"
	msg.Metadata["transformer_timestamp"] = time.Now()

	return msg, nil
}

func (t *HL7ToJSONTransformer) parseHL7ToJSON(hl7Message string) (map[string]interface{}, error) {
	// Clean up the message
	hl7Message = strings.ReplaceAll(hl7Message, "\r\n", "\r")
	hl7Message = strings.ReplaceAll(hl7Message, "\n", "\r")

	lines := strings.Split(hl7Message, "\r")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty HL7 message")
	}

	// Parse MSH first to get encoding characters
	mshLine := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "MSH") {
			mshLine = line
			break
		}
	}

	if mshLine == "" {
		return nil, fmt.Errorf("MSH segment not found")
	}

	// Extract encoding characters from MSH-2
	if len(mshLine) < 9 {
		return nil, fmt.Errorf("invalid MSH segment")
	}

	encodingChars := mshLine[4:8] // Usually ^~\&
	fieldSep := string(mshLine[3])
	componentSep := string(encodingChars[0])
	repetitionSep := string(encodingChars[1])
	escapeChar := string(encodingChars[2])
	subComponentSep := string(encodingChars[3])

	result := make(map[string]interface{})
	result["encoding"] = map[string]string{
		"field_separator":        fieldSep,
		"component_separator":    componentSep,
		"repetition_separator":   repetitionSep,
		"escape_character":       escapeChar,
		"subcomponent_separator": subComponentSep,
	}

	segments := make([]map[string]interface{}, 0)
	segmentsByType := make(map[string][]map[string]interface{})

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		segment := t.parseSegment(line, fieldSep, componentSep, repetitionSep, subComponentSep)
		if segment != nil {
			segments = append(segments, segment)

			segType := segment["segment_type"].(string)
			segmentsByType[segType] = append(segmentsByType[segType], segment)
		}
	}

	// Add segments to result
	for segType, segs := range segmentsByType {
		if len(segs) == 1 {
			result[segType] = segs[0]
		} else {
			result[segType] = segs
		}
	}

	result["_segments_ordered"] = segments
	result["_raw"] = hl7Message

	return result, nil
}

func (t *HL7ToJSONTransformer) parseSegment(line, fieldSep, componentSep, repetitionSep, subComponentSep string) map[string]interface{} {
	if line == "" {
		return nil
	}

	fields := strings.Split(line, fieldSep)
	if len(fields) == 0 {
		return nil
	}

	segmentType := fields[0]
	segment := make(map[string]interface{})
	segment["segment_type"] = segmentType

	// MSH is special - field separator is part of the segment ID
	startIdx := 1
	parsedFields := make(map[string]interface{})

	if segmentType == "MSH" {
		// MSH-1 is the field separator
		parsedFields["1"] = fieldSep
		// MSH-2 is the encoding characters
		if len(fields) > 1 {
			parsedFields["2"] = fields[1]
		}
		startIdx = 2
	}

	// Parse remaining fields
	fieldNum := startIdx
	for i := startIdx; i < len(fields); i++ {
		fieldNum++
		field := fields[i]

		if field == "" {
			parsedFields[fmt.Sprintf("%d", fieldNum)] = ""
			continue
		}

		// Parse field with repetitions
		parsedFields[fmt.Sprintf("%d", fieldNum)] = t.parseField(field, componentSep, repetitionSep, subComponentSep)
	}

	segment["fields"] = parsedFields

	// Add human-readable field names for common segments
	t.addSegmentMetadata(segment, segmentType, parsedFields)

	return segment
}

func (t *HL7ToJSONTransformer) parseField(field, componentSep, repetitionSep, subComponentSep string) interface{} {
	// Check for repetitions first
	if strings.Contains(field, repetitionSep) {
		repetitions := strings.Split(field, repetitionSep)
		parsed := make([]interface{}, len(repetitions))
		for i, rep := range repetitions {
			parsed[i] = t.parseFieldValue(rep, componentSep, subComponentSep)
		}
		return parsed
	}

	return t.parseFieldValue(field, componentSep, subComponentSep)
}

func (t *HL7ToJSONTransformer) parseFieldValue(value, componentSep, subComponentSep string) interface{} {
	// Check for components
	if strings.Contains(value, componentSep) {
		components := strings.Split(value, componentSep)
		parsed := make([]interface{}, len(components))
		for i, comp := range components {
			// Check for subcomponents
			if strings.Contains(comp, subComponentSep) {
				subcomponents := strings.Split(comp, subComponentSep)
				parsed[i] = subcomponents
			} else {
				parsed[i] = comp
			}
		}
		return parsed
	}

	// Check for subcomponents
	if strings.Contains(value, subComponentSep) {
		return strings.Split(value, subComponentSep)
	}

	return value
}

func (t *HL7ToJSONTransformer) addSegmentMetadata(segment map[string]interface{}, segmentType string, fields map[string]interface{}) {
	getField := func(idx string) string {
		if val, ok := fields[idx]; ok {
			switch v := val.(type) {
			case string:
				return v
			case []interface{}:
				if len(v) > 0 {
					if s, ok := v[0].(string); ok {
						return s
					}
				}
			}
		}
		return ""
	}

	getFieldArray := func(idx string) []interface{} {
		if val, ok := fields[idx]; ok {
			if arr, ok := val.([]interface{}); ok {
				return arr
			}
		}
		return nil
	}

	switch segmentType {
	case "MSH":
		segment["message_header"] = map[string]interface{}{
			"sending_application":   getField("3"),
			"sending_facility":      getField("4"),
			"receiving_application": getField("5"),
			"receiving_facility":    getField("6"),
			"datetime":              getField("7"),
			"security":              getField("8"),
			"message_type":          getField("9"),
			"message_control_id":    getField("10"),
			"processing_id":         getField("11"),
			"version_id":            getField("12"),
		}

	case "PID":
		patientName := getFieldArray("5")
		segment["patient"] = map[string]interface{}{
			"set_id":              getField("1"),
			"external_id":         getField("2"),
			"internal_id":         getField("3"),
			"alternate_id":        getField("4"),
			"patient_name":        patientName,
			"mother_maiden_name":  getField("6"),
			"date_of_birth":       getField("7"),
			"sex":                 getField("8"),
			"patient_alias":       getField("9"),
			"race":                getField("10"),
			"patient_address":     getFieldArray("11"),
			"country_code":        getField("12"),
			"phone_home":          getField("13"),
			"phone_business":      getField("14"),
			"primary_language":    getField("15"),
			"marital_status":      getField("16"),
			"religion":            getField("17"),
			"patient_account_num": getField("18"),
			"ssn":                 getField("19"),
		}

	case "PV1":
		segment["patient_visit"] = map[string]interface{}{
			"set_id":                    getField("1"),
			"patient_class":             getField("2"),
			"assigned_patient_location": getField("3"),
			"admission_type":            getField("4"),
			"preadmit_number":           getField("5"),
			"prior_patient_location":    getField("6"),
			"attending_doctor":          getFieldArray("7"),
			"referring_doctor":          getFieldArray("8"),
			"consulting_doctor":         getFieldArray("9"),
			"hospital_service":          getField("10"),
			"admit_datetime":            getField("44"),
			"discharge_datetime":        getField("45"),
		}

	case "OBR":
		segment["observation_request"] = map[string]interface{}{
			"set_id":                   getField("1"),
			"placer_order_number":      getField("2"),
			"filler_order_number":      getField("3"),
			"universal_service_id":     getFieldArray("4"),
			"priority":                 getField("5"),
			"requested_datetime":       getField("6"),
			"observation_datetime":     getField("7"),
			"observation_end_datetime": getField("8"),
			"collection_volume":        getField("9"),
			"collector_identifier":     getFieldArray("10"),
			"specimen_action_code":     getField("11"),
			"ordering_provider":        getFieldArray("16"),
			"results_status":           getField("25"),
		}

	case "OBX":
		segment["observation_result"] = map[string]interface{}{
			"set_id":               getField("1"),
			"value_type":           getField("2"),
			"observation_id":       getFieldArray("3"),
			"observation_sub_id":   getField("4"),
			"observation_value":    fields["5"],
			"units":                getField("6"),
			"reference_range":      getField("7"),
			"abnormal_flags":       getField("8"),
			"probability":          getField("9"),
			"nature_of_abnormal":   getField("10"),
			"observation_status":   getField("11"),
			"observation_datetime": getField("14"),
		}

	case "NK1":
		segment["next_of_kin"] = map[string]interface{}{
			"set_id":         getField("1"),
			"name":           getFieldArray("2"),
			"relationship":   getField("3"),
			"address":        getFieldArray("4"),
			"phone_number":   getField("5"),
			"business_phone": getField("6"),
			"contact_role":   getField("7"),
			"start_date":     getField("8"),
			"end_date":       getField("9"),
		}

	case "AL1":
		segment["allergy"] = map[string]interface{}{
			"set_id":              getField("1"),
			"allergen_type":       getField("2"),
			"allergen_code":       getFieldArray("3"),
			"allergy_severity":    getField("4"),
			"allergy_reaction":    getField("5"),
			"identification_date": getField("6"),
		}

	case "DG1":
		segment["diagnosis"] = map[string]interface{}{
			"set_id":             getField("1"),
			"diagnosis_coding":   getField("2"),
			"diagnosis_code":     getFieldArray("3"),
			"diagnosis_desc":     getField("4"),
			"diagnosis_datetime": getField("5"),
			"diagnosis_type":     getField("6"),
		}

	case "EVN":
		segment["event_type"] = map[string]interface{}{
			"event_type_code":   getField("1"),
			"recorded_datetime": getField("2"),
			"datetime_planned":  getField("3"),
			"event_reason_code": getField("4"),
			"operator_id":       getFieldArray("5"),
			"event_occurred":    getField("6"),
		}

	case "IN1":
		segment["insurance"] = map[string]interface{}{
			"set_id":                  getField("1"),
			"insurance_plan_id":       getField("2"),
			"insurance_company_id":    getField("3"),
			"insurance_company_name":  getField("4"),
			"insurance_company_addr":  getFieldArray("5"),
			"insurance_company_phone": getField("7"),
			"group_number":            getField("8"),
			"group_name":              getField("9"),
			"insured_name":            getFieldArray("16"),
			"insured_relationship":    getField("17"),
			"policy_number":           getField("36"),
		}
	}
}

func (t *HL7ToJSONTransformer) GetName() string {
	return "hl7_to_json"
}

// JSONFileDestination writes messages to JSON files
type JSONFileDestination struct {
	directory   string
	prettyPrint bool
}

func NewJSONFileDestination() *JSONFileDestination {
	return &JSONFileDestination{}
}

func (d *JSONFileDestination) Initialize(config map[string]interface{}) error {
	dir, ok := config["directory"].(string)
	if !ok {
		dir = "./output"
	}
	d.directory = dir

	if pretty, ok := config["pretty_print"].(bool); ok {
		d.prettyPrint = pretty
	} else {
		d.prettyPrint = true
	}

	return nil
}

func (d *JSONFileDestination) Start(ctx context.Context) error {
	return nil
}

func (d *JSONFileDestination) Write(ctx context.Context, msg Message) error {
	var data []byte
	var err error

	if d.prettyPrint {
		data, err = json.MarshalIndent(msg, "", "  ")
	} else {
		data, err = json.Marshal(msg)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	filename := fmt.Sprintf("%s/%s.json", d.directory, msg.ID)
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	log.Printf("Written message to %s", filename)
	return nil
}

func (d *JSONFileDestination) Stop() error {
	return nil
}

func (d *JSONFileDestination) GetType() string {
	return "json_file"
}

// ConsoleDestination writes messages to console
type ConsoleDestination struct {
	prettyPrint bool
}

func NewConsoleDestination() *ConsoleDestination {
	return &ConsoleDestination{prettyPrint: true}
}

func (c *ConsoleDestination) Initialize(config map[string]interface{}) error {
	if pretty, ok := config["pretty_print"].(bool); ok {
		c.prettyPrint = pretty
	}
	return nil
}

func (c *ConsoleDestination) Start(ctx context.Context) error {
	return nil
}

func (c *ConsoleDestination) Write(ctx context.Context, msg Message) error {
	var data []byte
	var err error

	if c.prettyPrint {
		data, err = json.MarshalIndent(msg.Data, "", "  ")
	} else {
		data, err = json.Marshal(msg.Data)
	}

	if err != nil {
		return err
	}

	fmt.Println("==================== MESSAGE ====================")
	fmt.Printf("ID: %s\n", msg.ID)
	fmt.Printf("Timestamp: %s\n", msg.Timestamp.Format(time.RFC3339))
	fmt.Println("Data:")
	fmt.Println(string(data))
	fmt.Println("=================================================")

	return nil
}

func (c *ConsoleDestination) Stop() error {
	return nil
}

func (c *ConsoleDestination) GetType() string {
	return "console"
}

// =============================================================================
// HTTP Source Connector - Polls HTTP endpoint
// =============================================================================

type HTTPSource struct {
	url          string
	method       string
	headers      map[string]string
	pollInterval time.Duration
	msgChan      chan Message
	stopChan     chan struct{}
}

func NewHTTPSource() *HTTPSource {
	return &HTTPSource{
		method:   "GET",
		headers:  make(map[string]string),
		stopChan: make(chan struct{}),
	}
}

func (s *HTTPSource) Initialize(config map[string]interface{}) error {
	url, ok := config["url"].(string)
	if !ok {
		return fmt.Errorf("url is required")
	}
	s.url = url

	if method, ok := config["method"].(string); ok {
		s.method = method
	}

	if headers, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			if str, ok := v.(string); ok {
				s.headers[k] = str
			}
		}
	}

	interval := 60
	if i, ok := config["poll_interval_seconds"].(float64); ok {
		interval = int(i)
	}
	s.pollInterval = time.Duration(interval) * time.Second

	s.msgChan = make(chan Message, 10)
	return nil
}

func (s *HTTPSource) Start(ctx context.Context) error {
	go s.poll(ctx)
	return nil
}

func (s *HTTPSource) poll(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			if err := s.fetchAndSend(ctx); err != nil {
				log.Printf("HTTP fetch error: %v", err)
			}
		}
	}
}

func (s *HTTPSource) fetchAndSend(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, s.method, s.url, nil)
	if err != nil {
		return err
	}

	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	msg := Message{
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data:      string(body),
		Metadata: map[string]interface{}{
			"source":      "http",
			"url":         s.url,
			"status_code": resp.StatusCode,
		},
	}

	select {
	case s.msgChan <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *HTTPSource) Read(ctx context.Context) (<-chan Message, error) {
	return s.msgChan, nil
}

func (s *HTTPSource) Stop() error {
	close(s.stopChan)
	return nil
}

func (s *HTTPSource) GetType() string {
	return "http"
}

// =============================================================================
// File Source Connector - Watches directory for files
// =============================================================================

type FileSource struct {
	directory    string
	pattern      string
	deleteAfter  bool
	pollInterval time.Duration
	msgChan      chan Message
	processed    map[string]bool
	stopChan     chan struct{}
}

func NewFileSource() *FileSource {
	return &FileSource{
		processed: make(map[string]bool),
		stopChan:  make(chan struct{}),
	}
}

func (s *FileSource) Initialize(config map[string]interface{}) error {
	dir, ok := config["directory"].(string)
	if !ok {
		return fmt.Errorf("directory is required")
	}
	s.directory = dir

	if pattern, ok := config["pattern"].(string); ok {
		s.pattern = pattern
	} else {
		s.pattern = "*"
	}

	if del, ok := config["delete_after_read"].(bool); ok {
		s.deleteAfter = del
	}

	interval := 5
	if i, ok := config["poll_interval_seconds"].(float64); ok {
		interval = int(i)
	}
	s.pollInterval = time.Duration(interval) * time.Second

	s.msgChan = make(chan Message, 100)
	return nil
}

func (s *FileSource) Start(ctx context.Context) error {
	go s.watch(ctx)
	return nil
}

func (s *FileSource) watch(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.scanDirectory(ctx)
		}
	}
}

func (s *FileSource) scanDirectory(ctx context.Context) {
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		log.Printf("Error reading directory: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		filepath := fmt.Sprintf("%s/%s", s.directory, filename)

		if s.processed[filepath] {
			continue
		}

		// Read file
		data, err := os.ReadFile(filepath)
		if err != nil {
			log.Printf("Error reading file %s: %v", filepath, err)
			continue
		}

		msg := Message{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
			Data:      string(data),
			Metadata: map[string]interface{}{
				"source":   "file",
				"filename": filename,
				"filepath": filepath,
				"size":     len(data),
			},
		}

		select {
		case s.msgChan <- msg:
			s.processed[filepath] = true
			if s.deleteAfter {
				os.Remove(filepath)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *FileSource) Read(ctx context.Context) (<-chan Message, error) {
	return s.msgChan, nil
}

func (s *FileSource) Stop() error {
	close(s.stopChan)
	return nil
}

func (s *FileSource) GetType() string {
	return "file"
}

// =============================================================================
// HTTP Destination Connector - Posts to HTTP endpoint
// =============================================================================

type HTTPDestination struct {
	url     string
	method  string
	headers map[string]string
	timeout time.Duration
}

func NewHTTPDestination() *HTTPDestination {
	return &HTTPDestination{
		method:  "POST",
		headers: make(map[string]string),
		timeout: 30 * time.Second,
	}
}

func (d *HTTPDestination) Initialize(config map[string]interface{}) error {
	url, ok := config["url"].(string)
	if !ok {
		return fmt.Errorf("url is required")
	}
	d.url = url

	if method, ok := config["method"].(string); ok {
		d.method = method
	}

	if headers, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			if str, ok := v.(string); ok {
				d.headers[k] = str
			}
		}
	}

	if timeout, ok := config["timeout_seconds"].(float64); ok {
		d.timeout = time.Duration(timeout) * time.Second
	}

	return nil
}

func (d *HTTPDestination) Start(ctx context.Context) error {
	return nil
}

func (d *HTTPDestination) Write(ctx context.Context, msg Message) error {
	jsonData, err := json.Marshal(msg.Data)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, d.method, d.url, strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range d.headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: d.timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	log.Printf("Message sent to %s (status: %d)", d.url, resp.StatusCode)
	return nil
}

func (d *HTTPDestination) Stop() error {
	return nil
}

func (d *HTTPDestination) GetType() string {
	return "http"
}

// =============================================================================
// Database Destination Connector - Writes to SQL database
// =============================================================================

type DatabaseDestination struct {
	driver string
	dsn    string
	table  string
	db     *sql.DB
}

func NewDatabaseDestination() *DatabaseDestination {
	return &DatabaseDestination{}
}

func (d *DatabaseDestination) Initialize(config map[string]interface{}) error {
	driver, ok := config["driver"].(string)
	if !ok {
		return fmt.Errorf("driver is required")
	}
	d.driver = driver

	dsn, ok := config["dsn"].(string)
	if !ok {
		return fmt.Errorf("dsn is required")
	}
	d.dsn = dsn

	table, ok := config["table"].(string)
	if !ok {
		return fmt.Errorf("table is required")
	}
	d.table = table

	return nil
}

func (d *DatabaseDestination) Start(ctx context.Context) error {
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		return err
	}
	d.db = db
	return d.db.Ping()
}

func (d *DatabaseDestination) Write(ctx context.Context, msg Message) error {
	jsonData, err := json.Marshal(msg.Data)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("INSERT INTO %s (id, timestamp, data, metadata) VALUES (?, ?, ?, ?)", d.table)

	metadataJSON, _ := json.Marshal(msg.Metadata)

	_, err = d.db.ExecContext(ctx, query, msg.ID, msg.Timestamp, string(jsonData), string(metadataJSON))
	return err
}

func (d *DatabaseDestination) Stop() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *DatabaseDestination) GetType() string {
	return "database"
}

// =============================================================================
// Transformers
// =============================================================================

// JSONPathTransformer extracts data using JSON path
type JSONPathTransformer struct {
	path string
}

func NewJSONPathTransformer() *JSONPathTransformer {
	return &JSONPathTransformer{}
}

func (t *JSONPathTransformer) Initialize(config map[string]interface{}) error {
	path, ok := config["path"].(string)
	if !ok {
		return fmt.Errorf("path is required")
	}
	t.path = path
	return nil
}

func (t *JSONPathTransformer) Transform(ctx context.Context, msg Message) (Message, error) {
	// Simplified JSONPath implementation - extend with proper library
	// For now, just return the message
	msg.Metadata["jsonpath_applied"] = t.path
	return msg, nil
}

func (t *JSONPathTransformer) GetName() string {
	return "jsonpath"
}

// FilterTransformer filters messages based on conditions
type FilterTransformer struct {
	field    string
	operator string
	value    interface{}
}

func NewFilterTransformer() *FilterTransformer {
	return &FilterTransformer{}
}

func (t *FilterTransformer) Initialize(config map[string]interface{}) error {
	field, ok := config["field"].(string)
	if !ok {
		return fmt.Errorf("field is required")
	}
	t.field = field

	operator, ok := config["operator"].(string)
	if !ok {
		return fmt.Errorf("operator is required")
	}
	t.operator = operator

	t.value = config["value"]
	return nil
}

func (t *FilterTransformer) Transform(ctx context.Context, msg Message) (Message, error) {
	// Implement filtering logic based on operator
	// For now, pass through
	return msg, nil
}

func (t *FilterTransformer) GetName() string {
	return "filter"
}

// CSVToJSONTransformer converts CSV to JSON
type CSVToJSONTransformer struct {
	delimiter rune
	hasHeader bool
}

func NewCSVToJSONTransformer() *CSVToJSONTransformer {
	return &CSVToJSONTransformer{
		delimiter: ',',
		hasHeader: true,
	}
}

func (t *CSVToJSONTransformer) Initialize(config map[string]interface{}) error {
	if del, ok := config["delimiter"].(string); ok && len(del) > 0 {
		t.delimiter = rune(del[0])
	}
	if header, ok := config["has_header"].(bool); ok {
		t.hasHeader = header
	}
	return nil
}

func (t *CSVToJSONTransformer) Transform(ctx context.Context, msg Message) (Message, error) {
	csvData, ok := msg.Data.(string)
	if !ok {
		return msg, fmt.Errorf("expected string data")
	}

	reader := csv.NewReader(strings.NewReader(csvData))
	reader.Comma = t.delimiter

	records, err := reader.ReadAll()
	if err != nil {
		return msg, err
	}

	if len(records) == 0 {
		return msg, fmt.Errorf("no records found")
	}

	var headers []string
	startIdx := 0

	if t.hasHeader {
		headers = records[0]
		startIdx = 1
	} else {
		for i := 0; i < len(records[0]); i++ {
			headers = append(headers, fmt.Sprintf("column_%d", i))
		}
	}

	result := make([]map[string]string, 0)
	for i := startIdx; i < len(records); i++ {
		row := make(map[string]string)
		for j, value := range records[i] {
			if j < len(headers) {
				row[headers[j]] = value
			}
		}
		result = append(result, row)
	}

	msg.Data = result
	msg.Metadata["transformation"] = "csv_to_json"
	return msg, nil
}

func (t *CSVToJSONTransformer) GetName() string {
	return "csv_to_json"
}
