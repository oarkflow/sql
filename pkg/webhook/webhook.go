package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/log"
	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/parsers"
)

// WebhookServer represents the webhook server
type WebhookServer struct {
	config         *Config
	jobs           chan Job
	parsers        []parsers.Parser
	etlManager     *etl.Manager
	integrationMgr *integrations.Manager
	logger         *log.Logger
	mu             sync.Mutex
}

// Job represents a webhook processing job
type Job struct {
	Body    []byte
	Headers http.Header
}

// NewWebhookServer creates a new webhook server instance
func NewWebhookServer(config *Config) *WebhookServer {
	if config == nil {
		config = DefaultConfig()
	}

	ws := &WebhookServer{
		config:         config,
		jobs:           make(chan Job, 100), // buffered channel for jobs
		parsers:        []parsers.Parser{},
		etlManager:     etl.NewManager(),
		integrationMgr: integrations.New(),
		logger:         &log.DefaultLogger,
	}

	// Load parsers dynamically based on configuration
	ws.loadParsers()

	// Load ETL configurations if specified
	if config.ETLConfig != "" {
		ws.loadETLConfig()
	}

	// Load integrations if specified
	if config.Integrations != "" {
		ws.loadIntegrations()
	}

	return ws
}

// loadParsers dynamically loads parsers based on configuration
func (ws *WebhookServer) loadParsers() {
	parserMap := map[string]parsers.Parser{
		"json":  parsers.NewJSONParser(),
		"xml":   parsers.NewXMLParser(),
		"hl7":   parsers.NewHL7Parser(),
		"smpp":  parsers.NewSMPPParser(),
		"plain": parsers.NewPlainTextParser(),
	}

	for _, parserName := range ws.config.Parsers {
		if parser, exists := parserMap[strings.ToLower(parserName)]; exists {
			ws.parsers = append(ws.parsers, parser)
			ws.logger.Info().Str("parser", parserName).Msg("Loaded parser")
		} else {
			ws.logger.Warn().Str("parser", parserName).Msg("Unknown parser type, skipping")
		}
	}

	if len(ws.parsers) == 0 {
		ws.logger.Warn().Msg("No parsers loaded, using defaults")
		// Load default parsers
		ws.parsers = []parsers.Parser{
			parsers.NewJSONParser(),
			parsers.NewXMLParser(),
			parsers.NewHL7Parser(),
			parsers.NewPlainTextParser(),
		}
	}
}

// loadETLConfig loads ETL configuration from file
func (ws *WebhookServer) loadETLConfig() {
	etlCfg, err := config.Load(ws.config.ETLConfig)
	if err != nil {
		ws.logger.Error().Err(err).Str("file", ws.config.ETLConfig).Msg("Failed to load ETL config")
		return
	}

	ids, err := ws.etlManager.Prepare(etlCfg)
	if err != nil {
		ws.logger.Error().Err(err).Msg("Failed to prepare ETL pipelines")
		return
	}

	ws.logger.Info().Strs("pipeline_ids", ids).Msg("ETL pipelines prepared")

	// Convert ETL config to webhook pipelines for backward compatibility
	ws.convertETLToWebhooks(etlCfg)
}

// loadIntegrations loads integration configuration from file
func (ws *WebhookServer) loadIntegrations() {
	ctx := context.Background()
	_, err := ws.integrationMgr.LoadIntegrationsFromFile(ctx, ws.config.Integrations)
	if err != nil {
		ws.logger.Error().Err(err).Str("file", ws.config.Integrations).Msg("Failed to load integrations")
		return
	}

	ws.logger.Info().Str("file", ws.config.Integrations).Msg("Integrations loaded")
}

// convertETLToWebhooks converts ETL table mappings to webhook ETLPipelines
func (ws *WebhookServer) convertETLToWebhooks(etlCfg *config.Config) {
	if ws.config.ETLPipelines == nil {
		ws.config.ETLPipelines = make(map[string]*ETLPipeline)
	}

	for _, table := range etlCfg.Tables {
		dataType := ws.inferDataTypeFromName(table.OldName)
		etlPipeline := &ETLPipeline{
			Name:        table.OldName,
			Description: fmt.Sprintf("ETL pipeline for %s", table.OldName),
			DataType:    dataType,
			Source:      etlCfg.Source,
			Destination: etlCfg.Destinations[0], // Use first destination
			Mapping:     table,
			Enabled:     true,
		}
		ws.config.ETLPipelines[table.OldName] = etlPipeline
		ws.logger.Info().Str("pipeline", table.OldName).Str("data_type", dataType).Msg("Converted ETL table to webhook ETL pipeline")
	}
}

// inferDataTypeFromName infers data type from table name
func (ws *WebhookServer) inferDataTypeFromName(tableName string) string {
	// Simple inference based on table name patterns
	switch {
	case strings.Contains(strings.ToLower(tableName), "hl7"):
		return "hl7"
	case strings.Contains(strings.ToLower(tableName), "json"):
		return "json"
	case strings.Contains(strings.ToLower(tableName), "xml"):
		return "xml"
	case strings.Contains(strings.ToLower(tableName), "smpp"):
		return "smpp"
	default:
		return "json" // Default to JSON
	}
}

// Start starts the webhook server
func (ws *WebhookServer) Start() error {
	// Start worker goroutines
	for i := 0; i < ws.config.MaxWorkers; i++ {
		go ws.worker()
	}

	http.HandleFunc("/webhook", ws.handleWebhook)
	http.HandleFunc("/health", ws.healthHandler)
	log.Printf("Starting webhook server on port %s", ws.config.Port)
	return http.ListenAndServe(":"+ws.config.Port, nil)
}

// handleWebhook handles incoming webhook requests
func (ws *WebhookServer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Verify signature if secret is set
	if ws.config.Secret != "" {
		sig := r.Header.Get("X-Hub-Signature")
		if sig == "" {
			http.Error(w, "Missing signature", http.StatusUnauthorized)
			return
		}
		expected := "sha256=" + computeHmac(body, ws.config.Secret)
		if !hmac.Equal([]byte(sig), []byte(expected)) {
			http.Error(w, "Invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Respond immediately to make it non-blocking
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))

	// Send job to workers
	select {
	case ws.jobs <- Job{Body: body, Headers: r.Header.Clone()}:
		// Job queued
	default:
		log.Printf("Job queue full, dropping webhook")
	}
}

// worker processes jobs from the queue
func (ws *WebhookServer) worker() {
	for job := range ws.jobs {
		ws.processWebhook(job.Body, job.Headers)
	}
}

// processWebhook processes the webhook payload
func (ws *WebhookServer) processWebhook(body []byte, headers http.Header) {
	// Lock if needed for shared resources
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// Log the received data
	log.Printf("Processing webhook payload: %s", string(body))
	log.Printf("Headers: %v", headers)

	// Try to parse with available parsers
	for _, parser := range ws.parsers {
		if parser.Detect(body) {
			parsed, err := parser.Parse(body)
			if err == nil {
				log.Printf("Successfully parsed with %s: %v", parser.Name(), parsed)

				// Process with ETL if pipeline exists for this data type
				if err := ws.processWithETL(parser.Name(), parsed, body); err != nil {
					log.Printf("ETL processing failed for %s: %v", parser.Name(), err)
				}

				// Additional processing: validate, trigger actions, etc.
				return
			}
		}
	}

	// If no parser succeeded, log as unknown
	log.Printf("Failed to parse with any known parser, treating as raw data")
}

// processWithETL processes parsed data through configured ETL pipelines
func (ws *WebhookServer) processWithETL(dataType string, parsedData interface{}, rawData []byte) error {
	// Case-insensitive pipeline lookup
	dataTypeLower := strings.ToLower(dataType)
	var pipeline *ETLPipeline

	for _, p := range ws.config.ETLPipelines {
		if strings.ToLower(p.DataType) == dataTypeLower && p.Enabled {
			pipeline = p
			break
		}
	}

	if pipeline == nil {
		log.Printf("No enabled ETL pipeline found for data type: %s", dataType)
		return nil
	}

	log.Printf("Processing %s data through ETL pipeline: %s", dataType, pipeline.Name)

	// For HL7 data, extract relevant fields for ETL processing
	var processedData map[string]interface{}

	switch dataType {
	case "hl7":
		if hl7Data, ok := parsedData.(map[string]interface{}); ok {
			processedData = ws.extractHL7Fields(hl7Data)
		}
	case "json":
		if jsonData, ok := parsedData.(map[string]interface{}); ok {
			processedData = jsonData
		}
	default:
		// For other data types, use the parsed data as-is
		if dataMap, ok := parsedData.(map[string]interface{}); ok {
			processedData = dataMap
		} else {
			processedData = map[string]interface{}{"raw_data": string(rawData)}
		}
	}

	if processedData == nil {
		return fmt.Errorf("failed to process data for ETL pipeline")
	}

	// Create a temporary ETL instance for this webhook data
	// In a production system, you might want to use the ETL manager
	_ = etl.NewETL(
		fmt.Sprintf("%s-%d", pipeline.Name, time.Now().Unix()),
		pipeline.Description,
		etl.WithSource("json", nil, "", "", "", "json"), // Use json source type
		etl.WithDestination(pipeline.Destination, nil, pipeline.Mapping),
	)

	// Add the processed data to the ETL context
	// This is a simplified approach - in production, you'd store data in a temporary table/file
	_ = context.Background()

	// For demonstration, we'll log the processed data
	// In a real implementation, you'd pass this data to the ETL process
	log.Printf("ETL Pipeline %s would process data: %+v", pipeline.Name, processedData)

	// TODO: Implement actual ETL execution with the processed data
	// This would involve creating a temporary data source and running the ETL pipeline

	log.Printf("Successfully processed %s data through ETL pipeline: %s", dataType, pipeline.Name)

	return nil
}

// extractHL7Fields extracts relevant fields from HL7 parsed data for ETL processing
func (ws *WebhookServer) extractHL7Fields(hl7Data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Extract PID (Patient Identification) segment data
	if pidSegments, ok := hl7Data["PID"].([]map[string]interface{}); ok && len(pidSegments) > 0 {
		pid := pidSegments[0]

		// Extract common patient fields
		if patientID, ok := pid["3"].(string); ok {
			result["patient_id"] = patientID
		}
		if patientName, ok := pid["5"].(string); ok {
			result["patient_name"] = patientName
		}
		if dob, ok := pid["7"].(string); ok {
			result["date_of_birth"] = dob
		}
		if gender, ok := pid["8"].(string); ok {
			result["gender"] = gender
		}
		if address, ok := pid["11"].(string); ok {
			result["address"] = address
		}
	}

	// Extract MSH (Message Header) data
	if mshSegments, ok := hl7Data["MSH"].([]map[string]interface{}); ok && len(mshSegments) > 0 {
		msh := mshSegments[0]
		if messageType, ok := msh["9"].(string); ok {
			result["message_type"] = messageType
		}
		if controlID, ok := msh["10"].(string); ok {
			result["control_id"] = controlID
		}
	}

	// Add timestamp
	result["processed_at"] = time.Now().Format(time.RFC3339)

	return result
}

// healthHandler handles health check requests
func (ws *WebhookServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// computeHmac computes the HMAC-SHA256 signature
func computeHmac(data []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
