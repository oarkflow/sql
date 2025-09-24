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
	"github.com/oarkflow/squealx"
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

	// Use the parsed data directly - the parser already provides structured data
	var processedData map[string]interface{}

	switch dataType {
	case "hl7":
		// Flatten HL7 structured data for ETL mapping
		if hl7Data, ok := parsedData.(map[string]interface{}); ok {
			processedData = ws.flattenHL7Data(hl7Data)
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

	// Apply ETL field mapping and insert into destination database
	if err := ws.executeETLMapping(pipeline, processedData); err != nil {
		log.Printf("ETL mapping execution failed: %v", err)
		return err
	}

	log.Printf("Successfully processed %s data through ETL pipeline: %s", dataType, pipeline.Name)

	return nil
}

// executeETLMapping applies field mapping and inserts data into destination database
func (ws *WebhookServer) executeETLMapping(pipeline *ETLPipeline, data map[string]interface{}) error {
	// Apply field mapping from ETL configuration
	mappedData := make(map[string]interface{})

	for sourceField, destField := range pipeline.Mapping.Mapping {
		if value, exists := data[sourceField]; exists {
			// Convert complex types to strings for database insertion
			mappedData[destField] = ws.convertForDatabase(value)
		}
	}

	// Add any extra values from configuration
	for key, value := range pipeline.Mapping.ExtraValues {
		mappedData[key] = ws.convertForDatabase(value)
	}

	// Add timestamp if not present
	if _, exists := mappedData["created_at"]; !exists {
		mappedData["created_at"] = time.Now()
	}
	if _, exists := mappedData["updated_at"]; !exists {
		mappedData["updated_at"] = time.Now()
	}

	log.Printf("Mapped data for insertion: %+v", mappedData)

	// Insert into destination database
	return ws.insertIntoDatabase(pipeline.Destination, pipeline.Mapping.NewName, mappedData, pipeline.Mapping)
}

// insertIntoDatabase inserts mapped data into the destination database
func (ws *WebhookServer) insertIntoDatabase(dest config.DataConfig, tableName string, data map[string]interface{}, mapping config.TableMapping) error {
	// Open database connection
	db, err := config.OpenDB(dest)
	if err != nil {
		return fmt.Errorf("failed to connect to destination database: %w", err)
	}
	defer db.Close()

	// Auto-create table if configured
	if mapping.AutoCreateTable {
		if err := ws.createTableIfNotExists(db, tableName, data, mapping); err != nil {
			log.Printf("Warning: failed to create table %s: %v", tableName, err)
		}
	}

	// Build INSERT query
	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for col, val := range data {
		columns = append(columns, fmt.Sprintf(`"%s"`, col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`,
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("Executing query: %s with values: %v", query, values)

	_, err = db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	log.Printf("Successfully inserted data into table %s", tableName)
	return nil
}

// createTableIfNotExists creates the destination table if it doesn't exist
func (ws *WebhookServer) createTableIfNotExists(db *squealx.DB, tableName string, data map[string]interface{}, mapping config.TableMapping) error {
	// Build CREATE TABLE query based on data types
	columns := make([]string, 0, len(data))

	for col, val := range data {
		sqlType := ws.inferSQLType(val, mapping.NormalizeSchema[col])
		// Use proper column quoting for the database type
		columns = append(columns, fmt.Sprintf(`"%s" %s`, col, sqlType))
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		tableName,
		strings.Join(columns, ", "))

	log.Printf("Creating table with query: %s", query)

	_, err := db.Exec(query)
	return err
}

// convertForDatabase converts complex data types to database-compatible types
func (ws *WebhookServer) convertForDatabase(value interface{}) interface{} {
	switch v := value.(type) {
	case []interface{}:
		// Convert array to comma-separated string
		var parts []string
		for _, item := range v {
			if item != nil {
				parts = append(parts, fmt.Sprintf("%v", item))
			}
		}
		return strings.Join(parts, " ")
	case map[string]interface{}:
		// Convert map to JSON-like string
		return fmt.Sprintf("%v", v)
	case nil:
		return nil
	default:
		return v
	}
}

// inferSQLType infers SQL data type from Go value and optional schema hint
func (ws *WebhookServer) inferSQLType(value interface{}, schemaHint string) string {
	if schemaHint != "" {
		switch strings.ToLower(schemaHint) {
		case "string", "text":
			return "TEXT"
		case "int", "integer":
			return "INTEGER"
		case "bool", "boolean":
			return "BOOLEAN"
		case "date", "datetime":
			return "TIMESTAMP"
		case "float", "decimal":
			return "REAL"
		}
	}

	switch value.(type) {
	case int, int32, int64:
		return "INTEGER"
	case float32, float64:
		return "REAL"
	case bool:
		return "BOOLEAN"
	case string:
		return "TEXT"
	case time.Time:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

// flattenHL7Data flattens HL7 structured data for ETL mapping compatibility
func (ws *WebhookServer) flattenHL7Data(hl7Data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for segmentName, segmentData := range hl7Data {
		if segmentArray, ok := segmentData.([]map[string]interface{}); ok && len(segmentArray) > 0 {
			// Use the first segment instance (most common case)
			segment := segmentArray[0]

			// Create flattened keys like "MSH.1", "MSH.2", "PID.3", etc.
			for fieldKey, fieldValue := range segment {
				flattenedKey := fmt.Sprintf("%s.%s", segmentName, fieldKey)
				result[flattenedKey] = fieldValue
			}
		}
	}

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
