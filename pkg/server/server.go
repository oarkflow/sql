package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/oarkflow/sql"
	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/config"
)

type Config struct {
	Version     string
	StaticPath  string
	EnableMocks bool
}

type Server struct {
	app                *fiber.App
	etlManager         *etl.Manager
	integrationManager *integrations.Manager
	executions         []ExecutionSummary
	configurations     []StoredConfiguration
	config             Config
}

type StoredConfiguration struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	Content    string    `json:"content"`
	CreatedAt  time.Time `json:"createdAt"`
	LastUsed   time.Time `json:"lastUsed"`
	Executions int       `json:"executions"`
}

type ExecutionSummary struct {
	ID               string          `json:"id"`
	Config           string          `json:"config"`
	Status           string          `json:"status"`
	RecordsProcessed int             `json:"recordsProcessed"`
	StartTime        time.Time       `json:"startTime"`
	EndTime          *time.Time      `json:"endTime,omitempty"`
	Error            string          `json:"error,omitempty"`
	DetailedMetrics  DetailedMetrics `json:"detailedMetrics,omitempty"`
}

type DetailedMetrics struct {
	Extracted        int64            `json:"extracted"`
	Mapped           int64            `json:"mapped"`
	Transformed      int64            `json:"transformed"`
	Loaded           int64            `json:"loaded"`
	Errors           int64            `json:"errors"`
	WorkerActivities []WorkerActivity `json:"workerActivities"`
}

type WorkerActivity struct {
	Node      string    `json:"node"`
	WorkerID  int       `json:"worker_id"`
	Processed int64     `json:"processed"`
	Failed    int64     `json:"failed"`
	Timestamp time.Time `json:"timestamp"`
	Activity  string    `json:"activity"`
}

type QueryRequest struct {
	Query string `json:"query"`
}

type QueryResponse struct {
	Columns       []string        `json:"columns"`
	Rows          [][]interface{} `json:"rows"`
	RowCount      int             `json:"rowCount"`
	ExecutionTime float64         `json:"executionTime"`
}

type ValidationResponse struct {
	Valid       bool     `json:"valid"`
	Errors      []string `json:"errors"`
	Suggestions []string `json:"suggestions"`
}

type SchemaResponse struct {
	Tables  []string            `json:"tables"`
	Columns map[string][]string `json:"columns"`
}

type SavedQuery struct {
	ID        string `json:"id"`
	Query     string `json:"query"`
	Name      string `json:"name,omitempty"`
	Timestamp string `json:"timestamp"`
	Success   bool   `json:"success"`
}

type ExecuteConfigRequest struct {
	Config string `json:"config"`
	Type   string `json:"type"` // "bcl", "yaml", "json"
}

func NewServer(cfg Config) *Server {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	// Initialize managers
	etlManager := etl.NewManager()
	integrationManager := integrations.New()

	server := &Server{
		app:                app,
		etlManager:         etlManager,
		integrationManager: integrationManager,
		executions:         []ExecutionSummary{},
		configurations:     []StoredConfiguration{},
		config:             cfg,
	}

	server.setupRoutes()
	return server
}

func (s *Server) setupRoutes() {
	s.app.Use(cors.New())
	s.app.Use(logger.New())

	// Health check
	s.app.Get("/api/health", s.healthHandler)

	// Query endpoints
	s.app.Post("/api/query", s.executeQueryHandler)
	s.app.Post("/api/query/validate", s.validateQueryHandler)
	s.app.Get("/api/query/history", s.getQueryHistoryHandler)
	s.app.Post("/api/query/save", s.saveQueryHandler)

	// Schema endpoints
	s.app.Get("/api/schema/:integration", s.getSchemaHandler)

	// Integration endpoints
	s.app.Get("/api/integrations", s.getIntegrationsHandler)
	s.app.Get("/api/integrations/:id", s.getIntegrationHandler)
	s.app.Post("/api/integrations", s.createIntegrationHandler)
	s.app.Put("/api/integrations/:id", s.updateIntegrationHandler)
	s.app.Delete("/api/integrations/:id", s.deleteIntegrationHandler)

	// ETL Pipeline endpoints
	s.app.Get("/api/pipelines", s.getPipelinesHandler)
	s.app.Get("/api/pipelines/:id", s.getPipelineHandler)
	s.app.Post("/api/pipelines", s.createPipelineHandler)
	s.app.Put("/api/pipelines/:id", s.updatePipelineHandler)
	s.app.Delete("/api/pipelines/:id", s.deletePipelineHandler)
	s.app.Post("/api/pipelines/:id/run", s.runPipelineHandler)

	// ETL Run endpoints
	s.app.Get("/api/runs", s.getRunsHandler)
	s.app.Get("/api/runs/:id", s.getRunHandler)

	// Execute config endpoint
	s.app.Post("/api/execute", s.executeConfigHandler)
	s.app.Get("/api/executions", s.getExecutionsHandler)
	s.app.Get("/api/configurations", s.getConfigurationsHandler)

	// Serve execute page
	s.app.Get("/execute", func(c *fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/execute.html")
	})

	// Legacy ETL endpoints (for compatibility)
	s.app.Get("/config", s.getConfigHandler)
	s.app.Post("/config", s.createConfigHandler)
	s.app.Get("/etls", s.listETLsHandler)
	s.app.Get("/etls/:id/start", s.startETLHandler)
	s.app.Post("/etls/:id/stop", s.stopETLHandler)
	s.app.Get("/etls/:id", s.getETLDetailsHandler)
	s.app.Get("/etls/:id/metrics", s.getETLMetricsHandler)

	// Serve static files
	s.app.Static("/", s.config.StaticPath)
}

func (s *Server) healthHandler(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "healthy",
		"version":   s.config.Version,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (s *Server) executeQueryHandler(c *fiber.Ctx) error {
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if strings.TrimSpace(req.Query) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Query cannot be empty"})
	}

	start := time.Now()
	records, err := sql.Query(context.Background(), req.Query)
	executionTime := time.Since(start).Seconds()

	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error":         err.Error(),
			"executionTime": executionTime,
		})
	}

	// Convert records to the expected format
	var columns []string
	var rows [][]interface{}

	if len(records) > 0 {
		// Get column names from the first record
		for key := range records[0] {
			columns = append(columns, key)
		}

		// Convert records to rows
		for _, record := range records {
			var row []interface{}
			for _, col := range columns {
				row = append(row, record[col])
			}
			rows = append(rows, row)
		}
	}

	response := QueryResponse{
		Columns:       columns,
		Rows:          rows,
		RowCount:      len(rows),
		ExecutionTime: executionTime,
	}

	return c.JSON(response)
}

func (s *Server) validateQueryHandler(c *fiber.Ctx) error {
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Basic validation - try to parse the query
	lexer := sql.NewLexer(req.Query)
	parser := sql.NewParser(lexer)
	parser.ParseQueryStatement()

	var errors []string
	var suggestions []string

	if len(parser.Errors()) > 0 {
		errors = parser.Errors()
		// Add some basic suggestions
		suggestions = []string{
			"Check for syntax errors",
			"Ensure table names are properly quoted",
			"Verify function names are correct",
		}
	}

	return c.JSON(ValidationResponse{
		Valid:       len(errors) == 0,
		Errors:      errors,
		Suggestions: suggestions,
	})
}

func (s *Server) getSchemaHandler(c *fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON(SchemaResponse{
			Tables:  []string{},
			Columns: map[string][]string{},
		})
	}
	// Mock schema data
	schema := SchemaResponse{
		Tables: []string{"users", "orders", "products"},
		Columns: map[string][]string{
			"users":    {"id", "name", "email", "created_at"},
			"orders":   {"id", "user_id", "product_id", "quantity", "order_date"},
			"products": {"id", "name", "price", "category"},
		},
	}

	return c.JSON(schema)
}

func (s *Server) getIntegrationsHandler(c *fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON([]map[string]interface{}{})
	}
	// Return mock integrations
	integrations := []map[string]interface{}{
		{
			"id":          "1",
			"name":        "users.csv",
			"type":        "file",
			"status":      "connected",
			"description": "User data CSV file",
			"config":      map[string]interface{}{"path": "/data/users.csv", "format": "csv"},
			"createdAt":   "2025-09-15T10:30:00Z",
			"lastUsed":    "2025-09-28T14:20:00Z",
		},
		{
			"id":          "2",
			"name":        "PostgreSQL DB",
			"type":        "database",
			"status":      "connected",
			"description": "Production database",
			"config":      map[string]interface{}{"host": "localhost", "port": 5432, "database": "prod_db"},
			"createdAt":   "2025-09-10T08:00:00Z",
			"lastUsed":    "2025-09-29T09:15:00Z",
		},
	}

	return c.JSON(integrations)
}

func (s *Server) getIntegrationHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":          id,
		"name":        "Mock Integration",
		"type":        "database",
		"status":      "connected",
		"description": "Mock integration",
	})
}

func (s *Server) createIntegrationHandler(c *fiber.Ctx) error {
	var integration map[string]interface{}
	if err := c.BodyParser(&integration); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Mock implementation
	integration["id"] = "new-id"
	integration["createdAt"] = time.Now().Format(time.RFC3339)

	return c.Status(201).JSON(integration)
}

func (s *Server) updateIntegrationHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	var updates map[string]interface{}
	if err := c.BodyParser(&updates); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Integration updated successfully",
	})
}

func (s *Server) deleteIntegrationHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Integration deleted successfully",
	})
}

// Placeholder implementations for other endpoints
func (s *Server) getQueryHistoryHandler(c *fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON([]SavedQuery{})
	}
	history := []SavedQuery{
		{
			ID:        "1",
			Query:     "SELECT * FROM read_file('users.csv')",
			Timestamp: "2025-09-29T10:00:00Z",
			Success:   true,
		},
	}
	return c.JSON(history)
}

func (s *Server) saveQueryHandler(c *fiber.Ctx) error {
	var req struct {
		Query string `json:"query"`
		Name  string `json:"name,omitempty"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	return c.JSON(map[string]interface{}{
		"id": "saved-query-id",
	})
}

func (s *Server) getPipelinesHandler(c *fiber.Ctx) error {
	return c.JSON([]map[string]interface{}{})
}

func (s *Server) getPipelineHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) createPipelineHandler(c *fiber.Ctx) error {
	var pipeline map[string]interface{}
	if err := c.BodyParser(&pipeline); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}
	return c.Status(201).JSON(pipeline)
}

func (s *Server) updatePipelineHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) deletePipelineHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) runPipelineHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id, "status": "running"})
}

func (s *Server) getRunsHandler(c *fiber.Ctx) error {
	return c.JSON([]map[string]interface{}{})
}

func (s *Server) getRunHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) executeConfigHandler(c *fiber.Ctx) error {
	var req ExecuteConfigRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if strings.TrimSpace(req.Config) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Config cannot be empty"})
	}

	// Generate execution ID
	executionID := fmt.Sprintf("exec-%d", time.Now().Unix())

	// Store configuration if not already stored
	configID := fmt.Sprintf("config-%d", time.Now().Unix())
	configName := fmt.Sprintf("Config %s", time.Now().Format("2006-01-02 15:04:05"))

	// Check if this exact config already exists
	configExists := false
	for i, storedConfig := range s.configurations {
		if storedConfig.Content == req.Config && storedConfig.Type == req.Type {
			configID = storedConfig.ID
			configName = storedConfig.Name
			s.configurations[i].LastUsed = time.Now()
			s.configurations[i].Executions++
			configExists = true
			break
		}
	}

	if !configExists {
		storedConfig := StoredConfiguration{
			ID:         configID,
			Name:       configName,
			Type:       req.Type,
			Content:    req.Config,
			CreatedAt:  time.Now(),
			LastUsed:   time.Now(),
			Executions: 1,
		}
		s.configurations = append(s.configurations, storedConfig)
	}

	// Add to executions list
	execution := ExecutionSummary{
		ID:        executionID,
		Config:    req.Config,
		Status:    "running",
		StartTime: time.Now(),
	}
	s.executions = append(s.executions, execution)

	// Parse config based on type
	var cfg *config.Config
	var err error
	switch strings.ToLower(req.Type) {
	case "bcl":
		cfg, err = config.LoadBCLFromString(req.Config)
	case "yaml", "yml":
		cfg, err = config.LoadYamlFromString(req.Config)
	case "json":
		cfg, err = config.LoadJsonFromString(req.Config)
	default:
		err = fmt.Errorf("unsupported config type: %s", req.Type)
	}

	if err != nil {
		// Update execution status
		for i := range s.executions {
			if s.executions[i].ID == executionID {
				s.executions[i].Status = "failed"
				s.executions[i].Error = err.Error()
				now := time.Now()
				s.executions[i].EndTime = &now
				break
			}
		}
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// Run ETL in goroutine
	go func() {
		recordsProcessed := 0
		var allMetrics etl.Metrics
		ids, err := s.etlManager.Prepare(cfg)
		if err != nil {
			// Update execution status
			for i := range s.executions {
				if s.executions[i].ID == executionID {
					s.executions[i].Status = "failed"
					s.executions[i].Error = err.Error()
					now := time.Now()
					s.executions[i].EndTime = &now
					break
				}
			}
			return
		}

		for _, id := range ids {
			if err := s.etlManager.Start(context.Background(), id); err != nil {
				// Update execution status
				for i := range s.executions {
					if s.executions[i].ID == executionID {
						s.executions[i].Status = "failed"
						s.executions[i].Error = err.Error()
						now := time.Now()
						s.executions[i].EndTime = &now
						break
					}
				}
				return
			}
		}

		// Collect final metrics after all ETL jobs complete
		time.Sleep(2 * time.Second) // Give time for final metrics to be recorded

		// Collect all metrics from running ETL instances
		for _, id := range ids {
			if etlInstance, exists := s.etlManager.GetETL(id); exists && etlInstance != nil {
				metrics := etlInstance.GetMetrics()
				recordsProcessed += int(metrics.Loaded)
				allMetrics.Extracted += metrics.Extracted
				allMetrics.Mapped += metrics.Mapped
				allMetrics.Transformed += metrics.Transformed
				allMetrics.Loaded += metrics.Loaded
				allMetrics.Errors += metrics.Errors
				// Collect WorkerActivities from all instances
				allMetrics.WorkerActivities = append(allMetrics.WorkerActivities, metrics.WorkerActivities...)
			}
		}

		// Update execution status with detailed metrics
		for i := range s.executions {
			if s.executions[i].ID == executionID {
				s.executions[i].Status = "completed"
				s.executions[i].RecordsProcessed = recordsProcessed
				s.executions[i].DetailedMetrics = DetailedMetrics{
					Extracted:        allMetrics.Extracted,
					Mapped:           allMetrics.Mapped,
					Transformed:      allMetrics.Transformed,
					Loaded:           allMetrics.Loaded,
					Errors:           allMetrics.Errors,
					WorkerActivities: convertWorkerActivities(allMetrics.WorkerActivities),
				}
				now := time.Now()
				s.executions[i].EndTime = &now
				break
			}
		}
	}()

	return c.JSON(fiber.Map{
		"executionId": executionID,
		"message":     "ETL execution started",
	})
}

func (s *Server) getExecutionsHandler(c *fiber.Ctx) error {
	return c.JSON(s.executions)
}

func (s *Server) getConfigurationsHandler(c *fiber.Ctx) error {
	// Return stored configurations
	configurations := make([]map[string]interface{}, len(s.configurations))
	for i, config := range s.configurations {
		configurations[i] = map[string]interface{}{
			"id":          config.ID,
			"name":        config.Name,
			"type":        config.Type,
			"description": fmt.Sprintf("Executed %d times, last used %s", config.Executions, config.LastUsed.Format("2006-01-02 15:04:05")),
			"path":        fmt.Sprintf("stored-%s", config.Type),
			"createdAt":   config.CreatedAt.Format(time.RFC3339),
			"lastUsed":    config.LastUsed.Format(time.RFC3339),
			"executions":  config.Executions,
		}
	}

	return c.JSON(configurations)
}

func convertWorkerActivities(activities []etl.WorkerActivity) []WorkerActivity {
	result := make([]WorkerActivity, len(activities))
	for i, activity := range activities {
		result[i] = WorkerActivity{
			Node:      activity.Node,
			WorkerID:  activity.WorkerID,
			Processed: activity.Processed,
			Failed:    activity.Failed,
			Timestamp: activity.Timestamp,
			Activity:  activity.Activity,
		}
	}
	return result
}

// Legacy ETL handlers
func (s *Server) getConfigHandler(c *fiber.Ctx) error {
	return c.JSON(config.Config{})
}

func (s *Server) createConfigHandler(c *fiber.Ctx) error {
	var cfg config.Config
	if err := c.BodyParser(&cfg); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid config"})
	}
	return c.JSON(map[string]interface{}{"message": "Config created"})
}

func (s *Server) listETLsHandler(c *fiber.Ctx) error {
	return c.SendString("<h1>ETL Jobs</h1><p>Mock ETL list</p>")
}

func (s *Server) startETLHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.Redirect(fmt.Sprintf("/etls/%s", id))
}

func (s *Server) stopETLHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id, "message": "ETL stopped"})
}

func (s *Server) getETLDetailsHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	return c.SendString(fmt.Sprintf("<h1>ETL %s Details</h1><p>Mock details</p>", id))
}

func (s *Server) getETLMetricsHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	if !s.config.EnableMocks {
		return c.JSON(map[string]interface{}{
			"id":      id,
			"metrics": map[string]interface{}{},
		})
	}
	return c.JSON(map[string]interface{}{
		"id": id,
		"metrics": map[string]interface{}{
			"processed": 1000,
			"duration":  60.5,
		},
	})
}

func (s *Server) Start(addr string) error {
	log.Printf("Starting API server on %s", addr)
	return s.app.Listen(addr)
}

func (s *Server) Shutdown() error {
	log.Println("Shutting down API server gracefully")
	return s.app.Shutdown()
}
