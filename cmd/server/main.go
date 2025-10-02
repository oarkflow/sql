package main

import (
	"context"
	"fmt"
	"log"
	"os"
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

type Server struct {
	app                *fiber.App
	etlManager         *etl.Manager
	integrationManager *integrations.Manager
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

func NewServer() *Server {
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

	// Legacy ETL endpoints (for compatibility)
	s.app.Get("/config", s.getConfigHandler)
	s.app.Post("/config", s.createConfigHandler)
	s.app.Get("/etls", s.listETLsHandler)
	s.app.Get("/etls/:id/start", s.startETLHandler)
	s.app.Post("/etls/:id/stop", s.stopETLHandler)
	s.app.Get("/etls/:id", s.getETLDetailsHandler)
	s.app.Get("/etls/:id/metrics", s.getETLMetricsHandler)
}

func (s *Server) healthHandler(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "healthy",
		"version":   "1.0.0",
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
	// For now, return mock schema data
	// In a real implementation, this would query the integration
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
	// Return mock integrations for now
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

func main() {
	server := NewServer()

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	addr := ":" + port
	log.Fatal(server.Start(addr))
}
