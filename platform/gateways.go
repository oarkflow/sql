package platform

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/oarkflow/squealx"
)

// HTTPGateway implements HTTP endpoints for data ingestion
type HTTPGateway struct {
	app        *fiber.App
	port       string
	messageBus MessageBus
	storage    Storage
}

// Storage interface for storing raw payloads
type Storage interface {
	Store(id string, data []byte) error
	Retrieve(id string) ([]byte, error)
	Delete(id string) error
}

func NewHTTPGateway(port string, messageBus MessageBus, storage Storage) *HTTPGateway {
	app := fiber.New()

	gw := &HTTPGateway{
		app:        app,
		port:       port,
		messageBus: messageBus,
		storage:    storage,
	}

	// Setup routes
	app.Post("/ingest", gw.handleIngest)
	app.Post("/ingest/file", gw.handleFileUpload)
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	return gw
}

func (gw *HTTPGateway) Start() error {
	log.Printf("Starting HTTP Gateway on port %s", gw.port)
	return gw.app.Listen(":" + gw.port)
}

func (gw *HTTPGateway) Stop() error {
	return gw.app.Shutdown()
}

func (gw *HTTPGateway) Name() string {
	return "http-gateway"
}

func (gw *HTTPGateway) handleIngest(c *fiber.Ctx) error {
	// Accept raw data
	body := c.Body()
	contentType := string(c.Get("Content-Type"))
	source := c.Get("X-Source", "http")
	origin := c.Get("X-Origin", c.IP())

	// Create IngestMessage
	msg := IngestMessage{
		ID:          uuid.New().String(),
		RawID:       uuid.New().String(),
		Payload:     body,
		Source:      source,
		Origin:      origin,
		Timestamp:   time.Now(),
		ContentType: contentType,
		Metadata: map[string]any{
			"headers": c.GetReqHeaders(),
			"method":  c.Method(),
			"path":    c.Path(),
		},
	}

	// Store raw payload
	if err := gw.storage.Store(msg.RawID, body); err != nil {
		log.Printf("Failed to store raw payload: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "storage failed"})
	}

	// Publish to message bus
	if err := gw.messageBus.Publish("ingest.raw", msg); err != nil {
		log.Printf("Failed to publish message: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "publish failed"})
	}

	return c.JSON(fiber.Map{
		"id":     msg.ID,
		"raw_id": msg.RawID,
		"status": "accepted",
	})
}

func (gw *HTTPGateway) handleFileUpload(c *fiber.Ctx) error {
	// Handle multipart file upload
	file, err := c.FormFile("file")
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "no file provided"})
	}

	// Open uploaded file
	src, err := file.Open()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to open file"})
	}
	defer src.Close()

	// Read file content
	body, err := io.ReadAll(src)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to read file"})
	}

	// Detect content type
	contentType := http.DetectContentType(body)
	source := c.Get("X-Source", "file-upload")
	origin := file.Filename

	// Create IngestMessage
	msg := IngestMessage{
		ID:          uuid.New().String(),
		RawID:       uuid.New().String(),
		Payload:     body,
		Source:      source,
		Origin:      origin,
		Timestamp:   time.Now(),
		ContentType: contentType,
		Metadata: map[string]any{
			"filename": file.Filename,
			"size":     file.Size,
			"headers":  c.GetReqHeaders(),
		},
	}

	// Store raw payload
	if err := gw.storage.Store(msg.RawID, body); err != nil {
		log.Printf("Failed to store raw payload: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "storage failed"})
	}

	// Publish to message bus
	if err := gw.messageBus.Publish("ingest.raw", msg); err != nil {
		log.Printf("Failed to publish message: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "publish failed"})
	}

	return c.JSON(fiber.Map{
		"id":       msg.ID,
		"raw_id":   msg.RawID,
		"filename": file.Filename,
		"status":   "accepted",
	})
}

// FileWatchGateway monitors directories for new files
type FileWatchGateway struct {
	watchPaths []string
	messageBus MessageBus
	storage    Storage
	stopChan   chan struct{}
}

func NewFileWatchGateway(watchPaths []string, messageBus MessageBus, storage Storage) *FileWatchGateway {
	return &FileWatchGateway{
		watchPaths: watchPaths,
		messageBus: messageBus,
		storage:    storage,
		stopChan:   make(chan struct{}),
	}
}

func (gw *FileWatchGateway) Start() error {
	log.Printf("Starting File Watch Gateway for paths: %v", gw.watchPaths)

	// For simplicity, we'll do periodic scanning
	// In production, use fsnotify for real file watching
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-gw.stopChan:
				return
			case <-ticker.C:
				gw.scanFiles()
			}
		}
	}()

	return nil
}

func (gw *FileWatchGateway) Stop() error {
	close(gw.stopChan)
	return nil
}

func (gw *FileWatchGateway) Name() string {
	return "file-watch-gateway"
}

func (gw *FileWatchGateway) scanFiles() {
	for _, path := range gw.watchPaths {
		files, err := filepath.Glob(filepath.Join(path, "*"))
		if err != nil {
			log.Printf("Error scanning path %s: %v", path, err)
			continue
		}

		for _, file := range files {
			if gw.isProcessed(file) {
				continue
			}

			if err := gw.processFile(file); err != nil {
				log.Printf("Error processing file %s: %v", file, err)
			}
		}
	}
}

func (gw *FileWatchGateway) isProcessed(file string) bool {
	// Simple check - in production, maintain a processed files registry
	// For now, just check if file exists (simplified)
	return false
}

func (gw *FileWatchGateway) processFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	contentType := http.DetectContentType(body)
	source := "file-watch"
	origin := filePath

	msg := IngestMessage{
		ID:          uuid.New().String(),
		RawID:       uuid.New().String(),
		Payload:     body,
		Source:      source,
		Origin:      origin,
		Timestamp:   time.Now(),
		ContentType: contentType,
		Metadata: map[string]any{
			"filepath": filePath,
		},
	}

	// Store raw payload
	if err := gw.storage.Store(msg.RawID, body); err != nil {
		return fmt.Errorf("failed to store raw payload: %w", err)
	}

	// Publish to message bus
	if err := gw.messageBus.Publish("ingest.raw", msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("Processed file: %s", filePath)
	return nil
}

// URLFetcherGateway fetches data from URLs
type URLFetcherGateway struct {
	urls       []string
	interval   time.Duration
	messageBus MessageBus
	storage    Storage
	stopChan   chan struct{}
}

func NewURLFetcherGateway(urls []string, interval time.Duration, messageBus MessageBus, storage Storage) *URLFetcherGateway {
	return &URLFetcherGateway{
		urls:       urls,
		interval:   interval,
		messageBus: messageBus,
		storage:    storage,
		stopChan:   make(chan struct{}),
	}
}

func (gw *URLFetcherGateway) Start() error {
	log.Printf("Starting URL Fetcher Gateway for URLs: %v", gw.urls)

	go func() {
		ticker := time.NewTicker(gw.interval)
		defer ticker.Stop()

		for {
			select {
			case <-gw.stopChan:
				return
			case <-ticker.C:
				gw.fetchURLs()
			}
		}
	}()

	return nil
}

func (gw *URLFetcherGateway) Stop() error {
	close(gw.stopChan)
	return nil
}

func (gw *URLFetcherGateway) Name() string {
	return "url-fetcher-gateway"
}

func (gw *URLFetcherGateway) fetchURLs() {
	for _, url := range gw.urls {
		if err := gw.fetchURL(url); err != nil {
			log.Printf("Error fetching URL %s: %v", url, err)
		}
	}
}

func (gw *URLFetcherGateway) fetchURL(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = http.DetectContentType(body)
	}
	source := "url-fetch"
	origin := url

	msg := IngestMessage{
		ID:          uuid.New().String(),
		RawID:       uuid.New().String(),
		Payload:     body,
		Source:      source,
		Origin:      origin,
		Timestamp:   time.Now(),
		ContentType: contentType,
		Metadata: map[string]any{
			"url":         url,
			"status_code": resp.StatusCode,
			"headers":     resp.Header,
		},
	}

	// Store raw payload
	if err := gw.storage.Store(msg.RawID, body); err != nil {
		return fmt.Errorf("failed to store raw payload: %w", err)
	}

	// Publish to message bus
	if err := gw.messageBus.Publish("ingest.raw", msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("Fetched URL: %s", url)
	return nil
}

// DatabasePollerGateway polls databases for new data
type DatabasePollerGateway struct {
	db         *squealx.DB
	query      string
	interval   time.Duration
	messageBus MessageBus
	storage    Storage
	lastID     string
	stopChan   chan struct{}
}

func NewDatabasePollerGateway(db *squealx.DB, query string, interval time.Duration, messageBus MessageBus, storage Storage) *DatabasePollerGateway {
	return &DatabasePollerGateway{
		db:         db,
		query:      query,
		interval:   interval,
		messageBus: messageBus,
		storage:    storage,
		stopChan:   make(chan struct{}),
	}
}

func (gw *DatabasePollerGateway) Start() error {
	log.Printf("Starting Database Poller Gateway")

	go func() {
		ticker := time.NewTicker(gw.interval)
		defer ticker.Stop()

		for {
			select {
			case <-gw.stopChan:
				return
			case <-ticker.C:
				if err := gw.pollDatabase(); err != nil {
					log.Printf("Error polling database: %v", err)
				}
			}
		}
	}()

	return nil
}

func (gw *DatabasePollerGateway) Stop() error {
	close(gw.stopChan)
	return nil
}

func (gw *DatabasePollerGateway) Name() string {
	return "db-poller-gateway"
}

func (gw *DatabasePollerGateway) pollDatabase() error {
	// Execute query to get new records
	rows, err := gw.db.Query(gw.query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// Convert to JSON
		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = values[i]
		}

		data, err := json.Marshal(record)
		if err != nil {
			return err
		}

		msg := IngestMessage{
			ID:          uuid.New().String(),
			RawID:       uuid.New().String(),
			Payload:     data,
			Source:      "db-poll",
			Origin:      gw.query,
			Timestamp:   time.Now(),
			ContentType: "application/json",
			Metadata: map[string]any{
				"query":   gw.query,
				"columns": columns,
			},
		}

		// Store raw payload
		if err := gw.storage.Store(msg.RawID, data); err != nil {
			return fmt.Errorf("failed to store raw payload: %w", err)
		}

		// Publish to message bus
		if err := gw.messageBus.Publish("ingest.raw", msg); err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}

	return nil
}
