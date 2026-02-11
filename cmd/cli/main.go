package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/server"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "etl",
		Usage: "ETL tool for data processing",
		Commands: []*cli.Command{
			{
				Name:  "cli",
				Usage: "Run ETL from a configuration file (requires server to be running)",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "file",
						Aliases:  []string{"f"},
						Usage:    "Path to the configuration file (BCL, YAML, or JSON)",
						Required: true,
					},
				},
				Action: runETLFromFile,
			},
			{
				Name:  "serve",
				Usage: "Start the ETL server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "port",
						Value: "8080",
						Usage: "Port to run the server on",
					},
					&cli.StringFlag{
						Name:  "static-path",
						Value: "./views",
						Usage: "Path to static files directory",
					},
					&cli.StringFlag{
						Name:  "version",
						Value: "1.0.0",
						Usage: "Server version",
					},
					&cli.StringFlag{
						Name:  "database-path",
						Value: "./data/app.db",
						Usage: "Path to the control-plane SQLite database",
					},
					&cli.StringFlag{
						Name:    "encryption-key",
						Usage:   "Key used to encrypt sensitive credential payloads",
						EnvVars: []string{"APP_ENCRYPTION_KEY"},
					},
					&cli.BoolFlag{
						Name:  "enable-mocks",
						Value: false,
						Usage: "Enable mock data for development",
					},
				},
				Action: startServer,
			},
			{
				Name:  "runner",
				Usage: "Manage ETL runner jobs via server",
				Subcommands: []*cli.Command{
					{
						Name:  "start",
						Usage: "Start the ETL server with runner capabilities",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  "port",
								Value: "8080",
								Usage: "Port to run the server on",
							},
							&cli.StringFlag{
								Name:  "host",
								Value: "localhost",
								Usage: "Host to bind the server to",
							},
							&cli.StringFlag{
								Name:  "database-path",
								Value: "./data/app.db",
								Usage: "Path to the control-plane SQLite database",
							},
							&cli.StringFlag{
								Name:    "encryption-key",
								Usage:   "Key used to encrypt sensitive credential payloads",
								EnvVars: []string{"APP_ENCRYPTION_KEY"},
							},
						},
						Action: startRunnerServer,
					},
					{
						Name:  "submit",
						Usage: "Submit an ETL job to the running server",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "file",
								Aliases:  []string{"f"},
								Usage:    "Path to the configuration file",
								Required: true,
							},
							&cli.StringFlag{
								Name:  "name",
								Usage: "Name for the job",
							},
							&cli.StringFlag{
								Name:  "server",
								Value: "http://localhost:8080",
								Usage: "Server URL to submit job to",
							},
						},
						Action: submitJobToServer,
					},
					{
						Name:  "status",
						Usage: "Get job status from server",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "job-id",
								Aliases:  []string{"j"},
								Usage:    "Job ID to check",
								Required: true,
							},
							&cli.StringFlag{
								Name:  "server",
								Value: "http://localhost:8080",
								Usage: "Server URL",
							},
						},
						Action: getJobStatusFromServer,
					},
					{
						Name:  "list",
						Usage: "List jobs from server",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  "status",
								Usage: "Filter by status (pending, running, completed, failed, cancelled)",
							},
							&cli.StringFlag{
								Name:  "server",
								Value: "http://localhost:8080",
								Usage: "Server URL",
							},
						},
						Action: listJobsFromServer,
					},
					{
						Name:  "cancel",
						Usage: "Cancel a job on server",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "job-id",
								Aliases:  []string{"j"},
								Usage:    "Job ID to cancel",
								Required: true,
							},
							&cli.StringFlag{
								Name:  "server",
								Value: "http://localhost:8080",
								Usage: "Server URL",
							},
						},
						Action: cancelJobOnServer,
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runETLFromFile(c *cli.Context) error {
	filePath := c.String("file")

	// Determine file type based on extension
	ext := filepath.Ext(filePath)
	var cfg *config.Config
	var err error

	switch ext {
	case ".bcl":
		cfg, err = config.LoadBCL(filePath)
	case ".yaml", ".yml":
		cfg, err = config.LoadYaml(filePath)
	case ".json":
		cfg, err = config.LoadJson(filePath)
	default:
		return fmt.Errorf("unsupported file type: %s. Supported types: .bcl, .yaml, .yml, .json", ext)
	}

	if err != nil {
		return fmt.Errorf("error loading config: %v", err)
	}

	// Synchronous execution
	fmt.Printf("Running ETL from %s\n", filePath)

	// For now, use the server to execute (this could be improved)
	// Convert config to the format expected by server
	configData, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("error marshaling config: %v", err)
	}

	configType := strings.TrimPrefix(ext, ".")

	reqData := map[string]interface{}{
		"config": string(configData),
		"type":   configType,
	}

	reqBody, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	// Make HTTP request to local server (assuming it's running)
	url := "http://localhost:8080/api/execute"
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("error executing ETL (make sure server is running): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned error %d: %s", resp.StatusCode, string(body))
	}

	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	executionID, ok := response["executionId"].(string)
	if !ok {
		return fmt.Errorf("invalid response from server")
	}

	fmt.Printf("ETL execution started with ID: %s\n", executionID)
	fmt.Println("Use 'etl runner status --job-id <id>' to check progress")

	return nil
}

func startServer(c *cli.Context) error {
	port := c.String("port")
	staticPath := c.String("static-path")
	version := c.String("version")
	enableMocks := c.Bool("enable-mocks")
	dbPath := c.String("database-path")
	encryptionKey := c.String("encryption-key")

	fmt.Printf("Starting ETL server on port %s\n", port)

	srvConfig := server.Config{
		Version:       version,
		StaticPath:    staticPath,
		EnableMocks:   enableMocks,
		DatabasePath:  dbPath,
		EncryptionKey: encryptionKey,
	}

	srv, err := server.NewServer(srvConfig)
	if err != nil {
		return err
	}
	addr := ":" + port

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		fmt.Printf("Server listening on %s\n", addr)
		serverErr <- srv.Start(addr)
	}()

	// Wait for either server error or shutdown signal
	select {
	case err := <-serverErr:
		return err
	case sig := <-sigChan:
		fmt.Printf("Received signal: %v. Initiating graceful shutdown...\n", sig)
		// Shutdown the server gracefully
		if err := srv.Shutdown(); err != nil {
			fmt.Printf("Error shutting down server: %v\n", err)
			return err
		}
		// Wait for the server to finish shutting down
		select {
		case err := <-serverErr:
			if err != nil {
				return err
			}
			fmt.Println("Server shut down gracefully")
			return nil
		case <-time.After(30 * time.Second):
			fmt.Println("Shutdown timeout reached, forcing exit")
			os.Exit(1)
		}
		return nil // This should not be reached
	}
}

func startRunnerServer(c *cli.Context) error {
	port := c.String("port")
	host := c.String("host")
	dbPath := c.String("database-path")
	encryptionKey := c.String("encryption-key")

	fmt.Printf("Starting ETL server with runner capabilities on %s:%s\n", host, port)

	srvConfig := server.Config{
		Version:       "1.0.0",
		StaticPath:    "./views",
		EnableMocks:   false,
		DatabasePath:  dbPath,
		EncryptionKey: encryptionKey,
	}

	srv, err := server.NewServer(srvConfig)
	if err != nil {
		return err
	}
	addr := host + ":" + port

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		fmt.Printf("Server listening on %s\n", addr)
		serverErr <- srv.Start(addr)
	}()

	// Wait for either server error or shutdown signal
	select {
	case err := <-serverErr:
		return err
	case sig := <-sigChan:
		fmt.Printf("Received signal: %v. Initiating graceful shutdown...\n", sig)
		// Shutdown the server gracefully
		if err := srv.Shutdown(); err != nil {
			fmt.Printf("Error shutting down server: %v\n", err)
			return err
		}
		// Wait for the server to finish shutting down
		select {
		case err := <-serverErr:
			if err != nil {
				return err
			}
			fmt.Println("Server shut down gracefully")
			return nil
		case <-time.After(30 * time.Second):
			fmt.Println("Shutdown timeout reached, forcing exit")
			os.Exit(1)
		}
		return nil // This should not be reached
	}
}

func submitJobToServer(c *cli.Context) error {
	filePath := c.String("file")
	jobName := c.String("name")
	serverURL := c.String("server")

	// Load config
	ext := filepath.Ext(filePath)
	var cfg *config.Config
	var err error

	switch ext {
	case ".bcl":
		cfg, err = config.LoadBCL(filePath)
	case ".yaml", ".yml":
		cfg, err = config.LoadYaml(filePath)
	case ".json":
		cfg, err = config.LoadJson(filePath)
	default:
		return fmt.Errorf("unsupported file type: %s", ext)
	}

	if err != nil {
		return fmt.Errorf("error loading config: %v", err)
	}

	// Convert config to the format expected by server
	configData, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("error marshaling config: %v", err)
	}

	// Determine config type
	configType := strings.TrimPrefix(ext, ".")

	// Prepare request
	reqData := map[string]interface{}{
		"config": string(configData),
		"type":   configType,
	}

	if jobName != "" {
		reqData["name"] = jobName
	}

	reqBody, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	// Make HTTP request to server
	url := serverURL + "/api/execute"
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("error submitting job to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned error %d: %s", resp.StatusCode, string(body))
	}

	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	executionID, ok := response["executionId"].(string)
	if !ok {
		return fmt.Errorf("invalid response from server")
	}

	fmt.Printf("Job submitted successfully with execution ID: %s\n", executionID)
	return nil
}

func getJobStatusFromServer(c *cli.Context) error {
	jobID := c.String("job-id")
	serverURL := c.String("server")

	url := serverURL + "/api/executions"
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error connecting to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned error %d: %s", resp.StatusCode, string(body))
	}

	var executions []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&executions); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	// Find the job by ID
	for _, exec := range executions {
		if id, ok := exec["id"].(string); ok && id == jobID {
			fmt.Printf("Job ID: %s\n", exec["id"])
			fmt.Printf("Config: %s\n", exec["config"])
			fmt.Printf("Status: %s\n", exec["status"])

			if startTime, ok := exec["startTime"].(string); ok {
				fmt.Printf("Start Time: %s\n", startTime)
			}

			if endTime, ok := exec["endTime"].(string); ok && endTime != "" {
				fmt.Printf("End Time: %s\n", endTime)
			}

			if errorMsg, ok := exec["error"].(string); ok && errorMsg != "" {
				fmt.Printf("Error: %s\n", errorMsg)
			}

			if metrics, ok := exec["detailedMetrics"].(map[string]interface{}); ok {
				fmt.Printf("Records Processed: %v\n", metrics["loaded"])
				fmt.Printf("Metrics - Extracted: %v, Mapped: %v, Transformed: %v, Loaded: %v, Errors: %v\n",
					metrics["extracted"], metrics["mapped"], metrics["transformed"], metrics["loaded"], metrics["errors"])
			}

			return nil
		}
	}

	return fmt.Errorf("job with ID %s not found", jobID)
}

func listJobsFromServer(c *cli.Context) error {
	statusFilter := c.String("status")
	serverURL := c.String("server")

	url := serverURL + "/api/executions"
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error connecting to server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned error %d: %s", resp.StatusCode, string(body))
	}

	var executions []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&executions); err != nil {
		return fmt.Errorf("error parsing response: %v", err)
	}

	if len(executions) == 0 {
		fmt.Println("No jobs found")
		return nil
	}

	fmt.Printf("%-20s %-30s %-12s %-20s\n", "Job ID", "Config", "Status", "Start Time")
	fmt.Println(strings.Repeat("-", 82))

	for _, exec := range executions {
		id := exec["id"].(string)
		status := exec["status"].(string)

		// Apply status filter if specified
		if statusFilter != "" && status != statusFilter {
			continue
		}

		config := exec["config"].(string)
		if len(config) > 28 {
			config = config[:25] + "..."
		}

		startTime := "N/A"
		if st, ok := exec["startTime"].(string); ok {
			if t, err := time.Parse(time.RFC3339, st); err == nil {
				startTime = t.Format("2006-01-02 15:04:05")
			}
		}

		fmt.Printf("%-20s %-30s %-12s %-20s\n", id, config, status, startTime)
	}

	return nil
}

func cancelJobOnServer(c *cli.Context) error {
	// Note: The current server doesn't have a cancel endpoint
	// This is a placeholder for future implementation
	return fmt.Errorf("job cancellation not yet implemented in server API")
}
