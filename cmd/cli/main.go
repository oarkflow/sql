package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/oarkflow/sql/etl"
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
				Usage: "Run ETL from a configuration file",
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
					&cli.BoolFlag{
						Name:  "enable-mocks",
						Value: false,
						Usage: "Enable mock data for development",
					},
				},
				Action: startServer,
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

	// Run ETL
	manager := etl.NewManager()
	ids, err := manager.Prepare(cfg)
	if err != nil {
		return fmt.Errorf("error preparing ETL: %v", err)
	}

	for _, id := range ids {
		fmt.Printf("Starting ETL job: %s\n", id)
		if err := manager.Start(context.Background(), id); err != nil {
			return fmt.Errorf("error starting ETL job %s: %v", id, err)
		}
		fmt.Printf("ETL job %s completed successfully\n", id)
	}

	return nil
}

func startServer(c *cli.Context) error {
	port := c.String("port")
	staticPath := c.String("static-path")
	version := c.String("version")
	enableMocks := c.Bool("enable-mocks")

	fmt.Printf("Starting ETL server on port %s\n", port)

	srvConfig := server.Config{
		Version:     version,
		StaticPath:  staticPath,
		EnableMocks: enableMocks,
	}

	srv := server.NewServer(srvConfig)
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
