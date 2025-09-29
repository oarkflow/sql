package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/config"
)

// ServiceETLExample demonstrates using integrations as ETL sources
func main() {
	fmt.Println("=== Service-based ETL Example ===\n")

	// 1. Set up integrations manager
	fmt.Println("1. Setting up integrations manager...")
	integrationManager := setupIntegrationsManager()

	// 2. Create ETL with service source
	fmt.Println("\n2. Creating ETL with service source...")
	etlInstance := createServiceETL(integrationManager)

	// 3. Run ETL process
	fmt.Println("\n3. Running ETL process...")
	runServiceETL(etlInstance)

	fmt.Println("\n=== Service ETL Example completed ===")
}

// setupIntegrationsManager creates and configures an integrations manager
func setupIntegrationsManager() *integrations.Manager {
	manager := integrations.New()

	// Add a sample database service
	dbService := integrations.Service{
		Name:          "sample_db",
		Type:          integrations.ServiceTypeDB,
		RequireAuth:   true,
		CredentialKey: "db_creds",
		Enabled:       true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	// Configure database service
	dbConfig := integrations.DatabaseConfig{
		Driver:          "postgres",
		Host:            "localhost",
		Port:            5432,
		Database:        "mydb",
		SSLMode:         "disable",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: "30m",
		ConnectTimeout:  "10s",
		ReadTimeout:     "5s",
		WriteTimeout:    "5s",
		PoolSize:        20,
	}

	dbService.Config = dbConfig

	// Add service to manager
	if err := manager.AddService(dbService); err != nil {
		log.Printf("Failed to add database service: %v", err)
	}

	// Add database credentials
	dbCred := integrations.Credential{
		Key:  "db_creds",
		Type: integrations.CredentialTypeDatabase,
		Data: integrations.DatabaseCredential{
			Username: "postgres",
			Password: "postgres",
		},
	}

	if err := manager.AddCredential(dbCred); err != nil {
		log.Printf("Failed to add database credentials: %v", err)
	}

	services, _ := manager.ServiceList()
	fmt.Printf("Integrations manager configured with %d services\n", len(services))
	return manager
}

// createServiceETL creates an ETL instance with service source
func createServiceETL(integrationManager *integrations.Manager) *etl.ETL {
	etlInstance := etl.NewETL(
		"service_etl_example",
		"ETL using Service Integration",
		etl.WithWorkerCount(4),
		etl.WithBatchSize(100),
		etl.WithStateFile("service_etl_state.json"),
		etl.WithMaxErrorThreshold(50),
	)

	// Add service source using the integration manager
	opts := []etl.Option{
		etl.WithServiceSource(
			integrationManager,
			"sample_db", // Service name
			"SELECT id, name, email, created_at FROM users WHERE created_at >= :created_at", // Custom query
			"",   // Table (not used when query is provided)
			"id", // Key field for checkpointing
			map[string]any{ // Credentials (can be empty if using service's credentials)
				"username": "readonly_user",
				"password": "readonly_pass",
			},
		),
		etl.WithDestination(config.DataConfig{
			Type:     "postgresql",
			Driver:   "postgres",
			Host:     "localhost",
			Username: "postgres",
			Password: "postgres",
			Port:     5432,
			Database: "mydb",
		}, nil, config.TableMapping{
			OldName:         "users",
			NewName:         "processed_users",
			CloneSource:     true,
			Migrate:         true,
			AutoCreateTable: true,
			Mapping: map[string]string{
				"id":         "user_id",
				"name":       "full_name",
				"email":      "email_address",
				"created_at": "registration_date",
			},
		}),
	}

	// Apply service source configuration
	for _, opt := range opts {
		if err := opt(etlInstance); err != nil {
			log.Fatalf("Failed to configure service ETL: %v", err)
		}
	}

	return etlInstance
}

// runServiceETL runs the service-based ETL process
func runServiceETL(etlInstance *etl.ETL) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Set up progress monitoring
	monitorProgress(etlInstance)

	// Run ETL
	startTime := time.Now()
	if err := etlInstance.Run(ctx, map[string]any{"created_at": "2023-01-01"}); err != nil {
		log.Printf("Service ETL failed: %v", err)
		return
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Service ETL completed in %v\n", elapsed)

	// Show final metrics
	metrics := etlInstance.GetEnhancedMetrics()
	if basic, ok := metrics["basic"].(etl.Metrics); ok {
		fmt.Printf("Final results: Extracted: %d, Loaded: %d, Errors: %d\n",
			basic.Extracted, basic.Loaded, basic.Errors)
	}
}

// monitorProgress monitors ETL progress in real-time
func monitorProgress(etlInstance *etl.ETL) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			metrics := etlInstance.GetEnhancedMetrics()
			if basic, ok := metrics["basic"].(etl.Metrics); ok {
				fmt.Printf("Progress: Extracted: %d, Loaded: %d, Errors: %d\n",
					basic.Extracted, basic.Loaded, basic.Errors)
			}

			// Check if ETL is still running
			if etlInstance.Status != "RUNNING" {
				ticker.Stop()
				break
			}
		}
	}()
}

// demonstrateServiceWithTableQuery shows using table-based queries
func demonstrateServiceWithTableQuery() {
	fmt.Println("=== Table-based Query Example ===")

	manager := setupIntegrationsManager()

	etlInstance := etl.NewETL(
		"table_query_etl",
		"ETL with Table-based Service Query",
		etl.WithWorkerCount(2),
		etl.WithBatchSize(50),
	)

	// Use table name instead of custom query
	serviceOpt := etl.WithServiceSource(
		manager,
		"sample_db",
		"",           // Empty query means use table name
		"products",   // Table name
		"product_id", // Key field for checkpointing
		nil,
	)

	if err := serviceOpt(etlInstance); err != nil {
		log.Printf("Failed to add table-based service: %v", err)
		return
	}

	// Add destination
	destOpt := etl.WithDestination(config.DataConfig{
		Type:     "postgresql",
		Driver:   "postgres",
		Host:     "localhost",
		Username: "postgres",
		Password: "postgres",
		Port:     5432,
		Database: "mydb",
	}, nil, config.TableMapping{
		NewName:         "product_data",
		CloneSource:     true,
		Migrate:         true,
		AutoCreateTable: true,
	})

	if err := destOpt(etlInstance); err != nil {
		log.Printf("Failed to add destination: %v", err)
		return
	}

	fmt.Println("Configured ETL with table-based query")

	// Run the table-based ETL
	ctx := context.Background()
	if err := etlInstance.Run(ctx); err != nil {
		log.Printf("Table-based ETL failed: %v", err)
	} else {
		fmt.Println("Table-based ETL completed successfully")
	}
}

// demonstrateServiceWithComplexQuery shows using complex SQL queries
func demonstrateServiceWithComplexQuery() {
	fmt.Println("=== Complex Query Example ===")

	manager := setupIntegrationsManager()

	etlInstance := etl.NewETL(
		"complex_query_etl",
		"ETL with Complex Service Query",
		etl.WithWorkerCount(2),
		etl.WithBatchSize(50),
	)

	// Use a complex query with aggregations
	complexQuery := `
		SELECT
			customer_id,
			COUNT(order_id) as total_orders,
			SUM(amount) as total_spent,
			AVG(amount) as avg_order_value,
			MAX(order_date) as last_order_date
		FROM orders
		WHERE order_date >= CURRENT_DATE - INTERVAL '1 year'
		GROUP BY customer_id
		HAVING COUNT(order_id) > 5
		ORDER BY total_spent DESC
	`

	serviceOpt := etl.WithServiceSource(
		manager,
		"sample_db",
		complexQuery,
		"",            // No table needed for custom query
		"customer_id", // Use customer ID as checkpoint key
		nil,
	)

	if err := serviceOpt(etlInstance); err != nil {
		log.Printf("Failed to add complex query service: %v", err)
		return
	}

	// Add destination for aggregated data
	destOpt := etl.WithDestination(config.DataConfig{
		Type:     "postgresql",
		Driver:   "postgres",
		Host:     "localhost",
		Username: "postgres",
		Password: "postgres",
		Port:     5432,
		Database: "mydb",
	}, nil, config.TableMapping{
		NewName:         "customer_analytics",
		CloneSource:     true,
		Migrate:         true,
		AutoCreateTable: true,
		Mapping: map[string]string{
			"customer_id":     "customer_id",
			"total_orders":    "order_count",
			"total_spent":     "total_revenue",
			"avg_order_value": "average_order",
			"last_order_date": "last_purchase",
		},
	})

	if err := destOpt(etlInstance); err != nil {
		log.Printf("Failed to add destination: %v", err)
		return
	}

	fmt.Println("Configured ETL with complex aggregation query")

	// Run the complex query ETL
	ctx := context.Background()
	if err := etlInstance.Run(ctx); err != nil {
		log.Printf("Complex query ETL failed: %v", err)
	} else {
		fmt.Println("Complex query ETL completed successfully")
	}
}

// demonstrateServiceErrorHandling shows error handling with service sources
func demonstrateServiceErrorHandling() {
	fmt.Println("=== Service Error Handling ===")

	manager := setupIntegrationsManager()

	etlInstance := etl.NewETL(
		"error_handling_etl",
		"ETL with Service Error Handling",
		etl.WithWorkerCount(2),
		etl.WithBatchSize(25),
		etl.WithMaxErrorThreshold(10),                           // Low threshold to demonstrate error handling
		etl.WithDLQConfig(1000, 3, time.Minute, 10*time.Minute), // Robust retry
	)

	// Add service source that might fail
	serviceOpt := etl.WithServiceSource(
		manager,
		"sample_db",
		"SELECT id, name, invalid_column FROM users", // This will cause errors
		"users",
		"id",
		nil,
	)

	if err := serviceOpt(etlInstance); err != nil {
		log.Printf("Failed to add service source: %v", err)
		return
	}

	// Add destination
	destOpt := etl.WithDestination(config.DataConfig{
		Type:     "postgresql",
		Driver:   "postgres",
		Host:     "localhost",
		Username: "postgres",
		Password: "postgres",
		Port:     5432,
		Database: "mydb",
	}, nil, config.TableMapping{
		NewName:         "error_test_data",
		CloneSource:     true,
		Migrate:         true,
		AutoCreateTable: true,
	})

	if err := destOpt(etlInstance); err != nil {
		log.Printf("Failed to add destination: %v", err)
		return
	}

	fmt.Println("Configured ETL with error-prone query to demonstrate error handling")

	// Run ETL (will likely fail due to invalid column)
	ctx := context.Background()
	if err := etlInstance.Run(ctx); err != nil {
		fmt.Printf("ETL failed as expected: %v\n", err)

		// Show error details
		state := etlInstance.GetStateInfo()
		if state != nil {
			fmt.Printf("Total errors recorded: %d\n", len(state.ErrorDetails))

			// Show recent errors
			for i, errorDetail := range state.ErrorDetails {
				if i >= 3 { // Show only first 3 errors
					break
				}
				fmt.Printf("Error %d: %s (Worker: %d)\n",
					i+1, errorDetail.Error, errorDetail.WorkerID)
			}
		}

		// Show dead letter queue stats
		if dlq := etlInstance.GetDeadLetterQueue(); dlq != nil {
			stats := dlq.GetQueueStats()
			fmt.Printf("Dead letter queue stats: %+v\n", stats)
		}
	}
}

// demonstrateServiceResume shows resume capability with service sources
func demonstrateServiceResume() {
	fmt.Println("=== Service Resume Capability ===")

	manager := setupIntegrationsManager()

	etlInstance := etl.NewETL(
		"resumable_service_etl",
		"Resumable Service ETL",
		etl.WithWorkerCount(2),
		etl.WithBatchSize(50),
		etl.WithStateFile("service_resume_state.json"),
	)

	// Add resumable service source
	serviceOpt := etl.WithServiceSource(
		manager,
		"sample_db",
		"SELECT id, name, email FROM users ORDER BY id LIMIT 1000", // Limited for demo
		"users",
		"id", // Use ID for checkpointing
		nil,
	)

	if err := serviceOpt(etlInstance); err != nil {
		log.Printf("Failed to add service source: %v", err)
		return
	}

	// Add destination
	destOpt := etl.WithDestination(config.DataConfig{
		Type:     "postgresql",
		Driver:   "postgres",
		Host:     "localhost",
		Username: "postgres",
		Password: "postgres",
		Port:     5432,
		Database: "mydb",
	}, nil, config.TableMapping{
		NewName:         "resumable_data",
		CloneSource:     true,
		Migrate:         true,
		AutoCreateTable: true,
	})

	if err := destOpt(etlInstance); err != nil {
		log.Printf("Failed to add destination: %v", err)
		return
	}

	// Check if resume is possible
	if etlInstance.GetStateManager().CanResume() {
		fmt.Println("Resume available")

		// Get resume information
		resumeInfo := etlInstance.GetStateManager().GetResumeInfo()
		fmt.Printf("Resume info: %+v\n", resumeInfo)

		// Perform resume
		if err := etlInstance.Resume(context.Background()); err != nil {
			log.Printf("Resume failed: %v", err)
		} else {
			fmt.Println("Resume completed successfully")
		}
	} else {
		fmt.Println("No resume available, running fresh ETL")

		// Run fresh ETL
		ctx := context.Background()
		if err := etlInstance.Run(ctx); err != nil {
			log.Printf("Fresh ETL run failed: %v", err)
		} else {
			fmt.Println("Fresh ETL run completed successfully")
		}
	}
}
