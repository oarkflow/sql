package main

import (
	"context"
	stdjson "encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/oarkflow/log"
	"github.com/oarkflow/mail"
	"github.com/oarkflow/sql"
	"github.com/oarkflow/sql/integrations"
)

func main() {
	logger := &log.DefaultLogger
	ctx := context.Background()
	manager := integrations.New(integrations.WithLogger(logger))

	configPath := flag.String("config", "config.json", "Path to configuration file")
	serviceName := flag.String("service", "prod-database", "Database integration service name")
	query := flag.String("query", "SELECT id, first_name, password_hash FROM users LIMIT 5", "Query to execute")
	applyExampleACL := flag.Bool("apply-example-acl", true, "Apply CLI whitelist/denylist to the selected DB service")
	tableWhitelist := flag.String("table-whitelist", "", "Comma-separated table whitelist (empty = allow all)")
	tableDenylist := flag.String("table-denylist", "", "Comma-separated table denylist")
	fieldWhitelist := flag.String("field-whitelist", "", "Comma-separated field whitelist (empty = allow all)")
	fieldDenylist := flag.String("field-denylist", "users.password_hash", "Comma-separated field denylist")
	flag.Parse()

	if _, err := manager.LoadIntegrationsFromFile(ctx, *configPath); err != nil {
		panic(err)
	}

	if *applyExampleACL {
		err := applyDatabaseAccessControlExample(
			manager,
			*serviceName,
			parseCSVList(*tableWhitelist),
			parseCSVList(*tableDenylist),
			parseCSVList(*fieldWhitelist),
			parseCSVList(*fieldDenylist),
		)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to apply example access control to service")
		}
	} else {
		fmt.Println("ACL override not applied from CLI; using integration config as-is.")
	}

	printDatabaseAccessControlConfigExample()
	runDatabaseIntegrationExamples(ctx, manager, *serviceName, *query)
}

func parseCSVList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func applyDatabaseAccessControlExample(
	manager *integrations.Manager,
	serviceName string,
	tableWhitelist, tableDenylist, fieldWhitelist, fieldDenylist []string,
) error {
	svc, err := manager.GetService(serviceName)
	if err != nil {
		return err
	}
	if svc.Type != integrations.ServiceTypeDB {
		return fmt.Errorf("service %q is not a database integration", serviceName)
	}
	cfg, ok := svc.Config.(integrations.DatabaseConfig)
	if !ok {
		return fmt.Errorf("service %q has invalid database config", serviceName)
	}
	cfg.TableWhitelist = tableWhitelist
	cfg.TableDenylist = tableDenylist
	cfg.FieldWhitelist = fieldWhitelist
	cfg.FieldDenylist = fieldDenylist
	svc.Config = cfg
	if err := manager.UpdateService(svc); err != nil {
		return err
	}
	fmt.Printf("Applied access control to service %q\n", serviceName)
	fmt.Printf("  table_whitelist: %v\n", cfg.TableWhitelist)
	fmt.Printf("  table_denylist: %v\n", cfg.TableDenylist)
	fmt.Printf("  field_whitelist: %v\n", cfg.FieldWhitelist)
	fmt.Printf("  field_denylist: %v\n", cfg.FieldDenylist)
	return nil
}

func printDatabaseAccessControlConfigExample() {
	example := integrations.DatabaseConfig{
		Driver:          "postgres",
		Host:            "localhost",
		Port:            5432,
		Database:        "app_db",
		SSLMode:         "disable",
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: "30m",
		ConnectTimeout:  "10s",
		ReadTimeout:     "5s",
		WriteTimeout:    "5s",
		PoolSize:        20,

		// Empty whitelist/denylist means no restriction.
		TableWhitelist: []string{"users", "orders"},
		TableDenylist:  []string{"audit_logs"},
		FieldWhitelist: []string{"users.id", "users.name", "orders.*"},
		FieldDenylist:  []string{"orders.internal_note"},
	}
	payload, _ := stdjson.MarshalIndent(example, "", "  ")
	fmt.Println("Database access-control config example:")
	fmt.Println(string(payload))
}

func runDatabaseIntegrationExamples(ctx context.Context, manager *integrations.Manager, serviceName, query string) {
	fmt.Printf("\nRunning examples for service %q\n", serviceName)

	if err := manager.ValidateDatabaseQuery(serviceName, query); err != nil {
		fmt.Printf("Validation failed for query %q: %v\n", query, err)
	} else {
		fmt.Printf("Validation passed for query %q\n", query)
	}

	result, err := manager.ExecuteDatabaseQuery(ctx, serviceName, query)
	if err != nil {
		fmt.Printf("Direct ExecuteDatabaseQuery failed: %v\n", err)
	} else {
		switch rows := result.(type) {
		case []map[string]any:
			fmt.Printf("Direct ExecuteDatabaseQuery returned %d row(s)\n", len(rows))
			if len(rows) > 0 {
				fmt.Printf("First row (post-filter): %+v\n", rows[0])
			}
		case map[string]any:
			fmt.Printf("Direct ExecuteDatabaseQuery response: %+v\n", rows)
		default:
			fmt.Printf("Direct ExecuteDatabaseQuery returned %T\n", result)
		}
	}

	tables, err := manager.ListDatabaseTables(ctx, serviceName)
	if err != nil {
		fmt.Printf("ListDatabaseTables failed: %v\n", err)
	} else {
		fmt.Printf("Visible tables (%d): %s\n", len(tables), strings.Join(tables, ", "))
		if len(tables) > 0 {
			fields, colErr := manager.ListDatabaseTableColumns(ctx, serviceName, tables[0])
			if colErr != nil {
				fmt.Printf("ListDatabaseTableColumns(%s) failed: %v\n", tables[0], colErr)
			} else {
				names := make([]string, 0, len(fields))
				for _, field := range fields {
					names = append(names, field.Name)
				}
				fmt.Printf("Visible columns for %s (%d): %s\n", tables[0], len(fields), strings.Join(names, ", "))
			}
		}
	}

	runtimeQuery := fmt.Sprintf("-- integration: %s\n%s", serviceName, query)
	runtimeRows, err := sql.Query(ctx, runtimeQuery, manager)
	if err != nil {
		fmt.Printf("sql.Query with integration directive failed: %v\n", err)
	} else {
		fmt.Printf("sql.Query with integration directive returned %d row(s)\n", len(runtimeRows))
	}
}

func testServices(ctx context.Context, manager *integrations.Manager) {
	services := map[string]any{
		"production-email": mail.Mail{
			To:   []string{"recipient@example.com"},
			Body: "Test email message",
		},
		"graphql-service":   "{ user { id name } }",
		"grpc-service":      "Test gRPC request",
		"kafka-service":     "Test Kafka message",
		"mqtt-service":      "Test MQTT message",
		"customtcp-service": "Test TCP message",
		"soap-service": `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
					<soapenv:Body>
						<ExampleRequest>
							<param>value</param>
						</ExampleRequest>
					</soapenv:Body>
				</soapenv:Envelope>`,
	}
	for service, payload := range services {
		if _, err := manager.Execute(ctx, service, payload); err != nil {
			manager.Logger().Error().Err(err).Str("service", service).Msg("Service execution failed")
		} else {
			manager.Logger().Info().Str("service", service).Msg("Service executed successfully")
		}
	}
}
