package main

import (
	"context"
	"flag"

	"github.com/oarkflow/log"

	"github.com/oarkflow/etl/integrations"
)

func main() {
	logger := &log.DefaultLogger
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()
	ctx := context.Background()
	manager := integrations.New(integrations.WithLogger(logger))
	_, err := manager.LoadConfig(ctx, *configPath)
	if err != nil {
		panic(err)
	}
	if err := manager.HealthCheck(ctx); err != nil {
		logger.Error().Err(err).Msg("Health check failed")
	} else {
		logger.Info().Msg("All services are healthy")
	}
	testServices(ctx, manager)
}

func testServices(ctx context.Context, manager *integrations.Manager) {
	services := map[string]any{
		"production-email": integrations.EmailPayload{
			To:      []string{"recipient@example.com"},
			Message: []byte("Test email message"),
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
