package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/oarkflow/log"
	"github.com/oarkflow/mail"

	"github.com/oarkflow/sql/integrations"
)

func main() {
	logger := &log.DefaultLogger
	ctx := context.Background()
	manager := integrations.New(integrations.WithLogger(logger))
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()
	_, err := manager.LoadIntegrationsFromFile(ctx, *configPath)
	if err != nil {
		panic(err)
	}
	service := "production-email"
	resp, err := manager.Execute(ctx, service, mail.Mail{
		To:   []string{"info@example.com"},
		Body: "This is a test message",
	})
	if err != nil {
		logger.Error().Err(err).Str("service", service).Msg("Service execution failed")
	} else {
		logger.Info().Str("service", service).Msg("Service executed successfully")
	}
	switch resp := resp.(type) {
	case *integrations.HTTPResponse:
		fmt.Println(string(resp.Body))
		for header, content := range resp.Headers {
			fmt.Println(fmt.Sprintf("%s: %v", header, content))
		}
		fmt.Println(resp.StatusCode)
	default:
		fmt.Println(resp)
	}
	// testServices(ctx, manager)
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
