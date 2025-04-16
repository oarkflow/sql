package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/oarkflow/json"
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
	service := "some-api-service"
	payload := map[string]any{
		"userId": 1,
		"id":     1000,
		"title":  "sunt aut facere repellat provident occaecati excepturi optio reprehenderit",
		"body":   "quia et suscipit\nsuscipit recusandae consequuntur expedita et cum\nreprehenderit molestiae ut ut quas totam\nnostrum rerum est autem sunt rem eveniet architecto",
	}
	bt, err := json.Marshal(payload)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to marshal payload")
		return
	}
	resp, err := manager.Execute(ctx, service, bt)
	if err != nil {
		logger.Error().Err(err).Str("service", service).Msg("Service execution failed")
	} else {
		logger.Info().Str("service", service).Msg("Service executed successfully")
	}
	switch resp := resp.(type) {
	case *integrations.HTTPResponse:
		fmt.Println(string(resp.Body))
	}
	// testServices(ctx, manager)
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
