package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/oarkflow/etl/integrations"
)

func main() {
	logger := &log.DefaultLogger
	integrations.InitMetrics()

	metricsSrv := &http.Server{Addr: ":9090", Handler: promhttp.Handler()}
	go func() {
		logger.Info().Msg("Starting metrics server on :9090")
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Err(err).Msg("Metrics server error")
		}
	}()

	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()
	cfg, err := integrations.LoadConfig(*configPath, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}

	serviceStore := integrations.NewInMemoryServiceStore()
	credentialStore := integrations.NewInMemoryCredentialStore()
	integration := integrations.NewIntegrationSystem(serviceStore, credentialStore, logger)

	for _, cred := range cfg.Credentials {
		if err := credentialStore.AddCredential(cred); err != nil {
			logger.Fatal().Err(err).Str("key", cred.Key).Msg("Failed to add credential")
		}
	}
	for _, svc := range cfg.Services {
		if err := serviceStore.AddService(svc); err != nil {
			logger.Fatal().Err(err).Str("name", svc.Name).Msg("Failed to add service")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	signal.Notify(reloadCh, syscall.SIGHUP)

	// Configuration reload loop.
	integration.Wg.Add(1)
	go func() {
		defer integration.Wg.Done()
		for {
			select {
			case <-reloadCh:
				logger.Info().Msg("Received SIGHUP: reloading configuration")
				newCfg, err := integrations.LoadConfig(*configPath, logger)
				if err != nil {
					logger.Error().Err(err).Msg("Config reload failed")
					continue
				}
				for _, svc := range newCfg.Services {
					if err := serviceStore.UpdateService(svc); err != nil {
						_ = serviceStore.AddService(svc)
					}
				}
				for _, cred := range newCfg.Credentials {
					if err := credentialStore.UpdateCredential(cred); err != nil {
						_ = credentialStore.AddCredential(cred)
					}
				}
				integration.CbLock.Lock()
				for _, svc := range newCfg.Services {
					if svc.Type == integrations.ServiceTypeAPI {
						if apiCfg, ok := svc.Config.(integrations.APIConfig); ok {
							integration.CircuitBreakers[svc.Name] = integrations.NewCircuitBreaker(apiCfg.CircuitBreakerThreshold)
						}
					}
				}
				integration.CbLock.Unlock()
				logger.Info().Msg("Configuration reloaded successfully")
			case <-ctx.Done():
				logger.Info().Msg("Configuration reload goroutine exiting due to shutdown")
				return
			}
		}
	}()

	// Health check at startup.
	if err := integration.HealthCheck(ctx); err != nil {
		logger.Error().Err(err).Msg("Health check failed")
	} else {
		logger.Info().Msg("All services are healthy")
	}

	// Example operations running concurrently.
	integration.Wg.Add(8)
	go func() {
		defer integration.Wg.Done()
		emailPayload := integrations.EmailPayload{
			To:      []string{"recipient@example.com"},
			Message: []byte("Test email message"),
		}
		if _, err := integration.Execute(ctx, "production-email", emailPayload); err != nil {
			logger.Error().Err(err).Msg("Email operation failed")
		} else {
			logger.Info().Msg("Email sent successfully")
		}
	}()
	go func() {
		defer integration.Wg.Done()
		apiPayload := []byte(`{"example": "data"}`)
		apiResult, err := integration.Execute(ctx, "some-api-service", apiPayload)
		if err != nil {
			logger.Error().Err(err).Msg("API request failed")
		} else if resp, ok := apiResult.(*http.Response); ok {
			logger.Info().Str("status", resp.Status).Msg("API request succeeded")
		}
	}()
	go func() {
		defer integration.Wg.Done()
		// GraphQL call
		query := "{ user { id name } }"
		resp, err := integration.Execute(ctx, "graphql-service", query)
		if err != nil {
			logger.Error().Err(err).Msg("GraphQL request failed")
		} else if r, ok := resp.(*http.Response); ok {
			logger.Info().Str("status", r.Status).Msg("GraphQL request succeeded")
		}
	}()
	go func() {
		defer integration.Wg.Done()
		// SOAP call with an XML payload.
		soapReq := `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
						<soapenv:Body>
							<ExampleRequest>
								<param>value</param>
							</ExampleRequest>
						</soapenv:Body>
					</soapenv:Envelope>`
		resp, err := integration.Execute(ctx, "soap-service", []byte(soapReq))
		if err != nil {
			logger.Error().Err(err).Msg("SOAP request failed")
		} else if r, ok := resp.(*http.Response); ok {
			logger.Info().Str("status", r.Status).Msg("SOAP request succeeded")
		}
	}()
	go func() {
		defer integration.Wg.Done()
		// Simulated gRPC call
		resp, err := integration.Execute(ctx, "grpc-service", "gRPC request data")
		if err != nil {
			logger.Error().Err(err).Msg("gRPC request failed")
		} else {
			logger.Info().Msgf("gRPC response: %v", resp)
		}
	}()
	go func() {
		defer integration.Wg.Done()
		if err := integration.ExecuteKafkaMessage(ctx, "kafka-service", "Test Kafka message"); err != nil {
			logger.Error().Err(err).Msg("Kafka message failed")
		}
	}()
	go func() {
		defer integration.Wg.Done()
		if err := integration.ExecuteMQTTMessage(ctx, "mqtt-service", "Test MQTT message"); err != nil {
			logger.Error().Err(err).Msg("MQTT message failed")
		}
	}()
	go func() {
		defer integration.Wg.Done()
		// Custom TCP message
		if err := integration.ExecuteCustomTCPMessage(ctx, "customtcp-service", "Test TCP message"); err != nil {
			logger.Error().Err(err).Msg("Custom TCP message failed")
		}
	}()

	<-sigCh
	logger.Info().Msg("Received termination signal, shutting down...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("Metrics server shutdown failed")
	}
	integration.Wg.Wait()
	logger.Info().Msg("Exiting integration system.")
}
