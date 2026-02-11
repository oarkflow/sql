package integrations

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/oarkflow/log"
)

func (is *Manager) LoadIntegrationsFromFile(ctx context.Context, path string) (*Config, error) {
	cfg, err := is.Init(path)
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-reloadCh:
				is.logger.Info().Msg("Received SIGHUP: reloading configuration")
				if _, err := is.Init(path); err != nil {
					is.logger.Error().Err(err).Msg("Failed to reload configuration")
				} else {
					is.logger.Info().Msg("Configuration reloaded successfully")
				}
			case <-ctx.Done():
				is.logger.Info().Msg("Configuration reload goroutine exiting due to shutdown")
				return
			}
		}
	}()
	return cfg, err
}

func (is *Manager) UpdateCredential(c Credential) error {
	return is.credentials.UpdateCredential(c)
}

func (is *Manager) AddCredential(c Credential) error {
	return is.credentials.AddCredential(c)
}

func (is *Manager) DeleteCredential(key string) error {
	return is.credentials.DeleteCredential(key)
}

func (is *Manager) UpdateService(c Service) error {
	is.m.Lock()
	if db, ok := is.dbConnections[c.Name]; ok {
		_ = db.Close()
		delete(is.dbConnections, c.Name)
	}
	is.m.Unlock()
	return is.services.UpdateService(c)
}

func (is *Manager) AddService(c Service) error {
	return is.services.AddService(c)
}

func (is *Manager) Init(path string) (*Config, error) {
	cfg, err := loadConfig(path, is.logger)
	if err != nil {
		return nil, err
	}
	for _, cred := range cfg.Credentials {
		if err := is.UpdateCredential(cred); err != nil {
			_ = is.AddCredential(cred)
		}
	}
	for _, svc := range cfg.Services {
		if err := is.UpdateService(svc); err != nil {
			_ = is.AddService(svc)
		}
		if svc.Type == ServiceTypeAPI {
			if apiCfg, ok := svc.Config.(APIConfig); ok {
				is.AddCB(svc.Name, NewCircuitBreaker(apiCfg.CircuitBreakerThreshold))
			}
		}
	}
	return cfg, err
}

type Config struct {
	Credentials []Credential `json:"credentials"`
	Services    []Service    `json:"services"`
}

func loadConfig(path string, logger *log.Logger) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read config file")
		return nil, err
	}
	content := os.ExpandEnv(string(data))
	var cfg Config
	if err = json.Unmarshal([]byte(content), &cfg); err != nil {
		logger.Error().Err(err).Msg("failed to unmarshal config")
		return nil, err
	}
	return &cfg, nil
}
