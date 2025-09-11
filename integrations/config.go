package integrations

import (
	"context"
	"encoding/json"
	"fmt"
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
				is.Init(path)
				is.logger.Info().Msg("Configuration reloaded successfully")
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

func (is *Manager) UpdateService(c Service) error {
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

func UnmarshalService(svc *Service) error {
	switch svc.Type {
	case ServiceTypeAPI:
		b, _ := json.Marshal(svc.Config)
		var apiCfg APIConfig
		if err := json.Unmarshal(b, &apiCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := apiCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = apiCfg
	case ServiceTypeSMTP:
		b, _ := json.Marshal(svc.Config)
		var smtpCfg SMTPConfig
		if err := json.Unmarshal(b, &smtpCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := smtpCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = smtpCfg
	case ServiceTypeSMPP:
		b, _ := json.Marshal(svc.Config)
		var smppCfg SMPPConfig
		if err := json.Unmarshal(b, &smppCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := smppCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = smppCfg
	case ServiceTypeDB:
		b, _ := json.Marshal(svc.Config)
		var dbCfg DatabaseConfig
		if err := json.Unmarshal(b, &dbCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := dbCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = dbCfg
	case ServiceTypeGraphQL:
		b, _ := json.Marshal(svc.Config)
		var gqlCfg GraphQLConfig
		if err := json.Unmarshal(b, &gqlCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := gqlCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = gqlCfg
	case ServiceTypeSOAP:
		b, _ := json.Marshal(svc.Config)
		var soapCfg SOAPConfig
		if err := json.Unmarshal(b, &soapCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := soapCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = soapCfg
	case ServiceTypeGRPC:
		b, _ := json.Marshal(svc.Config)
		var grpcCfg GRPCConfig
		if err := json.Unmarshal(b, &grpcCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := grpcCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = grpcCfg
	case ServiceTypeKafka:
		b, _ := json.Marshal(svc.Config)
		var kafkaCfg KafkaConfig
		if err := json.Unmarshal(b, &kafkaCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := kafkaCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = kafkaCfg
	case ServiceTypeMQTT:
		b, _ := json.Marshal(svc.Config)
		var mqttCfg MQTTConfig
		if err := json.Unmarshal(b, &mqttCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := mqttCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = mqttCfg
	case ServiceTypeFTP:
		b, _ := json.Marshal(svc.Config)
		var ftpCfg FTPConfig
		if err := json.Unmarshal(b, &ftpCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := ftpCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = ftpCfg
	case ServiceTypeSFTP:
		b, _ := json.Marshal(svc.Config)
		var sftpCfg SFTPConfig
		if err := json.Unmarshal(b, &sftpCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := sftpCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = sftpCfg
	case ServiceTypePush:
		b, _ := json.Marshal(svc.Config)
		var pushCfg PushConfig
		if err := json.Unmarshal(b, &pushCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := pushCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = pushCfg
	case ServiceTypeSlack:
		b, _ := json.Marshal(svc.Config)
		var slackCfg SlackConfig
		if err := json.Unmarshal(b, &slackCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := slackCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = slackCfg
	case ServiceTypeCustomTCP:
		b, _ := json.Marshal(svc.Config)
		var tcpCfg CustomTCPConfig
		if err := json.Unmarshal(b, &tcpCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := tcpCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = tcpCfg
	case ServiceTypeVoIP:
		b, _ := json.Marshal(svc.Config)
		var voipCfg VoIPConfig
		if err := json.Unmarshal(b, &voipCfg); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		if err := voipCfg.Validate(); err != nil {
			return fmt.Errorf("service %s: %v", svc.Name, err)
		}
		svc.Config = voipCfg
	}
	return nil
}

func loadConfig(path string, logger *log.Logger) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read config file")
		return nil, err
	}
	var cfg Config
	if err = json.Unmarshal(data, &cfg); err != nil {
		logger.Error().Err(err).Msg("failed to unmarshal config")
		return nil, err
	}
	// Validate and convert service configurations.
	for i := range cfg.Services {
		if err := UnmarshalService(&cfg.Services[i]); err != nil {
			logger.Error().Err(err).Str("service", cfg.Services[i].Name).Msg("failed to unmarshal service config")
			return nil, err
		}
	}
	return &cfg, nil
}
