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

func (is *Manager) LoadConfig(ctx context.Context, path string) (*Config, error) {
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

func (is *Manager) Init(path string) (*Config, error) {
	cfg, err := loadConfig(path, is.logger)
	if err != nil {
		return nil, err
	}
	for _, cred := range cfg.Credentials {
		if err := is.credentials.UpdateCredential(cred); err != nil {
			_ = is.credentials.AddCredential(cred)
		}
	}
	for _, svc := range cfg.Services {
		if err := is.services.UpdateService(svc); err != nil {
			_ = is.services.AddService(svc)
		}
	}
	for _, svc := range cfg.Services {
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
	var cfg Config
	if err = json.Unmarshal(data, &cfg); err != nil {
		logger.Error().Err(err).Msg("failed to unmarshal config")
		return nil, err
	}
	// Validate and convert service configurations.
	for i, svc := range cfg.Services {
		switch svc.Type {
		case ServiceTypeAPI:
			b, _ := json.Marshal(svc.Config)
			var apiCfg APIConfig
			if err := json.Unmarshal(b, &apiCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := apiCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = apiCfg
		case ServiceTypeSMTP:
			b, _ := json.Marshal(svc.Config)
			var smtpCfg SMTPConfig
			if err := json.Unmarshal(b, &smtpCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := smtpCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = smtpCfg
		case ServiceTypeSMPP:
			b, _ := json.Marshal(svc.Config)
			var smppCfg SMPPConfig
			if err := json.Unmarshal(b, &smppCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := smppCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = smppCfg
		case ServiceTypeDB:
			b, _ := json.Marshal(svc.Config)
			var dbCfg DatabaseConfig
			if err := json.Unmarshal(b, &dbCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := dbCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = dbCfg
		case ServiceTypeGraphQL:
			b, _ := json.Marshal(svc.Config)
			var gqlCfg GraphQLConfig
			if err := json.Unmarshal(b, &gqlCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := gqlCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = gqlCfg
		case ServiceTypeSOAP:
			b, _ := json.Marshal(svc.Config)
			var soapCfg SOAPConfig
			if err := json.Unmarshal(b, &soapCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := soapCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = soapCfg
		case ServiceTypeGRPC:
			b, _ := json.Marshal(svc.Config)
			var grpcCfg GRPCConfig
			if err := json.Unmarshal(b, &grpcCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := grpcCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = grpcCfg
		case ServiceTypeKafka:
			b, _ := json.Marshal(svc.Config)
			var kafkaCfg KafkaConfig
			if err := json.Unmarshal(b, &kafkaCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := kafkaCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = kafkaCfg
		case ServiceTypeMQTT:
			b, _ := json.Marshal(svc.Config)
			var mqttCfg MQTTConfig
			if err := json.Unmarshal(b, &mqttCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := mqttCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = mqttCfg
		case ServiceTypeFTP:
			b, _ := json.Marshal(svc.Config)
			var ftpCfg FTPConfig
			if err := json.Unmarshal(b, &ftpCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := ftpCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = ftpCfg
		case ServiceTypeSFTP:
			b, _ := json.Marshal(svc.Config)
			var sftpCfg SFTPConfig
			if err := json.Unmarshal(b, &sftpCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := sftpCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = sftpCfg
		case ServiceTypePush:
			b, _ := json.Marshal(svc.Config)
			var pushCfg PushConfig
			if err := json.Unmarshal(b, &pushCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := pushCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = pushCfg
		case ServiceTypeSlack:
			b, _ := json.Marshal(svc.Config)
			var slackCfg SlackConfig
			if err := json.Unmarshal(b, &slackCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := slackCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = slackCfg
		case ServiceTypeCustomTCP:
			b, _ := json.Marshal(svc.Config)
			var tcpCfg CustomTCPConfig
			if err := json.Unmarshal(b, &tcpCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := tcpCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = tcpCfg
		case ServiceTypeVoIP:
			b, _ := json.Marshal(svc.Config)
			var voipCfg VoIPConfig
			if err := json.Unmarshal(b, &voipCfg); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			if err := voipCfg.Validate(); err != nil {
				return nil, fmt.Errorf("service %s: %v", svc.Name, err)
			}
			cfg.Services[i].Config = voipCfg
		}
	}
	return &cfg, nil
}
