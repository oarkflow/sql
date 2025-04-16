package integrations

import (
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/json"
)

type Service struct {
	Name          string      `json:"name"`
	Type          ServiceType `json:"type"`
	Config        any         `json:"config"`
	RequireAuth   bool        `json:"require_auth"`
	CredentialKey string      `json:"credential_key"`
	Enabled       bool        `json:"enabled"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

// UnmarshalJSON for Service dynamically decodes the configuration based on the service type.
func (s *Service) UnmarshalJSON(data []byte) error {
	type Alias Service
	aux := &struct {
		Config json.RawMessage `json:"config"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	switch s.Type {
	case ServiceTypeAPI:
		var cfg APIConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeSMTP:
		var cfg SMTPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeSMPP:
		var cfg SMPPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeDB:
		var cfg DatabaseConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeGraphQL:
		var cfg GraphQLConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeSOAP:
		var cfg SOAPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeGRPC:
		var cfg GRPCConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeKafka:
		var cfg KafkaConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeMQTT:
		var cfg MQTTConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeFTP:
		var cfg FTPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeSFTP:
		var cfg SFTPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypePush:
		var cfg PushConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeSlack:
		var cfg SlackConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeCustomTCP:
		var cfg CustomTCPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	case ServiceTypeVoIP:
		var cfg VoIPConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	// New case for WebCrawler integration.
	case ServiceTypeWebCrawler:
		var cfg WebCrawlerConfig
		if err := json.Unmarshal(aux.Config, &cfg); err != nil {
			return err
		}
		s.Config = cfg
	default:
		return fmt.Errorf("unknown service type: %s", s.Type)
	}
	return nil
}

type ServiceStore interface {
	AddService(Service) error
	GetService(string) (Service, error)
	UpdateService(Service) error
	DeleteService(string) error
	ListServices() ([]Service, error)
}

type InMemoryServiceStore struct {
	services map[string]Service
	mu       sync.RWMutex
}

func NewInMemoryServiceStore() *InMemoryServiceStore {
	return &InMemoryServiceStore{
		services: make(map[string]Service),
	}
}

func (s *InMemoryServiceStore) AddService(service Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.services[service.Name]; exists {
		return fmt.Errorf("service already exists: %s", service.Name)
	}
	s.services[service.Name] = service
	return nil
}

func (s *InMemoryServiceStore) GetService(name string) (Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	service, exists := s.services[name]
	if !exists {
		return Service{}, fmt.Errorf("service not found: %s", name)
	}
	return service, nil
}

func (s *InMemoryServiceStore) UpdateService(service Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[service.Name] = service
	return nil
}

func (s *InMemoryServiceStore) DeleteService(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.services[name]; !exists {
		return fmt.Errorf("service not found: %s", name)
	}
	delete(s.services, name)
	return nil
}

func (s *InMemoryServiceStore) ListServices() ([]Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	services := make([]Service, 0, len(s.services))
	for _, service := range s.services {
		services = append(services, service)
	}
	return services, nil
}
