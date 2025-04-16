package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/smtp"
	"sync"
	"time"
)

// Service Types
type ServiceType string

const (
	ServiceTypeAPI  ServiceType = "api"
	ServiceTypeSMTP ServiceType = "smtp"
	ServiceTypeSMPP ServiceType = "smpp"
	ServiceTypeDB   ServiceType = "database"
)

// Credential Types
type CredentialType string

const (
	CredentialTypeAPIKey    CredentialType = "api_key"
	CredentialTypeBearer    CredentialType = "bearer"
	CredentialTypeBasicAuth CredentialType = "basic"
	CredentialTypeOAuth2    CredentialType = "oauth2"
	CredentialTypeSMTP      CredentialType = "smtp"
	CredentialTypeSMPP      CredentialType = "smpp"
	CredentialTypeDatabase  CredentialType = "database"
)

// Service Configuration Structs
type APIConfig struct {
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers"`
	RequestBody string            `json:"request_body"`
	ContentType string            `json:"content_type"`
	Timeout     time.Duration     `json:"timeout"`
}

type SMTPConfig struct {
	Server      string `json:"server"`
	Port        int    `json:"port"`
	From        string `json:"from"`
	UseTLS      bool   `json:"use_tls"`
	UseSTARTTLS bool   `json:"use_starttls"`
}

type SMPPConfig struct {
	SystemType string `json:"system_type"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	SourceAddr string `json:"source_addr"`
	DestAddr   string `json:"dest_addr"`
}

type DatabaseConfig struct {
	Driver   string `json:"driver"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	SSLMode  string `json:"ssl_mode"`
}

// Service Definition
type Service struct {
	Name          string      `json:"name"`
	Type          ServiceType `json:"type"`
	Config        interface{} `json:"config"`
	CredentialKey string      `json:"credential_key"`
	Enabled       bool        `json:"enabled"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

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
	default:
		return fmt.Errorf("unknown service type: %s", s.Type)
	}
	return nil
}

// Credential Definition
type Credential struct {
	Key         string         `json:"key"`
	Type        CredentialType `json:"type"`
	Data        interface{}    `json:"data"`
	Description string         `json:"description"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

func (c *Credential) UnmarshalJSON(data []byte) error {
	type Alias Credential
	aux := &struct {
		Data json.RawMessage `json:"data"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	switch c.Type {
	case CredentialTypeAPIKey:
		var d struct{ Key string }
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeBearer:
		var d struct{ Token string }
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeBasicAuth:
		var d struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeSMTP:
		var d struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeOAuth2:
		var d struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	default:
		return fmt.Errorf("unsupported credential type: %s", c.Type)
	}
	return nil
}

// Storage Interfaces
type ServiceStore interface {
	AddService(Service) error
	GetService(string) (Service, error)
	UpdateService(Service) error
	DeleteService(string) error
	ListServices() ([]Service, error)
}

type CredentialStore interface {
	AddCredential(Credential) error
	GetCredential(string) (Credential, error)
	UpdateCredential(Credential) error
	DeleteCredential(string) error
	ListCredentials() ([]Credential, error)
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
	if _, exists := s.services[service.Name]; !exists {
		return fmt.Errorf("service not found: %s", service.Name)
	}
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

// Complete InMemoryCredentialStore implementation
type InMemoryCredentialStore struct {
	credentials map[string]Credential
	mu          sync.RWMutex
}

func NewInMemoryCredentialStore() *InMemoryCredentialStore {
	return &InMemoryCredentialStore{
		credentials: make(map[string]Credential),
	}
}

func (c *InMemoryCredentialStore) AddCredential(cred Credential) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.credentials[cred.Key]; exists {
		return fmt.Errorf("credential already exists: %s", cred.Key)
	}
	c.credentials[cred.Key] = cred
	return nil
}

func (c *InMemoryCredentialStore) GetCredential(key string) (Credential, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cred, exists := c.credentials[key]
	if !exists {
		return Credential{}, fmt.Errorf("credential not found: %s", key)
	}
	return cred, nil
}

func (c *InMemoryCredentialStore) UpdateCredential(cred Credential) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.credentials[cred.Key]; !exists {
		return fmt.Errorf("credential not found: %s", cred.Key)
	}
	c.credentials[cred.Key] = cred
	return nil
}

func (c *InMemoryCredentialStore) DeleteCredential(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.credentials[key]; !exists {
		return fmt.Errorf("credential not found: %s", key)
	}
	delete(c.credentials, key)
	return nil
}

func (c *InMemoryCredentialStore) ListCredentials() ([]Credential, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	creds := make([]Credential, 0, len(c.credentials))
	for _, cred := range c.credentials {
		creds = append(creds, cred)
	}
	return creds, nil
}

// Integration System Core
type IntegrationSystem struct {
	services    ServiceStore
	credentials CredentialStore
}

func NewIntegrationSystem(serviceStore ServiceStore, credentialStore CredentialStore) *IntegrationSystem {
	return &IntegrationSystem{
		services:    serviceStore,
		credentials: credentialStore,
	}
}

// API Client Methods
func (is *IntegrationSystem) ExecuteAPIRequest(serviceName string, body []byte) (*http.Response, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}

	if service.Type != ServiceTypeAPI {
		return nil, fmt.Errorf("not an API service: %s", serviceName)
	}

	cfg, ok := service.Config.(APIConfig)
	if !ok {
		return nil, errors.New("invalid API configuration")
	}

	cred, err := is.credentials.GetCredential(service.CredentialKey)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(cfg.Method, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	// Add headers
	for k, v := range cfg.Headers {
		req.Header.Add(k, v)
	}

	// Add authentication
	switch cred.Type {
	case CredentialTypeAPIKey:
		data, ok := cred.Data.(struct{ Key string })
		if !ok {
			return nil, errors.New("invalid API key credential")
		}
		req.Header.Add("X-API-Key", data.Key)
	case CredentialTypeBearer:
		data, ok := cred.Data.(struct{ Token string })
		if !ok {
			return nil, errors.New("invalid bearer token credential")
		}
		req.Header.Add("Authorization", "Bearer "+data.Token)
	default:
		return nil, fmt.Errorf("unsupported credential type for API: %s", cred.Type)
	}

	client := &http.Client{Timeout: cfg.Timeout}
	return client.Do(req)
}

// SMTP Client Methods
func (is *IntegrationSystem) SendEmail(serviceName string, to []string, message []byte) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}

	if service.Type != ServiceTypeSMTP {
		return fmt.Errorf("not an SMTP service: %s", serviceName)
	}

	cfg, ok := service.Config.(SMTPConfig)
	if !ok {
		return errors.New("invalid SMTP configuration")
	}

	cred, err := is.credentials.GetCredential(service.CredentialKey)
	if err != nil {
		return err
	}

	authData, ok := cred.Data.(struct {
		Username string `json:"username"`
		Password string `json:"password"`
	})
	if !ok {
		return errors.New("invalid SMTP credential data")
	}

	auth := smtp.PlainAuth("", authData.Username, authData.Password, cfg.Server)

	if cfg.UseTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: false,
			ServerName:         cfg.Server,
		}

		conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", cfg.Server, cfg.Port), tlsConfig)
		if err != nil {
			return err
		}
		defer conn.Close()

		client, err := smtp.NewClient(conn, cfg.Server)
		if err != nil {
			return err
		}
		defer client.Close()

		if err = client.Auth(auth); err != nil {
			return err
		}

		if err = client.Mail(cfg.From); err != nil {
			return err
		}

		for _, addr := range to {
			if err = client.Rcpt(addr); err != nil {
				return err
			}
		}

		w, err := client.Data()
		if err != nil {
			return err
		}
		defer w.Close()

		_, err = w.Write(message)
		return err
	} else if cfg.UseSTARTTLS {
		client, err := smtp.Dial(fmt.Sprintf("%s:%d", cfg.Server, cfg.Port))
		if err != nil {
			return err
		}
		defer client.Close()

		tlsConfig := &tls.Config{
			InsecureSkipVerify: false,
			ServerName:         cfg.Server,
		}
		if err = client.StartTLS(tlsConfig); err != nil {
			return err
		}

		if err = client.Auth(auth); err != nil {
			return err
		}

		if err = client.Mail(cfg.From); err != nil {
			return err
		}

		for _, addr := range to {
			if err = client.Rcpt(addr); err != nil {
				return err
			}
		}

		w, err := client.Data()
		if err != nil {
			return err
		}
		defer w.Close()
		_, err = w.Write(message)
		return err
	}

	return smtp.SendMail(
		fmt.Sprintf("%s:%d", cfg.Server, cfg.Port),
		auth,
		cfg.From,
		to,
		message,
	)
}

// SMPP Client Methods
func (is *IntegrationSystem) ExecuteSMPPRequest(serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	if service.Type != ServiceTypeSMPP {
		return fmt.Errorf("not an SMPP service: %s", serviceName)
	}
	// Place holder: Cast to SMPPConfig and retrieve credentials if needed.
	// TODO: Implement actual SMPP integration.
	return fmt.Errorf("SMPP integration not implemented")
}

// Database Client Methods
func (is *IntegrationSystem) ExecuteDBQuery(serviceName string, query string) (interface{}, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	if service.Type != ServiceTypeDB {
		return nil, fmt.Errorf("not a Database service: %s", serviceName)
	}
	// Place holder: Cast to DatabaseConfig and retrieve credentials if needed.
	// TODO: Implement actual Database integration.
	return nil, fmt.Errorf("Database integration not implemented")
}

// Usage Example
func main() {
	// Initialize stores
	serviceStore := NewInMemoryServiceStore()
	credentialStore := NewInMemoryCredentialStore()
	integration := NewIntegrationSystem(serviceStore, credentialStore)

	// Add SMTP Credential
	err := credentialStore.AddCredential(Credential{
		Key:  "smtp-prod",
		Type: CredentialTypeSMTP,
		Data: struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}{
			Username: "user@example.com",
			Password: "securepassword",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	if err != nil {
		panic(err)
	}

	// Add SMTP Service
	err = serviceStore.AddService(Service{
		Name: "production-email",
		Type: ServiceTypeSMTP,
		Config: SMTPConfig{
			Server: "smtp.example.com",
			Port:   587,
			From:   "noreply@example.com",
			UseTLS: true,
		},
		CredentialKey: "smtp-prod",
		Enabled:       true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	})
	if err != nil {
		panic(err)
	}

	// Send email example
	err = integration.SendEmail("production-email", []string{"recipient@example.com"}, []byte("Test message"))
	if err != nil {
		panic(err)
	}
}
