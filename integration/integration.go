package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/smtp"
	"sync"
	"time"
)

type ServiceType string
type CredentialType string

const (
	ServiceTypeAPI  ServiceType = "api"
	ServiceTypeSMTP ServiceType = "smtp"
	ServiceTypeSMPP ServiceType = "smpp"
	ServiceTypeDB   ServiceType = "database"

	CredentialTypeAPIKey    CredentialType = "api_key"
	CredentialTypeBearer    CredentialType = "bearer"
	CredentialTypeBasicAuth CredentialType = "basic"
	CredentialTypeOAuth2    CredentialType = "oauth2"
	CredentialTypeSMTP      CredentialType = "smtp"
	CredentialTypeSMPP      CredentialType = "smpp"
	CredentialTypeDatabase  CredentialType = "database"
)

type APIKeyCredential struct {
	Key string `json:"key"`
}

type BearerCredential struct {
	Token string `json:"token"`
}

type BasicAuthCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SMTPAuthCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

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

type Service struct {
	Name          string      `json:"name"`
	Type          ServiceType `json:"type"`
	Config        any         `json:"config"`
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

type Credential struct {
	Key         string         `json:"key"`
	Type        CredentialType `json:"type"`
	Data        any            `json:"data"`
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
		var d APIKeyCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeBearer:
		var d BearerCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeBasicAuth, CredentialTypeSMTP:
		var d SMTPAuthCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	default:
		return fmt.Errorf("unsupported credential type: %s", c.Type)
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

type IntegrationExecutor interface {
	Execute(ctx context.Context, serviceName string, payload any) (any, error)
}

type EmailPayload struct {
	To      []string
	Message []byte
}

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

func (is *IntegrationSystem) ExecuteAPIRequest(ctx context.Context, serviceName string, body []byte) (*http.Response, error) {
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

	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	for k, v := range cfg.Headers {
		req.Header.Add(k, v)
	}

	switch cred.Type {
	case CredentialTypeAPIKey:
		data, ok := cred.Data.(APIKeyCredential)
		if !ok {
			return nil, errors.New("invalid API key credential")
		}
		req.Header.Add("X-API-Key", data.Key)
	case CredentialTypeBearer:
		data, ok := cred.Data.(BearerCredential)
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

func (is *IntegrationSystem) ExecuteAPIRequestWithRetry(ctx context.Context, serviceName string, body []byte, maxRetries int) (*http.Response, error) {
	var resp *http.Response
	var err error

	for i := 0; i < maxRetries; i++ {
		resp, err = is.ExecuteAPIRequest(ctx, serviceName, body)
		if err == nil {
			return resp, nil
		}
		log.Printf("API request attempt %d/%d failed: %v", i+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		backoff := time.Duration(math.Pow(2, float64(i))) * time.Second
		time.Sleep(backoff)
	}
	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, err)
}

func (is *IntegrationSystem) SendEmail(ctx context.Context, serviceName string, to []string, message []byte) error {
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

	authData, ok := cred.Data.(SMTPAuthCredential)
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
	}

	return smtp.SendMail(
		fmt.Sprintf("%s:%d", cfg.Server, cfg.Port),
		auth,
		cfg.From,
		to,
		message,
	)
}

func (is *IntegrationSystem) SendSMS(ctx context.Context, serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}

	if service.Type != ServiceTypeSMPP {
		return fmt.Errorf("not an SMPP service: %s", serviceName)
	}

	cfg, ok := service.Config.(SMPPConfig)
	if !ok {
		return errors.New("invalid SMPP configuration")
	}

	log.Printf("SMPP: Sending SMS via %s:%d using system type %s\nMessage: %s", cfg.Host, cfg.Port, cfg.SystemType, message)

	return nil
}

func (is *IntegrationSystem) ExecuteDatabaseQuery(ctx context.Context, serviceName, query string) (any, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}

	if service.Type != ServiceTypeDB {
		return nil, fmt.Errorf("not a database service: %s", serviceName)
	}

	cfg, ok := service.Config.(DatabaseConfig)
	if !ok {
		return nil, errors.New("invalid Database configuration")
	}

	connStr := fmt.Sprintf("%s:%d/%s?sslmode=%s", cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)
	log.Printf("DB: Connecting to %s with connection string: %s", cfg.Driver, connStr)

	log.Printf("DB: Executing query: %s", query)
	return "dummy result", nil
}

func (is *IntegrationSystem) Execute(ctx context.Context, serviceName string, payload any) (any, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}

	switch service.Type {
	case ServiceTypeAPI:
		body, ok := payload.([]byte)
		if !ok {
			return nil, fmt.Errorf("invalid payload for API service, expected []byte")
		}
		return is.ExecuteAPIRequestWithRetry(ctx, serviceName, body, 3)
	case ServiceTypeSMTP:
		emailPayload, ok := payload.(EmailPayload)
		if !ok {
			return nil, fmt.Errorf("invalid payload for SMTP service, expected EmailPayload")
		}
		err := is.SendEmail(ctx, serviceName, emailPayload.To, emailPayload.Message)
		return nil, err
	case ServiceTypeSMPP:
		msg, ok := payload.(string)
		if !ok {
			return nil, fmt.Errorf("invalid payload for SMPP service, expected string message")
		}
		return nil, is.SendSMS(ctx, serviceName, msg)
	case ServiceTypeDB:
		query, ok := payload.(string)
		if !ok {
			return nil, fmt.Errorf("invalid payload for DB service, expected string query")
		}
		return is.ExecuteDatabaseQuery(ctx, serviceName, query)
	default:
		return nil, fmt.Errorf("unsupported service type: %s", service.Type)
	}
}

func main() {

	serviceStore := NewInMemoryServiceStore()
	credentialStore := NewInMemoryCredentialStore()
	integration := NewIntegrationSystem(serviceStore, credentialStore)

	if err := credentialStore.AddCredential(Credential{
		Key:  "smtp-prod",
		Type: CredentialTypeSMTP,
		Data: SMTPAuthCredential{
			Username: "user@example.com",
			Password: "securepassword",
		},
		Description: "SMTP production credentials",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}); err != nil {
		log.Fatalf("Failed to add SMTP credential: %v", err)
	}

	if err := serviceStore.AddService(Service{
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
	}); err != nil {
		log.Fatalf("Failed to add SMTP service: %v", err)
	}

	if err := credentialStore.AddCredential(Credential{
		Key:  "api-key-1",
		Type: CredentialTypeAPIKey,
		Data: APIKeyCredential{
			Key: "my-secure-api-key",
		},
		Description: "API key for service 1",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}); err != nil {
		log.Fatalf("Failed to add API credential: %v", err)
	}

	if err := serviceStore.AddService(Service{
		Name: "some-api-service",
		Type: ServiceTypeAPI,
		Config: APIConfig{
			URL:     "https://api.example.com/endpoint",
			Method:  "POST",
			Headers: map[string]string{"Content-Type": "application/json"},
			Timeout: 5 * time.Second,
		},
		CredentialKey: "api-key-1",
		Enabled:       true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}); err != nil {
		log.Fatalf("Failed to add API service: %v", err)
	}

	if err := serviceStore.AddService(Service{
		Name: "sms-service",
		Type: ServiceTypeSMPP,
		Config: SMPPConfig{
			SystemType: "default",
			Host:       "smpp.example.com",
			Port:       2775,
			SourceAddr: "SENDER",
			DestAddr:   "RECIPIENT",
		},
		CredentialKey: "",
		Enabled:       true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}); err != nil {
		log.Fatalf("Failed to add SMPP service: %v", err)
	}

	if err := serviceStore.AddService(Service{
		Name: "prod-database",
		Type: ServiceTypeDB,
		Config: DatabaseConfig{
			Driver:   "sqlite3",
			Host:     "localhost",
			Port:     0,
			Database: "prod.db",
			SSLMode:  "disable",
		},
		CredentialKey: "",
		Enabled:       true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}); err != nil {
		log.Fatalf("Failed to add Database service: %v", err)
	}

	ctx := context.Background()

	emailPayload := EmailPayload{
		To:      []string{"recipient@example.com"},
		Message: []byte("Test email message"),
	}
	if _, err := integration.Execute(ctx, "production-email", emailPayload); err != nil {
		log.Fatalf("Failed to execute email operation: %v", err)
	}
	log.Println("Email sent successfully.")

	apiPayload := []byte(`{"example": "data"}`)
	apiResult, err := integration.Execute(ctx, "some-api-service", apiPayload)
	if err != nil {
		log.Printf("API request failed: %v", err)
	} else {
		if resp, ok := apiResult.(*http.Response); ok {
			log.Printf("API request succeeded with status: %s", resp.Status)
		}
	}

	if _, err := integration.Execute(ctx, "sms-service", "Test SMS message"); err != nil {
		log.Printf("SMS sending failed: %v", err)
	} else {
		log.Println("SMS sent successfully.")
	}

	dbResult, err := integration.Execute(ctx, "prod-database", "SELECT * FROM users;")
	if err != nil {
		log.Printf("Database query failed: %v", err)
	} else {
		log.Printf("Database query result: %v", dbResult)
	}

}
