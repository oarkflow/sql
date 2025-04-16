package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

type OAuth2Credential struct {
	ClientID     string    `json:"client_id"`
	ClientSecret string    `json:"client_secret"`
	AuthURL      string    `json:"auth_url"`
	TokenURL     string    `json:"token_url"`
	Scope        string    `json:"scope"`
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type APIConfig struct {
	URL                     string            `json:"url"`
	Method                  string            `json:"method"`
	Headers                 map[string]string `json:"headers"`
	RequestBody             string            `json:"request_body"`
	ContentType             string            `json:"content_type"`
	Timeout                 time.Duration     `json:"timeout"`
	TLSInsecureSkipVerify   bool              `json:"tls_insecure_skip_verify"`
	RetryCount              int               `json:"retry_count"`
	CircuitBreakerThreshold int               `json:"circuit_breaker_threshold"`
}

func (cfg APIConfig) Validate() error {
	if cfg.URL == "" {
		return errors.New("APIConfig: URL must be provided")
	}
	if cfg.Method == "" {
		return errors.New("APIConfig: HTTP method must be provided")
	}
	return nil
}

type SMTPConfig struct {
	Server            string        `json:"server"`
	Port              int           `json:"port"`
	From              string        `json:"from"`
	UseTLS            bool          `json:"use_tls"`
	UseSTARTTLS       bool          `json:"use_starttls"`
	ConnectionTimeout time.Duration `json:"connection_timeout"`
	MaxConnections    int           `json:"max_connections"`
}

func (cfg SMTPConfig) Validate() error {
	if cfg.Server == "" {
		return errors.New("SMTPConfig: server must be provided")
	}
	if cfg.Port == 0 {
		return errors.New("SMTPConfig: port must be provided")
	}
	return nil
}

type SMPPConfig struct {
	SystemType string `json:"system_type"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	SourceAddr string `json:"source_addr"`
	DestAddr   string `json:"dest_addr"`
	RetryCount int    `json:"retry_count"`
}

func (cfg SMPPConfig) Validate() error {
	if cfg.Host == "" {
		return errors.New("SMPPConfig: host must be provided")
	}
	if cfg.Port == 0 {
		return errors.New("SMPPConfig: port must be provided")
	}
	return nil
}

type DatabaseConfig struct {
	Driver          string        `json:"driver"`
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	Database        string        `json:"database"`
	SSLMode         string        `json:"ssl_mode"`
	MaxOpenConns    int           `json:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`
	ConnectTimeout  time.Duration `json:"connect_timeout"`
	ReadTimeout     time.Duration `json:"read_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout"`
	PoolSize        int           `json:"pool_size"`
}

func (cfg DatabaseConfig) Validate() error {
	if cfg.Driver == "" {
		return errors.New("DatabaseConfig: driver must be provided")
	}
	if cfg.Host == "" || cfg.Database == "" {
		return errors.New("DatabaseConfig: host and database must be provided")
	}
	return nil
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
	case CredentialTypeOAuth2:
		var d OAuth2Credential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = &d
	default:
		return fmt.Errorf("unsupported credential type: %s", c.Type)
	}
	return nil
}

func refreshOAuth2Token(auth *OAuth2Credential) error {
	values := url.Values{}
	values.Set("grant_type", "refresh_token")
	values.Set("refresh_token", auth.RefreshToken)
	values.Set("client_id", auth.ClientID)
	values.Set("client_secret", auth.ClientSecret)

	resp, err := http.PostForm(auth.TokenURL, values)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OAuth2 token refresh failed: %s", resp.Status)
	}
	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return err
	}
	auth.AccessToken = tokenResp.AccessToken
	if tokenResp.RefreshToken != "" {
		auth.RefreshToken = tokenResp.RefreshToken
	}
	auth.ExpiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
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

type CircuitBreaker struct {
	failureCount int
	threshold    int
	openUntil    time.Time
	lock         sync.Mutex
}

func NewCircuitBreaker(threshold int) *CircuitBreaker {
	return &CircuitBreaker{threshold: threshold}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	return time.Now().After(cb.openUntil)
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount = 0
}

func (cb *CircuitBreaker) RecordFailure(cooldown time.Duration) {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		cb.openUntil = time.Now().Add(cooldown)
	}
}

type Integrator interface {
	Execute(ctx context.Context, serviceName string, payload any) (any, error)
	HealthCheck(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type EmailPayload struct {
	To      []string
	Message []byte
}

type IntegrationSystem struct {
	services        ServiceStore
	credentials     CredentialStore
	circuitBreakers map[string]*CircuitBreaker
	cbLock          sync.Mutex
}

func NewIntegrationSystem(serviceStore ServiceStore, credentialStore CredentialStore) *IntegrationSystem {
	return &IntegrationSystem{
		services:        serviceStore,
		credentials:     credentialStore,
		circuitBreakers: make(map[string]*CircuitBreaker),
	}
}

func (is *IntegrationSystem) getCircuitBreaker(serviceName string) *CircuitBreaker {
	is.cbLock.Lock()
	defer is.cbLock.Unlock()
	cb, exists := is.circuitBreakers[serviceName]
	if !exists {
		threshold := 3 // default
		service, err := is.services.GetService(serviceName)
		if err == nil && service.Type == ServiceTypeAPI {
			if apiCfg, ok := service.Config.(APIConfig); ok && apiCfg.CircuitBreakerThreshold > 0 {
				threshold = apiCfg.CircuitBreakerThreshold
			}
		}
		cb = NewCircuitBreaker(threshold)
		is.circuitBreakers[serviceName] = cb
	}
	return cb
}

func getHTTPClient(apiCfg APIConfig) *http.Client {
	return &http.Client{
		Timeout: apiCfg.Timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: apiCfg.TLSInsecureSkipVerify},
		},
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

	cred, err := is.GetCredential(service.CredentialKey, service.Type)
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
	case CredentialTypeOAuth2:
		auth, ok := cred.Data.(*OAuth2Credential)
		if !ok {
			return nil, errors.New("invalid OAuth2 credential")
		}
		// Refresh if token is missing or about to expire within 1 minute.
		if auth.AccessToken == "" || time.Until(auth.ExpiresAt) < time.Minute {
			if err := refreshOAuth2Token(auth); err != nil {
				return nil, fmt.Errorf("failed to refresh OAuth2 token: %w", err)
			}
		}
		req.Header.Add("Authorization", "Bearer "+auth.AccessToken)
	default:
		return nil, fmt.Errorf("unsupported credential type for API: %s", cred.Type)
	}
	client := getHTTPClient(cfg)
	return client.Do(req)
}

func (is *IntegrationSystem) ExecuteAPIRequestWithRetry(ctx context.Context, serviceName string, body []byte, maxRetries int) (*http.Response, error) {
	var resp *http.Response
	var err error
	baseDelay := time.Second
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
		jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
		backoff := baseDelay*time.Duration(1<<i) + jitter
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

	cred, err := is.GetCredential(service.CredentialKey, service.Type)
	if err != nil {
		return err
	}
	authData, ok := cred.Data.(SMTPAuthCredential)
	if !ok {
		return errors.New("invalid SMTP credential data")
	}
	auth := smtp.PlainAuth("", authData.Username, authData.Password, cfg.Server)

	if cfg.UseTLS {
		tlsConfig := &tls.Config{InsecureSkipVerify: false, ServerName: cfg.Server}
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
	return smtp.SendMail(fmt.Sprintf("%s:%d", cfg.Server, cfg.Port), auth, cfg.From, to, message)
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
	cb := is.getCircuitBreaker(serviceName)
	if !cb.AllowRequest() {
		return nil, fmt.Errorf("service %s is temporarily unavailable due to repeated failures", serviceName)
	}
	var res any
	var execErr error
	switch service.Type {
	case ServiceTypeAPI:
		body, ok := payload.([]byte)
		if !ok {
			execErr = fmt.Errorf("invalid payload for API service, expected []byte")
			break
		}
		maxRetries := 3
		if apiCfg, ok := service.Config.(APIConfig); ok && apiCfg.RetryCount > 0 {
			maxRetries = apiCfg.RetryCount
		}
		res, execErr = is.ExecuteAPIRequestWithRetry(ctx, serviceName, body, maxRetries)
	case ServiceTypeSMTP:
		emailPayload, ok := payload.(EmailPayload)
		if !ok {
			execErr = fmt.Errorf("invalid payload for SMTP service, expected EmailPayload")
			break
		}
		execErr = is.SendEmail(ctx, serviceName, emailPayload.To, emailPayload.Message)
	case ServiceTypeSMPP:
		msg, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for SMPP service, expected string message")
			break
		}
		execErr = is.SendSMS(ctx, serviceName, msg)
	case ServiceTypeDB:
		query, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for DB service, expected string query")
			break
		}
		res, execErr = is.ExecuteDatabaseQuery(ctx, serviceName, query)
	default:
		execErr = fmt.Errorf("unsupported service type: %s", service.Type)
	}
	if execErr != nil {
		cb.RecordFailure(30 * time.Second)
	} else {
		cb.RecordSuccess()
	}
	return res, execErr
}

func (is *IntegrationSystem) HealthCheck(ctx context.Context) error {
	services, err := is.services.ListServices()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(services))
	for _, service := range services {
		wg.Add(1)
		go func(s Service) {
			defer wg.Done()
			switch s.Type {
			case ServiceTypeAPI:
				cfg, ok := s.Config.(APIConfig)
				if !ok {
					errCh <- fmt.Errorf("invalid API configuration for service %s", s.Name)
					return
				}
				req, err := http.NewRequestWithContext(ctx, "HEAD", cfg.URL, nil)
				if err != nil {
					errCh <- err
					return
				}
				client := getHTTPClient(cfg)
				resp, err := client.Do(req)
				if err != nil || resp.StatusCode >= 400 {
					errCh <- fmt.Errorf("health check failed for API service %s", s.Name)
				}
			case ServiceTypeSMTP:
				cfg, ok := s.Config.(SMTPConfig)
				if !ok {
					errCh <- fmt.Errorf("invalid SMTP configuration for service %s", s.Name)
					return
				}
				if cfg.UseTLS {
					tlsConfig := &tls.Config{InsecureSkipVerify: false, ServerName: cfg.Server}
					conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", cfg.Server, cfg.Port), tlsConfig)
					if err != nil {
						errCh <- fmt.Errorf("health check failed for SMTP service %s: %v", s.Name, err)
						return
					}
					conn.Close()
				}
			}
		}(service)
	}
	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		errMsg := "health check errors: "
		for e := range errCh {
			errMsg += e.Error() + "; "
		}
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (is *IntegrationSystem) GetCredential(key string, serviceType ServiceType) (Credential, error) {
	cred, err := is.credentials.GetCredential(key)
	if err != nil {
		log.Printf("Failed to get credential for service type %s: %v\n", serviceType, err)
	}
	return cred, err
}

func (is *IntegrationSystem) Shutdown(ctx context.Context) error {
	log.Println("Shutting down integration system...")
	return nil
}

type Config struct {
	Credentials []Credential `json:"credentials"`
	Services    []Service    `json:"services"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err = json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	// Validate each service's configuration.
	for i, svc := range cfg.Services {
		switch svc.Type {
		case ServiceTypeAPI:
			{
				b, _ := json.Marshal(svc.Config)
				var apiCfg APIConfig
				if err := json.Unmarshal(b, &apiCfg); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				if err := apiCfg.Validate(); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				cfg.Services[i].Config = apiCfg
			}
		case ServiceTypeSMTP:
			{
				b, _ := json.Marshal(svc.Config)
				var smtpCfg SMTPConfig
				if err := json.Unmarshal(b, &smtpCfg); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				if err := smtpCfg.Validate(); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				cfg.Services[i].Config = smtpCfg
			}
		case ServiceTypeSMPP:
			{
				b, _ := json.Marshal(svc.Config)
				var smppCfg SMPPConfig
				if err := json.Unmarshal(b, &smppCfg); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				if err := smppCfg.Validate(); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				cfg.Services[i].Config = smppCfg
			}
		case ServiceTypeDB:
			{
				b, _ := json.Marshal(svc.Config)
				var dbCfg DatabaseConfig
				if err := json.Unmarshal(b, &dbCfg); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				if err := dbCfg.Validate(); err != nil {
					return nil, fmt.Errorf("service %s: %v", svc.Name, err)
				}
				cfg.Services[i].Config = dbCfg
			}
		}
	}
	return &cfg, nil
}

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	serviceStore := NewInMemoryServiceStore()
	credentialStore := NewInMemoryCredentialStore()
	integration := NewIntegrationSystem(serviceStore, credentialStore)

	for _, cred := range cfg.Credentials {
		if err := credentialStore.AddCredential(cred); err != nil {
			log.Fatalf("Failed to add credential %s: %v", cred.Key, err)
		}
	}

	for _, svc := range cfg.Services {
		if err := serviceStore.AddService(svc); err != nil {
			log.Fatalf("Failed to add service %s: %v", svc.Name, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal: %v, initiating shutdown", sig)
		cancel()
		if err := integration.Shutdown(context.Background()); err != nil {
			log.Printf("Error during shutdown: %v", err)
		}
	}()

	// Listen for SIGHUP to support configuration reload without downtime.
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	go func() {
		for {
			<-reloadCh
			log.Println("Received SIGHUP: Reloading configuration...")
			newCfg, err := loadConfig(*configPath)
			if err != nil {
				log.Printf("Failed to reload config: %v", err)
				continue
			}
			// Update services.
			for _, svc := range newCfg.Services {
				if err := serviceStore.UpdateService(svc); err != nil {
					_ = serviceStore.AddService(svc)
				}
			}
			// Update credentials.
			for _, cred := range newCfg.Credentials {
				if err := credentialStore.UpdateCredential(cred); err != nil {
					_ = credentialStore.AddCredential(cred)
				}
			}
			// Update circuit breakers for API services with new thresholds.
			integration.cbLock.Lock()
			for _, svc := range newCfg.Services {
				if svc.Type == ServiceTypeAPI {
					if apiCfg, ok := svc.Config.(APIConfig); ok {
						integration.circuitBreakers[svc.Name] = NewCircuitBreaker(apiCfg.CircuitBreakerThreshold)
					}
				}
			}
			integration.cbLock.Unlock()
			log.Println("Configuration reloaded successfully.")
		}
	}()

	if err := integration.HealthCheck(ctx); err != nil {
		log.Printf("Health check failed: %v", err)
	} else {
		log.Println("All services are healthy.")
	}

	emailPayload := EmailPayload{
		To:      []string{"recipient@example.com"},
		Message: []byte("Test email message"),
	}
	if _, err := integration.Execute(ctx, "production-email", emailPayload); err != nil {
		log.Fatalf("Email operation failed: %v", err)
	}
	log.Println("Email sent successfully.")

	apiPayload := []byte(`{"example": "data"}`)
	apiResult, err := integration.Execute(ctx, "some-api-service", apiPayload)
	if err != nil {
		log.Printf("API request failed: %v", err)
	} else if resp, ok := apiResult.(*http.Response); ok {
		log.Printf("API request succeeded with status: %s", resp.Status)
	}

	if _, err := integration.Execute(ctx, "sms-service", "Test SMS message"); err != nil {
		log.Printf("SMS operation failed: %v", err)
	} else {
		log.Println("SMS sent successfully.")
	}

	dbResult, err := integration.Execute(ctx, "prod-database", "SELECT * FROM users;")
	if err != nil {
		log.Printf("Database query failed: %v", err)
	} else {
		log.Printf("Database query result: %v", dbResult)
	}

	<-ctx.Done()
	log.Println("Exiting integration system.")
}
