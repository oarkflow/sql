package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/oarkflow/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	Timeout                 string            `json:"timeout"` // Use string so we can parse duration
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
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("APIConfig: invalid timeout %v", err)
	}
	return nil
}

type SMTPConfig struct {
	Server            string `json:"server"`
	Port              int    `json:"port"`
	From              string `json:"from"`
	UseTLS            bool   `json:"use_tls"`
	UseSTARTTLS       bool   `json:"use_starttls"`
	ConnectionTimeout string `json:"connection_timeout"` // as string duration
	MaxConnections    int    `json:"max_connections"`
}

func (cfg SMTPConfig) Validate() error {
	if cfg.Server == "" {
		return errors.New("SMTPConfig: server must be provided")
	}
	if cfg.Port == 0 {
		return errors.New("SMTPConfig: port must be provided")
	}
	if _, err := time.ParseDuration(cfg.ConnectionTimeout); err != nil {
		return fmt.Errorf("SMTPConfig: invalid connection_timeout %v", err)
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
	Driver          string `json:"driver"`
	Host            string `json:"host"`
	Port            int    `json:"port"`
	Database        string `json:"database"`
	SSLMode         string `json:"ssl_mode"`
	MaxOpenConns    int    `json:"max_open_conns"`
	MaxIdleConns    int    `json:"max_idle_conns"`
	ConnMaxLifetime string `json:"conn_max_lifetime"` // as string duration
	ConnectTimeout  string `json:"connect_timeout"`   // as string duration
	ReadTimeout     string `json:"read_timeout"`      // as string duration
	WriteTimeout    string `json:"write_timeout"`     // as string duration
	PoolSize        int    `json:"pool_size"`
}

func (cfg DatabaseConfig) Validate() error {
	if cfg.Driver == "" {
		return errors.New("DatabaseConfig: driver must be provided")
	}
	if cfg.Host == "" || cfg.Database == "" {
		return errors.New("DatabaseConfig: host and database must be provided")
	}
	if _, err := time.ParseDuration(cfg.ConnMaxLifetime); err != nil {
		return fmt.Errorf("DatabaseConfig: invalid conn_max_lifetime %v", err)
	}
	if _, err := time.ParseDuration(cfg.ConnectTimeout); err != nil {
		return fmt.Errorf("DatabaseConfig: invalid connect_timeout %v", err)
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

type cbState int

const (
	Closed cbState = iota
	Open
	HalfOpen
)

type CircuitBreaker struct {
	failureCount int
	threshold    int
	state        cbState
	lastFailure  time.Time
	openDuration time.Duration
	lock         sync.Mutex
}

func NewCircuitBreaker(threshold int) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		state:        Closed,
		openDuration: 30 * time.Second,
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	now := time.Now()
	switch cb.state {
	case Closed:
		return true
	case Open:
		// If open, check if the cooldown has passed, then try half-open
		if now.Sub(cb.lastFailure) > cb.openDuration {
			cb.state = HalfOpen
			return true
		}
		return false
	case HalfOpen:
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount = 0
	cb.state = Closed
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.lock.Lock()
	defer cb.lock.Unlock()
	cb.failureCount++
	if cb.failureCount >= cb.threshold {
		cb.state = Open
		cb.lastFailure = time.Now()
	}
}

var (
	apiRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "integration_api_request_duration_seconds",
		Help:    "API request duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"service"})
	apiRequestFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "integration_api_request_failures_total",
		Help: "Total API request failures",
	}, []string{"service"})
)

func initMetrics() {
	prometheus.MustRegister(apiRequestDuration)
	prometheus.MustRegister(apiRequestFailures)
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

type EmailPayload struct {
	To      []string
	Message []byte
}

type IntegrationSystem struct {
	services        ServiceStore
	credentials     CredentialStore
	circuitBreakers map[string]*CircuitBreaker
	cbLock          sync.Mutex
	logger          *log.Logger
	wg              sync.WaitGroup
}

func NewIntegrationSystem(serviceStore ServiceStore, credentialStore CredentialStore, logger *log.Logger) *IntegrationSystem {
	return &IntegrationSystem{
		services:        serviceStore,
		credentials:     credentialStore,
		circuitBreakers: make(map[string]*CircuitBreaker),
		logger:          logger,
	}
}

func (is *IntegrationSystem) getCircuitBreaker(serviceName string, threshold int) *CircuitBreaker {
	is.cbLock.Lock()
	defer is.cbLock.Unlock()
	cb, exists := is.circuitBreakers[serviceName]
	if !exists {
		cb = NewCircuitBreaker(threshold)
		is.circuitBreakers[serviceName] = cb
	}
	return cb
}

func parseDuration(d string) time.Duration {
	dur, err := time.ParseDuration(d)
	if err != nil {
		return 10 * time.Second
	}
	return dur
}

func getHTTPClient(apiCfg APIConfig) *http.Client {
	return &http.Client{
		Timeout: parseDuration(apiCfg.Timeout),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: apiCfg.TLSInsecureSkipVerify},
		},
	}
}

func refreshOAuth2Token(auth *OAuth2Credential, logger *log.Logger) error {
	values := url.Values{}
	values.Set("grant_type", "refresh_token")
	values.Set("refresh_token", auth.RefreshToken)
	values.Set("client_id", auth.ClientID)
	values.Set("client_secret", auth.ClientSecret)

	resp, err := http.PostForm(auth.TokenURL, values)
	if err != nil {
		logger.Error().Err(err).Msg("failed to refresh OAuth2 token")
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
	logger.Info().Msg("OAuth2 token refreshed")
	return nil
}

func (is *IntegrationSystem) GetCredential(key string) (Credential, error) {
	cred, err := is.credentials.GetCredential(key)
	if err != nil {
		is.logger.Error().Str("key", key).Err(err).Msg("failed to get credential")
	}
	return cred, err
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
	cred, err := is.GetCredential(service.CredentialKey)
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
		data, ok := cred.Data.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid API key credential format")
		}
		if key, ok := data["key"].(string); ok {
			req.Header.Add("X-API-Key", key)
		} else {
			return nil, errors.New("missing API key value")
		}
	case CredentialTypeBearer:
		data, ok := cred.Data.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid bearer token credential format")
		}
		if token, ok := data["token"].(string); ok {
			req.Header.Add("Authorization", "Bearer "+token)
		} else {
			return nil, errors.New("missing bearer token value")
		}
	case CredentialTypeOAuth2:
		auth, ok := cred.Data.(*OAuth2Credential)
		if !ok {
			return nil, errors.New("invalid OAuth2 credential")
		}
		if auth.AccessToken == "" || time.Until(auth.ExpiresAt) < time.Minute {
			if err := refreshOAuth2Token(auth, is.logger); err != nil {
				return nil, fmt.Errorf("failed to refresh OAuth2 token: %w", err)
			}
		}
		req.Header.Add("Authorization", "Bearer "+auth.AccessToken)
	default:
		return nil, fmt.Errorf("unsupported credential type for API: %s", cred.Type)
	}
	start := time.Now()
	client := getHTTPClient(cfg)
	resp, err := client.Do(req)
	duration := time.Since(start)
	apiRequestDuration.WithLabelValues(serviceName).Observe(duration.Seconds())
	if err != nil {
		apiRequestFailures.WithLabelValues(serviceName).Inc()
	}
	return resp, err
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
		is.logger.Warn().Err(err).Int("attempt", i+1).Msg("API request failed, will retry")
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
	cred, err := is.GetCredential(service.CredentialKey)
	if err != nil {
		return err
	}
	authData, ok := cred.Data.(map[string]interface{})
	if !ok {
		return errors.New("invalid SMTP credential data")
	}
	username, uok := authData["username"].(string)
	password, pok := authData["password"].(string)
	if !uok || !pok {
		return errors.New("invalid SMTP credential fields")
	}
	auth := smtp.PlainAuth("", username, password, cfg.Server)
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
	fmt.Println(cfg)
	is.logger.Info().Str("service", serviceName).Str("message", message).Msg("Sending SMS")
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
	is.logger.Info().Str("driver", cfg.Driver).Str("connStr", connStr).Str("query", query).Msg("Executing database query")
	return "dummy result", nil
}

func (is *IntegrationSystem) Execute(ctx context.Context, serviceName string, payload any) (any, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	threshold := 3
	if service.Type == ServiceTypeAPI {
		if apiCfg, ok := service.Config.(APIConfig); ok && apiCfg.CircuitBreakerThreshold > 0 {
			threshold = apiCfg.CircuitBreakerThreshold
		}
	}
	cb := is.getCircuitBreaker(serviceName, threshold)
	if !cb.AllowRequest() {
		return nil, fmt.Errorf("service %s temporarily unavailable due to failures", serviceName)
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
			execErr = fmt.Errorf("invalid payload for DB service, expected query string")
			break
		}
		res, execErr = is.ExecuteDatabaseQuery(ctx, serviceName, query)
	default:
		execErr = fmt.Errorf("unsupported service type: %s", service.Type)
	}
	if execErr != nil {
		cb.RecordFailure()
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

func (is *IntegrationSystem) Shutdown(ctx context.Context) error {
	is.logger.Info().Msg("Shutting down integration system...")
	is.wg.Wait()
	return nil
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
		}
	}
	return &cfg, nil
}

func main() {
	logger := &log.DefaultLogger
	initMetrics()
	metricsSrv := &http.Server{Addr: ":9090", Handler: promhttp.Handler()}
	go func() {
		logger.Info().Msg("Starting metrics server on :9090")
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("Metrics server error")
		}
	}()
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()
	cfg, err := loadConfig(*configPath, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}
	serviceStore := NewInMemoryServiceStore()
	credentialStore := NewInMemoryCredentialStore()
	integration := NewIntegrationSystem(serviceStore, credentialStore, logger)
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
	var bgWG sync.WaitGroup
	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		for {
			<-reloadCh
			logger.Info().Msg("Received SIGHUP: reloading configuration")
			newCfg, err := loadConfig(*configPath, logger)
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
			integration.cbLock.Lock()
			for _, svc := range newCfg.Services {
				if svc.Type == ServiceTypeAPI {
					if apiCfg, ok := svc.Config.(APIConfig); ok {
						integration.circuitBreakers[svc.Name] = NewCircuitBreaker(apiCfg.CircuitBreakerThreshold)
					}
				}
			}
			integration.cbLock.Unlock()
			logger.Info().Msg("Configuration reloaded successfully")
		}
	}()
	if err := integration.HealthCheck(ctx); err != nil {
		logger.Error().Err(err).Msg("Health check failed")
	} else {
		logger.Info().Msg("All services are healthy")
	}
	integration.wg.Add(4)
	go func() {
		defer integration.wg.Done()
		emailPayload := EmailPayload{
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
		defer integration.wg.Done()
		apiPayload := []byte(`{"example": "data"}`)
		apiResult, err := integration.Execute(ctx, "some-api-service", apiPayload)
		if err != nil {
			logger.Error().Err(err).Msg("API request failed")
		} else if resp, ok := apiResult.(*http.Response); ok {
			logger.Info().Str("status", resp.Status).Msg("API request succeeded")
		}
	}()
	go func() {
		defer integration.wg.Done()
		if _, err := integration.Execute(ctx, "sms-service", "Test SMS message"); err != nil {
			logger.Error().Err(err).Msg("SMS operation failed")
		} else {
			logger.Info().Msg("SMS sent successfully")
		}
	}()
	go func() {
		defer integration.wg.Done()
		dbResult, err := integration.Execute(ctx, "prod-database", "SELECT * FROM users;")
		if err != nil {
			logger.Error().Err(err).Msg("Database query failed")
		} else {
			logger.Info().Any("result", dbResult).Msg("Database query result")
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
	bgWG.Wait()
	_ = integration.Shutdown(context.Background())
	logger.Info().Msg("Exiting integration system.")
}
