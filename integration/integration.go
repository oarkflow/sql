package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
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

//
// ==== COMMON TYPES & CONSTANTS ====
//

// ServiceType represents the type of service.
type ServiceType string

// CredentialType represents the type of credential.
type CredentialType string

// Existing service types.
const (
	ServiceTypeAPI  ServiceType = "api"
	ServiceTypeSMTP ServiceType = "smtp"
	ServiceTypeSMPP ServiceType = "smpp"
	ServiceTypeDB   ServiceType = "database"
	// New additional service types:
	ServiceTypeGraphQL   ServiceType = "graphql"
	ServiceTypeSOAP      ServiceType = "soap"
	ServiceTypeGRPC      ServiceType = "grpc"
	ServiceTypeKafka     ServiceType = "kafka"
	ServiceTypeMQTT      ServiceType = "mqtt"
	ServiceTypeFTP       ServiceType = "ftp"
	ServiceTypeSFTP      ServiceType = "sftp"
	ServiceTypePush      ServiceType = "push"
	ServiceTypeSlack     ServiceType = "slack"
	ServiceTypeCustomTCP ServiceType = "custom_tcp"
	ServiceTypeVoIP      ServiceType = "voip"
)

// Credential types.
const (
	CredentialTypeAPIKey    CredentialType = "api_key"
	CredentialTypeBearer    CredentialType = "bearer"
	CredentialTypeBasicAuth CredentialType = "basic"
	CredentialTypeOAuth2    CredentialType = "oauth2"
	CredentialTypeSMTP      CredentialType = "smtp"
	CredentialTypeSMPP      CredentialType = "smpp"
	CredentialTypeDatabase  CredentialType = "database"
)

//
// ==== CREDENTIAL CONFIGURATIONS ====
//

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

type SMPPCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type DatabaseCredential struct {
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

//
// ==== SERVICE CONFIGURATIONS ====
//

// APIConfig supports HTTP REST services.
type APIConfig struct {
	URL                     string            `json:"url"`
	Method                  string            `json:"method"`
	Headers                 map[string]string `json:"headers"`
	RequestBody             string            `json:"request_body"`
	ContentType             string            `json:"content_type"`
	Timeout                 string            `json:"timeout"` // e.g., "10s"
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
		return fmt.Errorf("APIConfig: invalid timeout: %v", err)
	}
	return nil
}

// SMTPConfig for sending emails.
type SMTPConfig struct {
	Server            string `json:"server"`
	Port              int    `json:"port"`
	From              string `json:"from"`
	UseTLS            bool   `json:"use_tls"`
	UseSTARTTLS       bool   `json:"use_starttls"`
	ConnectionTimeout string `json:"connection_timeout"`
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
		return fmt.Errorf("SMTPConfig: invalid connection_timeout: %v", err)
	}
	return nil
}

// SMPPConfig for SMS messaging.
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

// DatabaseConfig for relational databases.
type DatabaseConfig struct {
	Driver          string `json:"driver"`
	Host            string `json:"host"`
	Port            int    `json:"port"`
	Database        string `json:"database"`
	SSLMode         string `json:"ssl_mode"`
	MaxOpenConns    int    `json:"max_open_conns"`
	MaxIdleConns    int    `json:"max_idle_conns"`
	ConnMaxLifetime string `json:"conn_max_lifetime"`
	ConnectTimeout  string `json:"connect_timeout"`
	ReadTimeout     string `json:"read_timeout"`
	WriteTimeout    string `json:"write_timeout"`
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
		return fmt.Errorf("DatabaseConfig: invalid conn_max_lifetime: %v", err)
	}
	if _, err := time.ParseDuration(cfg.ConnectTimeout); err != nil {
		return fmt.Errorf("DatabaseConfig: invalid connect_timeout: %v", err)
	}
	return nil
}

// GraphQLConfig for GraphQL endpoints.
type GraphQLConfig struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Timeout string            `json:"timeout"`
}

func (cfg GraphQLConfig) Validate() error {
	if cfg.URL == "" {
		return errors.New("GraphQLConfig: URL must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("GraphQLConfig: invalid timeout: %v", err)
	}
	return nil
}

// SOAPConfig for SOAP-based web services.
type SOAPConfig struct {
	URL        string `json:"url"`
	SOAPAction string `json:"soap_action"`
	Timeout    string `json:"timeout"`
}

func (cfg SOAPConfig) Validate() error {
	if cfg.URL == "" {
		return errors.New("SOAPConfig: URL must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("SOAPConfig: invalid timeout: %v", err)
	}
	return nil
}

// GRPCConfig for gRPC services.
type GRPCConfig struct {
	Address string `json:"address"`
	Timeout string `json:"timeout"`
}

func (cfg GRPCConfig) Validate() error {
	if cfg.Address == "" {
		return errors.New("GRPCConfig: address must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("GRPCConfig: invalid timeout: %v", err)
	}
	return nil
}

// KafkaConfig for message queue services.
type KafkaConfig struct {
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	Timeout string   `json:"timeout"`
}

func (cfg KafkaConfig) Validate() error {
	if len(cfg.Brokers) == 0 {
		return errors.New("KafkaConfig: at least one broker must be provided")
	}
	if cfg.Topic == "" {
		return errors.New("KafkaConfig: topic must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("KafkaConfig: invalid timeout: %v", err)
	}
	return nil
}

// MQTTConfig for IoT messaging.
type MQTTConfig struct {
	Server   string `json:"server"`
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
	Timeout  string `json:"timeout"`
}

func (cfg MQTTConfig) Validate() error {
	if cfg.Server == "" {
		return errors.New("MQTTConfig: server must be provided")
	}
	if cfg.ClientID == "" {
		return errors.New("MQTTConfig: clientID must be provided")
	}
	if cfg.Topic == "" {
		return errors.New("MQTTConfig: topic must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("MQTTConfig: invalid timeout: %v", err)
	}
	return nil
}

// FTPConfig for FTP file transfers.
type FTPConfig struct {
	Server   string `json:"server"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Timeout  string `json:"timeout"`
}

func (cfg FTPConfig) Validate() error {
	if cfg.Server == "" {
		return errors.New("FTPConfig: server must be provided")
	}
	if cfg.Port == 0 {
		return errors.New("FTPConfig: port must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("FTPConfig: invalid timeout: %v", err)
	}
	return nil
}

// SFTPConfig for SFTP file transfers.
type SFTPConfig struct {
	Server   string `json:"server"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"` // In production use key-based authentication.
	Timeout  string `json:"timeout"`
}

func (cfg SFTPConfig) Validate() error {
	if cfg.Server == "" {
		return errors.New("SFTPConfig: server must be provided")
	}
	if cfg.Port == 0 {
		return errors.New("SFTPConfig: port must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("SFTPConfig: invalid timeout: %v", err)
	}
	return nil
}

// PushConfig for push notifications.
type PushConfig struct {
	Provider string `json:"provider"`
	APIKey   string `json:"api_key"`
	Endpoint string `json:"endpoint"`
	Timeout  string `json:"timeout"`
}

func (cfg PushConfig) Validate() error {
	if cfg.Provider == "" {
		return errors.New("PushConfig: provider must be provided")
	}
	if cfg.APIKey == "" {
		return errors.New("PushConfig: API key must be provided")
	}
	if cfg.Endpoint == "" {
		return errors.New("PushConfig: endpoint must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("PushConfig: invalid timeout: %v", err)
	}
	return nil
}

// SlackConfig for Slack messaging.
type SlackConfig struct {
	WebhookURL string `json:"webhook_url"`
	Channel    string `json:"channel"`
	Timeout    string `json:"timeout"`
}

func (cfg SlackConfig) Validate() error {
	if cfg.WebhookURL == "" {
		return errors.New("SlackConfig: webhook URL must be provided")
	}
	if cfg.Channel == "" {
		return errors.New("SlackConfig: channel must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("SlackConfig: invalid timeout: %v", err)
	}
	return nil
}

// CustomTCPConfig for raw TCP communications.
type CustomTCPConfig struct {
	Address string `json:"address"`
	Timeout string `json:"timeout"`
}

func (cfg CustomTCPConfig) Validate() error {
	if cfg.Address == "" {
		return errors.New("CustomTCPConfig: address must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("CustomTCPConfig: invalid timeout: %v", err)
	}
	return nil
}

// VoIPConfig for SIP/VoIP communications.
type VoIPConfig struct {
	SIPServer string `json:"sip_server"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Timeout   string `json:"timeout"`
}

func (cfg VoIPConfig) Validate() error {
	if cfg.SIPServer == "" {
		return errors.New("VoIPConfig: SIP server must be provided")
	}
	if cfg.Username == "" || cfg.Password == "" {
		return errors.New("VoIPConfig: username and password must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("VoIPConfig: invalid timeout: %v", err)
	}
	return nil
}

//
// ==== SERVICE & CREDENTIAL STRUCTS ====
//

type Service struct {
	Name          string      `json:"name"`
	Type          ServiceType `json:"type"`
	Config        any         `json:"config"`
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
	default:
		return fmt.Errorf("unknown service type: %s", s.Type)
	}
	return nil
}

// Credential struct holds credential information.
type Credential struct {
	Key         string         `json:"key"`
	Type        CredentialType `json:"type"`
	Data        any            `json:"data"`
	Description string         `json:"description"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// UnmarshalJSON for Credential decodes the data field based on the credential type.
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
	case CredentialTypeSMPP:
		var d SMPPCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	case CredentialTypeDatabase:
		var d DatabaseCredential
		if err := json.Unmarshal(aux.Data, &d); err != nil {
			return err
		}
		c.Data = d
	default:
		return fmt.Errorf("unsupported credential type: %s", c.Type)
	}
	return nil
}

//
// ==== CIRCUIT BREAKER ====
//

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

//
// ==== METRICS ====
//

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

//
// ==== SERVICE & CREDENTIAL STORES ====
//

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

//
// ==== INTEGRATION SYSTEM ====
//

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

func getHTTPClient(timeout string, insecure bool) *http.Client {
	return &http.Client{
		Timeout: parseDuration(timeout),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}
}

//
// ==== EXECUTION FUNCTIONS ====
//

// ExecuteAPIRequest handles typical REST API calls.
func (is *IntegrationSystem) ExecuteAPIRequest(ctx context.Context, serviceName string, body []byte) (*http.Response, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
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
	// Handle credentials for API
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
	client := getHTTPClient(cfg.Timeout, cfg.TLSInsecureSkipVerify)
	resp, err := client.Do(req)
	duration := time.Since(start)
	apiRequestDuration.WithLabelValues(serviceName).Observe(duration.Seconds())
	if err != nil {
		apiRequestFailures.WithLabelValues(serviceName).Inc()
	}
	return resp, err
}

// ExecuteAPIRequestWithRetry uses context-aware backoff.
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
		jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
		backoff := baseDelay*time.Duration(1<<i) + jitter
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, err)
}

// ExecuteGraphQLRequest is similar to APIRequest but for GraphQL.
func (is *IntegrationSystem) ExecuteGraphQLRequest(ctx context.Context, serviceName string, query string) (*http.Response, error) {
	// For GraphQL, we use HTTP POST with a JSON payload
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(GraphQLConfig)
	if !ok {
		return nil, errors.New("invalid GraphQL configuration")
	}
	reqBody := map[string]string{"query": query}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	client := getHTTPClient(cfg.Timeout, false)
	req, err := http.NewRequestWithContext(ctx, "POST", cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Add headers if provided.
	for k, v := range cfg.Headers {
		req.Header.Add(k, v)
	}
	return client.Do(req)
}

// ExecuteSOAPRequest sends an XML SOAP request.
func (is *IntegrationSystem) ExecuteSOAPRequest(ctx context.Context, serviceName string, xmlBody []byte) (*http.Response, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(SOAPConfig)
	if !ok {
		return nil, errors.New("invalid SOAP configuration")
	}
	client := getHTTPClient(cfg.Timeout, false)
	req, err := http.NewRequestWithContext(ctx, "POST", cfg.URL, bytes.NewReader(xmlBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", cfg.SOAPAction)
	return client.Do(req)
}

// ExecuteGRPCRequest simulates a gRPC call.
func (is *IntegrationSystem) ExecuteGRPCRequest(ctx context.Context, serviceName string, request string) (string, error) {
	// In a real implementation, you'd use a gRPC client stub.
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return "", err
	}
	cfg, ok := service.Config.(GRPCConfig)
	if !ok {
		return "", errors.New("invalid GRPC configuration")
	}
	// Simulate delay.
	select {
	case <-time.After(parseDuration(cfg.Timeout)):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	is.logger.Info().Str("address", cfg.Address).Msg("Simulated gRPC call executed")
	return "grpc response: OK", nil
}

// ExecuteKafkaMessage simulates sending a message to Kafka.
func (is *IntegrationSystem) ExecuteKafkaMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(KafkaConfig)
	if !ok {
		return errors.New("invalid Kafka configuration")
	}
	// Simulate message send delay.
	select {
	case <-time.After(parseDuration(cfg.Timeout)):
	case <-ctx.Done():
		return ctx.Err()
	}
	is.logger.Info().Str("topic", cfg.Topic).Msgf("Simulated Kafka message sent: %s", message)
	return nil
}

// ExecuteMQTTMessage simulates sending an MQTT message.
func (is *IntegrationSystem) ExecuteMQTTMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(MQTTConfig)
	if !ok {
		return errors.New("invalid MQTT configuration")
	}
	select {
	case <-time.After(parseDuration(cfg.Timeout)):
	case <-ctx.Done():
		return ctx.Err()
	}
	is.logger.Info().Str("topic", cfg.Topic).Msgf("Simulated MQTT message sent: %s", message)
	return nil
}

// ExecuteFTPTransfer performs a simple FTP NOOP command.
func (is *IntegrationSystem) ExecuteFTPTransfer(ctx context.Context, serviceName string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(FTPConfig)
	if !ok {
		return errors.New("invalid FTP configuration")
	}
	address := fmt.Sprintf("%s:%d", cfg.Server, cfg.Port)
	conn, err := net.DialTimeout("tcp", address, parseDuration(cfg.Timeout))
	if err != nil {
		return err
	}
	defer conn.Close()
	// Read initial FTP banner.
	banner := make([]byte, 256)
	_, err = conn.Read(banner)
	if err != nil && err != io.EOF {
		return err
	}
	// Send NOOP command.
	_, err = conn.Write([]byte("NOOP\r\n"))
	if err != nil {
		return err
	}
	is.logger.Info().Msg("FTP NOOP command executed successfully")
	return nil
}

// ExecuteSFTPTransfer simulates an SFTP transfer.
func (is *IntegrationSystem) ExecuteSFTPTransfer(ctx context.Context, serviceName string) error {
	// In a real system use an SFTP client (e.g., golang.org/x/crypto/ssh + SFTP library).
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(SFTPConfig)
	if !ok {
		return errors.New("invalid SFTP configuration")
	}
	// Simulate delay.
	select {
	case <-time.After(parseDuration(cfg.Timeout)):
	case <-ctx.Done():
		return ctx.Err()
	}
	is.logger.Info().Msg("Simulated SFTP transfer executed successfully")
	return nil
}

// ExecutePushNotification simulates sending a push notification.
func (is *IntegrationSystem) ExecutePushNotification(ctx context.Context, serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(PushConfig)
	if !ok {
		return errors.New("invalid Push configuration")
	}
	select {
	case <-time.After(parseDuration(cfg.Timeout)):
	case <-ctx.Done():
		return ctx.Err()
	}
	is.logger.Info().Msgf("Simulated push notification sent via %s: %s", cfg.Provider, message)
	return nil
}

// ExecuteSlackMessage sends a message via a Slack webhook.
func (is *IntegrationSystem) ExecuteSlackMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(SlackConfig)
	if !ok {
		return errors.New("invalid Slack configuration")
	}
	payload := map[string]string{"channel": cfg.Channel, "text": message}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	client := getHTTPClient(cfg.Timeout, false)
	req, err := http.NewRequestWithContext(ctx, "POST", cfg.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	_, err = client.Do(req)
	if err != nil {
		return err
	}
	is.logger.Info().Msg("Slack message sent successfully")
	return nil
}

// ExecuteCustomTCPMessage sends a message over raw TCP.
func (is *IntegrationSystem) ExecuteCustomTCPMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(CustomTCPConfig)
	if !ok {
		return errors.New("invalid CustomTCP configuration")
	}
	conn, err := net.DialTimeout("tcp", cfg.Address, parseDuration(cfg.Timeout))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(message))
	if err != nil {
		return err
	}
	is.logger.Info().Msg("Custom TCP message sent successfully")
	return nil
}

// ExecuteVoIPCall simulates a VoIP (SIP) call.
func (is *IntegrationSystem) ExecuteVoIPCall(ctx context.Context, serviceName string, target string) (string, error) {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return "", err
	}
	cfg, ok := service.Config.(VoIPConfig)
	if !ok {
		return "", errors.New("invalid VoIP configuration")
	}
	select {
	case <-time.After(parseDuration(cfg.Timeout)):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	is.logger.Info().Msgf("Simulated VoIP call to %s via SIP server %s", target, cfg.SIPServer)
	return "voip call connected", nil
}

//
// ==== EXECUTION ENTRY POINT ====
//

// Execute dispatches an operation based on service type.
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
	case ServiceTypeGraphQL:
		query, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for GraphQL service, expected query string")
			break
		}
		res, execErr = is.ExecuteGraphQLRequest(ctx, serviceName, query)
	case ServiceTypeSOAP:
		xmlPayload, ok := payload.([]byte)
		if !ok {
			execErr = fmt.Errorf("invalid payload for SOAP service, expected XML []byte")
			break
		}
		res, execErr = is.ExecuteSOAPRequest(ctx, serviceName, xmlPayload)
	case ServiceTypeGRPC:
		req, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for GRPC service, expected request string")
			break
		}
		var grpcResp string
		grpcResp, execErr = is.ExecuteGRPCRequest(ctx, serviceName, req)
		res = grpcResp
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
	case ServiceTypeKafka:
		msg, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for Kafka service, expected message string")
			break
		}
		execErr = is.ExecuteKafkaMessage(ctx, serviceName, msg)
	case ServiceTypeMQTT:
		msg, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for MQTT service, expected message string")
			break
		}
		execErr = is.ExecuteMQTTMessage(ctx, serviceName, msg)
	case ServiceTypeFTP:
		execErr = is.ExecuteFTPTransfer(ctx, serviceName)
	case ServiceTypeSFTP:
		execErr = is.ExecuteSFTPTransfer(ctx, serviceName)
	case ServiceTypePush:
		msg, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for Push service, expected message string")
			break
		}
		execErr = is.ExecutePushNotification(ctx, serviceName, msg)
	case ServiceTypeSlack:
		msg, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for Slack service, expected message string")
			break
		}
		execErr = is.ExecuteSlackMessage(ctx, serviceName, msg)
	case ServiceTypeCustomTCP:
		msg, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for CustomTCP service, expected message string")
			break
		}
		execErr = is.ExecuteCustomTCPMessage(ctx, serviceName, msg)
	case ServiceTypeVoIP:
		target, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for VoIP service, expected target phone/address string")
			break
		}
		var voipResp string
		voipResp, execErr = is.ExecuteVoIPCall(ctx, serviceName, target)
		res = voipResp
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

//
// ==== EXISTING SUPPORT FUNCTIONS (SMTP, SMS, DB) ====
//

func (is *IntegrationSystem) SendEmail(ctx context.Context, serviceName string, to []string, message []byte) error {
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(SMTPConfig)
	if !ok {
		return fmt.Errorf("not a valid SMTP configuration for service: %s", serviceName)
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
	// For SMPP, we simulate the SMS sending.
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(SMPPConfig)
	if !ok {
		return fmt.Errorf("not a valid SMPP configuration for service: %s", serviceName)
	}
	var smppUser, smppPass string
	if service.CredentialKey != "" {
		cred, err := is.GetCredential(service.CredentialKey)
		if err != nil {
			return err
		}
		if cred.Type != CredentialTypeSMPP {
			return fmt.Errorf("expected SMPP credential for service %s", serviceName)
		}
		data, ok := cred.Data.(map[string]interface{})
		if ok {
			if u, ok := data["username"].(string); ok {
				smppUser = u
			}
			if p, ok := data["password"].(string); ok {
				smppPass = p
			}
		}
	}
	is.logger.Info().Any("config", cfg).Str("service", serviceName).
		Str("message", message).Str("username", smppUser).Str("pass", smppPass).
		Msg("Simulated sending SMS with SMPP credentials")
	return nil
}

func (is *IntegrationSystem) ExecuteDatabaseQuery(ctx context.Context, serviceName, query string) (any, error) {
	// For database execution, we simulate a dummy query.
	service, err := is.services.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(DatabaseConfig)
	if !ok {
		return nil, errors.New("invalid Database configuration")
	}
	connStr := fmt.Sprintf("%s:%d/%s?sslmode=%s", cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)
	if service.CredentialKey != "" {
		cred, err := is.GetCredential(service.CredentialKey)
		if err == nil && cred.Type == CredentialTypeDatabase {
			data, ok := cred.Data.(map[string]interface{})
			if ok {
				username, uok := data["username"].(string)
				password, pok := data["password"].(string)
				if uok && pok {
					connStr = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
						username, password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)
				}
			}
		}
	}
	is.logger.Info().Str("driver", cfg.Driver).Str("connStr", connStr).
		Str("query", query).Msg("Simulated executing database query")
	return "dummy database result", nil
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

//
// ==== HEALTH CHECK & SHUTDOWN ====
//

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
				client := getHTTPClient(cfg.Timeout, cfg.TLSInsecureSkipVerify)
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
				// Add additional health checks as needed.
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
	// Wait for all background goroutines to finish.
	is.wg.Wait()
	return nil
}

//
// ==== CONFIGURATION LOADING ====
//

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

//
// ==== MAIN ====
//

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

	// Configuration reload loop.
	integration.wg.Add(1)
	go func() {
		defer integration.wg.Done()
		for {
			select {
			case <-reloadCh:
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
	integration.wg.Add(8)
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
		defer integration.wg.Done()
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
		defer integration.wg.Done()
		// Simulated gRPC call
		resp, err := integration.Execute(ctx, "grpc-service", "gRPC request data")
		if err != nil {
			logger.Error().Err(err).Msg("gRPC request failed")
		} else {
			logger.Info().Msgf("gRPC response: %v", resp)
		}
	}()
	go func() {
		defer integration.wg.Done()
		if err := integration.ExecuteKafkaMessage(ctx, "kafka-service", "Test Kafka message"); err != nil {
			logger.Error().Err(err).Msg("Kafka message failed")
		}
	}()
	go func() {
		defer integration.wg.Done()
		if err := integration.ExecuteMQTTMessage(ctx, "mqtt-service", "Test MQTT message"); err != nil {
			logger.Error().Err(err).Msg("MQTT message failed")
		}
	}()
	go func() {
		defer integration.wg.Done()
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
	integration.wg.Wait()
	logger.Info().Msg("Exiting integration system.")
}
