package integrations

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/errors"
)

type ServiceType string

type CredentialType string

const (
	ServiceTypeAPI        ServiceType = "rest"
	ServiceTypeSMTP       ServiceType = "smtp"
	ServiceTypeSMPP       ServiceType = "smpp"
	ServiceTypeDB         ServiceType = "database"
	ServiceTypeGraphQL    ServiceType = "graphql"
	ServiceTypeSOAP       ServiceType = "soap"
	ServiceTypeGRPC       ServiceType = "grpc"
	ServiceTypeKafka      ServiceType = "kafka"
	ServiceTypeMQTT       ServiceType = "mqtt"
	ServiceTypeFTP        ServiceType = "ftp"
	ServiceTypeSFTP       ServiceType = "sftp"
	ServiceTypePush       ServiceType = "push"
	ServiceTypeSlack      ServiceType = "slack"
	ServiceTypeCustomTCP  ServiceType = "custom_tcp"
	ServiceTypeVoIP       ServiceType = "voip"
	ServiceTypeWebCrawler ServiceType = "webcrawler"
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

// AuthHeader configurable auth header mapping.
type AuthHeader struct {
	Header string `json:"header"`
	Prefix string `json:"prefix"`
}

// APIConfig to allow configurable auth headers.
type APIConfig struct {
	URL                     string                `json:"url"`
	Method                  string                `json:"method"`
	Headers                 map[string]string     `json:"headers"`
	DataKey                 string                `json:"data_key"`
	RequestBody             string                `json:"request_body"`
	Timeout                 string                `json:"timeout"` // e.g., "10s"
	TLSInsecureSkipVerify   bool                  `json:"tls_insecure_skip_verify"`
	RetryCount              int                   `json:"retry_count"`
	CircuitBreakerThreshold int                   `json:"circuit_breaker_threshold"`
	AuthHeaders             map[string]AuthHeader `json:"auth_headers"`
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

// FieldMapping for WebCrawler.
type FieldMapping struct {
	Field    string `json:"field"`
	Selector string `json:"selector"`
	Target   string `json:"target"`
}

// WebCrawlerConfig defines the configuration for a WebCrawler integration.
type WebCrawlerConfig struct {
	Endpoint      string         `json:"endpoint"`
	Rules         string         `json:"rules"`
	Target        string         `json:"target"`
	OutputFormat  string         `json:"output_format"`
	FieldMappings []FieldMapping `json:"field_mappings"`
	Timeout       string         `json:"timeout"` // e.g., "10s"
	UserAgent     string         `json:"user_agent"`
	ProxyURL      string         `json:"proxy_url"`
}

func (cfg WebCrawlerConfig) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("WebCrawlerConfig: endpoint must be provided")
	}
	if cfg.Rules == "" {
		return errors.New("WebCrawlerConfig: rules must be provided")
	}
	if _, err := time.ParseDuration(cfg.Timeout); err != nil {
		return fmt.Errorf("WebCrawlerConfig: invalid timeout: %v", err)
	}
	return nil
}

// sampleFromStruct converts a struct into a map[string]any.
func sampleFromStruct(s any) (map[string]any, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// RequiredConfigFormat returns sample configuration and credential examples based on
// default values from the actual config structures.
func RequiredConfigFormat(serviceType ServiceType) (map[string]any, []map[string]any) {
	var cfg any
	var creds []map[string]any
	switch serviceType {
	case ServiceTypeAPI:
		cfg = APIConfig{
			URL:                     "https://example.com/api",
			Method:                  "GET",
			Timeout:                 "10s",
			TLSInsecureSkipVerify:   false,
			RetryCount:              3,
			CircuitBreakerThreshold: 5,
			Headers:                 map[string]string{"Content-Type": "application/json"},
			AuthHeaders: map[string]AuthHeader{
				string(CredentialTypeAPIKey): {Header: "X-API-KEY", Prefix: ""},
				string(CredentialTypeBearer): {Header: "Authorization", Prefix: "Bearer "},
				string(CredentialTypeOAuth2): {Header: "Authorization", Prefix: "Bearer "},
			},
		}
		// Build sample credentials based on config types.
		if m, _ := sampleFromStruct(APIKeyCredential{Key: "your_api_key"}); m != nil {
			creds = append(creds, map[string]any{"type": CredentialTypeAPIKey, "data": m})
		}
		if m, _ := sampleFromStruct(BearerCredential{Token: "your_token"}); m != nil {
			creds = append(creds, map[string]any{"type": CredentialTypeBearer, "data": m})
		}
		if m, _ := sampleFromStruct(OAuth2Credential{
			ClientID:     "your_client_id",
			ClientSecret: "your_client_secret",
			AuthURL:      "https://example.com/auth",
			TokenURL:     "https://example.com/token",
			Scope:        "read write",
			AccessToken:  "your_access_token",
			RefreshToken: "your_refresh_token",
			ExpiresAt:    time.Now().Add(1 * time.Hour),
		}); m != nil {
			creds = append(creds, map[string]any{"type": CredentialTypeOAuth2, "data": m})
		}
	case ServiceTypeSMTP:
		cfg = SMTPConfig{
			Server:            "smtp.example.com",
			Port:              587,
			From:              "noreply@example.com",
			UseTLS:            true,
			UseSTARTTLS:       false,
			ConnectionTimeout: "10s",
			MaxConnections:    10,
		}
		if m, _ := sampleFromStruct(SMTPAuthCredential{Username: "smtp_user", Password: "smtp_pass"}); m != nil {
			creds = append(creds, map[string]any{"type": CredentialTypeBasicAuth, "data": m})
		}
	case ServiceTypeSMPP:
		cfg = SMPPConfig{
			SystemType: "SMPP",
			Host:       "smpp.example.com",
			Port:       2775,
			SourceAddr: "smpp_src",
			DestAddr:   "smpp_dest",
			RetryCount: 3,
		}
		if m, _ := sampleFromStruct(SMPPCredential{Username: "smpp_user", Password: "smpp_pass"}); m != nil {
			creds = append(creds, map[string]any{"type": CredentialTypeSMPP, "data": m})
		}
	case ServiceTypeDB:
		cfg = DatabaseConfig{
			Driver:          "postgres",
			Host:            "db.example.com",
			Port:            5432,
			Database:        "sampledb",
			SSLMode:         "disable",
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: "30m",
			ConnectTimeout:  "10s",
			ReadTimeout:     "5s",
			WriteTimeout:    "5s",
			PoolSize:        20,
		}
		if m, _ := sampleFromStruct(DatabaseCredential{Username: "db_user", Password: "db_pass"}); m != nil {
			creds = append(creds, map[string]any{"type": CredentialTypeDatabase, "data": m})
		}
	case ServiceTypeGraphQL:
		cfg = GraphQLConfig{
			URL:     "https://example.com/graphql",
			Headers: map[string]string{"Content-Type": "application/json"},
			Timeout: "10s",
		}
	case ServiceTypeSOAP:
		cfg = SOAPConfig{
			URL:        "https://example.com/soap",
			SOAPAction: "urn:action",
			Timeout:    "10s",
		}
	case ServiceTypeGRPC:
		cfg = GRPCConfig{
			Address: "grpc.example.com:50051",
			Timeout: "10s",
		}
	case ServiceTypeKafka:
		cfg = KafkaConfig{
			Brokers: []string{"broker1:9092", "broker2:9092"},
			Topic:   "example.topic",
			Timeout: "10s",
		}
	case ServiceTypeMQTT:
		cfg = MQTTConfig{
			Server:   "mqtt.example.com",
			ClientID: "client1",
			Topic:    "example/topic",
			Timeout:  "10s",
		}
	case ServiceTypeFTP:
		cfg = FTPConfig{
			Server:   "ftp.example.com",
			Port:     21,
			Username: "ftp_user",
			Password: "ftp_pass",
			Timeout:  "10s",
		}
	case ServiceTypeSFTP:
		cfg = SFTPConfig{
			Server:   "sftp.example.com",
			Port:     22,
			Username: "sftp_user",
			Password: "sftp_pass",
			Timeout:  "10s",
		}
	case ServiceTypePush:
		cfg = PushConfig{
			Provider: "push_provider",
			APIKey:   "your_push_api_key",
			Endpoint: "https://push.example.com",
			Timeout:  "10s",
		}
	case ServiceTypeSlack:
		cfg = SlackConfig{
			WebhookURL: "https://hooks.slack.com/services/XXX/YYY/ZZZ",
			Channel:    "#general",
			Timeout:    "10s",
		}
	case ServiceTypeCustomTCP:
		cfg = CustomTCPConfig{
			Address: "tcp.example.com:1234",
			Timeout: "10s",
		}
	case ServiceTypeVoIP:
		cfg = VoIPConfig{
			SIPServer: "sip.example.com",
			Username:  "voip_user",
			Password:  "voip_pass",
			Timeout:   "10s",
		}
	case ServiceTypeWebCrawler:
		cfg = WebCrawlerConfig{
			Endpoint:      "https://example.com",
			Rules:         ".item",
			Target:        "body",
			OutputFormat:  "json",
			FieldMappings: []FieldMapping{{Field: "title", Selector: "h1", Target: "text"}},
			Timeout:       "10s",
			UserAgent:     "Mozilla/5.0",
			ProxyURL:      "http://proxy.example.com:8080",
		}
	default:
		cfg = nil
	}
	m, _ := sampleFromStruct(cfg)
	return m, creds
}
