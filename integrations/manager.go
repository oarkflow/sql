package integrations

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"sync"
	"time"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/json"
	"github.com/oarkflow/log"
)

type Manager struct {
	services        ServiceStore
	credentials     CredentialStore
	circuitBreakers map[string]*CircuitBreaker
	m               sync.Mutex
	logger          *log.Logger
	wg              sync.WaitGroup
}

type Options func(*Manager)

func WithServiceStore(store ServiceStore) Options {
	return func(m *Manager) {
		m.services = store
	}
}

func WithCredentialStore(store CredentialStore) Options {
	return func(m *Manager) {
		m.credentials = store
	}
}

func WithLogger(logger *log.Logger) Options {
	return func(m *Manager) {
		m.logger = logger
	}
}

func New(opts ...Options) *Manager {
	m := &Manager{
		services:        NewInMemoryServiceStore(),
		credentials:     NewInMemoryCredentialStore(),
		circuitBreakers: make(map[string]*CircuitBreaker),
		logger:          &log.DefaultLogger,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (is *Manager) Lock() {
	is.m.Lock()
}

func (is *Manager) Unlock() {
	is.m.Unlock()
}

func (is *Manager) Add(delta int) {
	is.wg.Add(delta)
}

func (is *Manager) Done() {
	is.wg.Done()
}

func (is *Manager) Wait() {
	is.wg.Wait()
}

func (is *Manager) Logger() *log.Logger {
	return is.logger
}

func (is *Manager) AddCB(name string, cb *CircuitBreaker) {
	is.Lock()
	defer is.Unlock()
	is.circuitBreakers[name] = cb
}

func (is *Manager) getCircuitBreaker(serviceName string, threshold int) *CircuitBreaker {
	is.Lock()
	defer is.Unlock()
	cb, exists := is.circuitBreakers[serviceName]
	if !exists {
		cb = NewCircuitBreaker(threshold)
		is.circuitBreakers[serviceName] = cb
	}
	return cb
}

func (is *Manager) ExecuteAPIRequest(ctx context.Context, serviceName string, body []byte) (*http.Response, error) {
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
	client := getHTTPClient(cfg.Timeout, cfg.TLSInsecureSkipVerify)
	return client.Do(req)
}

// ExecuteAPIRequestWithRetry uses context-aware backoff.
func (is *Manager) ExecuteAPIRequestWithRetry(ctx context.Context, serviceName string, body []byte, maxRetries int) (*http.Response, error) {
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
func (is *Manager) ExecuteGraphQLRequest(ctx context.Context, serviceName string, query string) (*http.Response, error) {
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
func (is *Manager) ExecuteSOAPRequest(ctx context.Context, serviceName string, xmlBody []byte) (*http.Response, error) {
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
func (is *Manager) ExecuteGRPCRequest(ctx context.Context, serviceName string, request string) (string, error) {
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
func (is *Manager) ExecuteKafkaMessage(ctx context.Context, serviceName string, message string) error {
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
func (is *Manager) ExecuteMQTTMessage(ctx context.Context, serviceName string, message string) error {
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
func (is *Manager) ExecuteFTPTransfer(ctx context.Context, serviceName string) error {
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
func (is *Manager) ExecuteSFTPTransfer(ctx context.Context, serviceName string) error {
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
func (is *Manager) ExecutePushNotification(ctx context.Context, serviceName string, message string) error {
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
func (is *Manager) ExecuteSlackMessage(ctx context.Context, serviceName string, message string) error {
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
func (is *Manager) ExecuteCustomTCPMessage(ctx context.Context, serviceName string, message string) error {
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
func (is *Manager) ExecuteVoIPCall(ctx context.Context, serviceName string, target string) (string, error) {
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

type EmailPayload struct {
	To      []string
	Message []byte
}

// Execute dispatches an operation based on service type.
func (is *Manager) Execute(ctx context.Context, serviceName string, payload any) (any, error) {
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

func (is *Manager) SendEmail(ctx context.Context, serviceName string, to []string, message []byte) error {
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

func (is *Manager) SendSMS(ctx context.Context, serviceName string, message string) error {
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

func (is *Manager) ExecuteDatabaseQuery(ctx context.Context, serviceName, query string) (any, error) {
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

func (is *Manager) GetCredential(key string) (Credential, error) {
	cred, err := is.credentials.GetCredential(key)
	if err != nil {
		is.logger.Error().Str("key", key).Err(err).Msg("failed to get credential")
	}
	return cred, err
}

func (is *Manager) HealthCheck(ctx context.Context) error {
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
