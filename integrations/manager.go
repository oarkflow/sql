package integrations

import (
	"bytes"
	"context"
	"crypto/tls"
	stdJson "encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/oarkflow/errors"
	"github.com/oarkflow/json"
	"github.com/oarkflow/json/jsonparser"
	"github.com/oarkflow/log"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/connection"
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

func (is *Manager) ServiceList() ([]Service, error) {
	return is.services.ListServices()
}

func (is *Manager) GetService(service string) (Service, error) {
	return is.services.GetService(service)
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

func (is *Manager) ExecuteAPIRequest(ctx context.Context, serviceName string, body any) (*http.Response, error) {
	service, err := is.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(APIConfig)
	if !ok {
		return nil, errors.New("invalid API configuration")
	}
	cred, err := is.GetCredential(service.CredentialKey, service.RequireAuth)
	if err != nil {
		if err.Error() == "credential not found" {
			if service.RequireAuth {
				return nil, err
			}
		}
	}
	var reader io.Reader
	switch body := body.(type) {
	case []byte:
		reader = bytes.NewReader(body)
	case nil:
	default:
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(bodyBytes)
	}
	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, reader)
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
		key, ok := data["key"].(string)
		if !ok {
			return nil, errors.New("missing API key value")
		}
		headerConf := cfg.AuthHeaders[string(CredentialTypeAPIKey)]
		req.Header.Add(headerConf.Header, headerConf.Prefix+key)
	case CredentialTypeBearer:
		data, ok := cred.Data.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid bearer token credential format")
		}
		token, ok := data["token"].(string)
		if !ok {
			return nil, errors.New("missing bearer token value")
		}
		headerConf := cfg.AuthHeaders[string(CredentialTypeBearer)]
		req.Header.Add(headerConf.Header, headerConf.Prefix+token)
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
		headerConf := cfg.AuthHeaders[string(CredentialTypeOAuth2)]
		req.Header.Add(headerConf.Header, headerConf.Prefix+auth.AccessToken)
	default:
		if service.RequireAuth {
			return nil, fmt.Errorf("unsupported credential type for API: %s", cred.Type)
		}
	}
	client := getHTTPClient(cfg.Timeout, cfg.TLSInsecureSkipVerify)
	return client.Do(req)
}

// ExecuteAPIRequestWithRetry uses context-aware backoff.
func (is *Manager) ExecuteAPIRequestWithRetry(ctx context.Context, serviceName string, body any, maxRetries int) (*http.Response, error) {
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
	service, err := is.GetService(serviceName)
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
	service, err := is.GetService(serviceName)
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
	service, err := is.GetService(serviceName)
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
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Str("topic", cfg.Topic).Msgf("Simulated Kafka message sent: %s", message)
	return nil
}

// ExecuteMQTTMessage simulates sending an MQTT message.
func (is *Manager) ExecuteMQTTMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Str("topic", cfg.Topic).Msgf("Simulated MQTT message sent: %s", message)
	return nil
}

// ExecuteFTPTransfer performs a simple FTP NOOP command.
func (is *Manager) ExecuteFTPTransfer(ctx context.Context, serviceName string) error {
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Msg("FTP NOOP command executed successfully")
	return nil
}

// ExecuteSFTPTransfer simulates an SFTP transfer.
func (is *Manager) ExecuteSFTPTransfer(ctx context.Context, serviceName string) error {
	// In a real system use an SFTP client (e.g., golang.org/x/crypto/ssh + SFTP library).
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Msg("Simulated SFTP transfer executed successfully")
	return nil
}

// ExecutePushNotification simulates sending a push notification.
func (is *Manager) ExecutePushNotification(ctx context.Context, serviceName string, message string) error {
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Msgf("Simulated push notification sent via %s: %s", cfg.Provider, message)
	return nil
}

// ExecuteSlackMessage sends a message via a Slack webhook.
func (is *Manager) ExecuteSlackMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Msg("Slack message sent successfully")
	return nil
}

// ExecuteCustomTCPMessage sends a message over raw TCP.
func (is *Manager) ExecuteCustomTCPMessage(ctx context.Context, serviceName string, message string) error {
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Msg("Custom TCP message sent successfully")
	return nil
}

// ExecuteVoIPCall simulates a VoIP (SIP) call.
func (is *Manager) ExecuteVoIPCall(ctx context.Context, serviceName string, target string) (string, error) {
	service, err := is.GetService(serviceName)
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
	is.logger.Info().Str("service_name", serviceName).Msgf("Simulated VoIP call to %s via SIP server %s", target, cfg.SIPServer)
	return "voip call connected", nil
}

// New function to execute a WebCrawler request.
func (is *Manager) ExecuteWebCrawlerRequest(ctx context.Context, serviceName string) ([]map[string]any, error) {
	service, err := is.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(WebCrawlerConfig)
	if !ok {
		return nil, errors.New("invalid WebCrawler configuration")
	}
	// Create custom client with timeout and proxy if provided.
	client := &http.Client{Timeout: parseDuration(cfg.Timeout)}
	if cfg.ProxyURL != "" {
		if parsed, err := url.Parse(cfg.ProxyURL); err == nil {
			client.Transport = &http.Transport{Proxy: http.ProxyURL(parsed)}
		}
	}
	req, err := http.NewRequestWithContext(ctx, "GET", cfg.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	if cfg.UserAgent != "" {
		req.Header.Set("User-Agent", cfg.UserAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}
	var records []map[string]any
	doc.Find(cfg.Rules).Each(func(i int, row *goquery.Selection) {
		record := make(map[string]any)
		for _, mapping := range cfg.FieldMappings {
			elem := row.Find(mapping.Selector)
			var value string
			switch {
			case mapping.Target == "html":
				value, _ = elem.Html()
			default:
				value = elem.Text()
			}
			record[mapping.Field] = value
		}
		records = append(records, record)
	})
	return records, nil
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

type SMSPayload struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Message string `json:"message"`
}

type EmailPayload struct {
	To      []string `json:"to"`
	Message []byte   `json:"message"`
}

type HTTPResponse struct {
	StatusCode int                 `json:"status_code"`
	Body       stdJson.RawMessage  `json:"body"`
	Headers    map[string][]string `json:"headers"`
}

// Execute dispatches an operation based on service type.
func (is *Manager) Execute(ctx context.Context, serviceName string, payload any) (any, error) {
	service, err := is.GetService(serviceName)
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
		maxRetries := 3
		apiCfg, ok := service.Config.(APIConfig)
		if ok && apiCfg.RetryCount > 0 {
			maxRetries = apiCfg.RetryCount
		}
		resp, err := is.ExecuteAPIRequestWithRetry(ctx, serviceName, payload, maxRetries)
		if err != nil {
			execErr = err
		} else {
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				execErr = err
			} else {
				if apiCfg.DataKey != "" {
					body, _, _, err = jsonparser.Get(body, apiCfg.DataKey)
				}
				res = &HTTPResponse{
					StatusCode: resp.StatusCode,
					Body:       body,
					Headers:    resp.Header,
				}
			}
		}
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
		msg, ok := payload.(SMSPayload)
		if !ok {
			execErr = fmt.Errorf("invalid payload for SMPP service, expected to and message")
			break
		}
		execErr = is.SendSMSViaSMPP(ctx, serviceName, msg)
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
	// New case for WebCrawler integration.
	case ServiceTypeWebCrawler:
		var crawlerResult []map[string]any
		crawlerResult, execErr = is.ExecuteWebCrawlerRequest(ctx, serviceName)
		res = crawlerResult
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
	service, err := is.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(SMTPConfig)
	if !ok {
		return fmt.Errorf("not a valid SMTP configuration for service: %s", serviceName)
	}
	cred, err := is.GetCredential(service.CredentialKey, service.RequireAuth)
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

func (is *Manager) SendSMSViaSMPP(ctx context.Context, serviceName string, message SMSPayload) error {
	// For SMPP, we simulate the SMS sending.
	service, err := is.GetService(serviceName)
	if err != nil {
		return err
	}
	cfg, ok := service.Config.(SMPPConfig)
	if !ok {
		return fmt.Errorf("not a valid SMPP configuration for service: %s", serviceName)
	}
	var smppUser, smppPass string
	if service.CredentialKey != "" {
		cred, err := is.GetCredential(service.CredentialKey, service.RequireAuth)
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
		Str("message", message.Message).Str("username", smppUser).Str("pass", smppPass).
		Msg("Simulated sending SMS with SMPP credentials")
	return nil
}

func (is *Manager) ExecuteDatabaseQuery(ctx context.Context, serviceName, query string) (any, error) {
	service, err := is.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(DatabaseConfig)
	if !ok {
		return nil, errors.New("invalid Database configuration")
	}

	if service.CredentialKey == "" {
		return nil, errors.New("credentials not found")
	}
	cred, err := is.GetCredential(service.CredentialKey, service.RequireAuth)
	if err != nil {
		return nil, err
	}
	if cred.Type != CredentialTypeDatabase {
		return nil, errors.New("credential is not for database")
	}
	var username, password string
	var uok, pok bool
	switch credentials := cred.Data.(type) {
	case map[string]any:
		username, uok = credentials["username"].(string)
		password, pok = credentials["password"].(string)
		if !(uok && pok) {
			return nil, errors.New("credentials missing")
		}
	case DatabaseCredential:
		username = credentials.Username
		password = credentials.Password
	}
	db, _, err := connection.FromConfig(squealx.Config{
		Driver:      cfg.Driver,
		Host:        cfg.Host,
		Port:        cfg.Port,
		Username:    username,
		Password:    password,
		Database:    cfg.Database,
		MaxIdleCons: cfg.MaxIdleConns,
		MaxOpenCons: cfg.MaxOpenConns,
	})
	defer db.Close()
	if err != nil {
		return nil, err
	}
	var dest []map[string]any
	err = db.Select(&dest, query)
	if err != nil {
		return nil, err
	}
	is.logger.Info().Str("service_name", serviceName).Str("driver", cfg.Driver).Str("query", query).Msg("Executing database query")
	return dest, nil
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

func (is *Manager) GetCredential(key string, requireAuth ...bool) (Credential, error) {
	cred, err := is.credentials.GetCredential(key, requireAuth...)
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
