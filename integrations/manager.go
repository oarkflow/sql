package integrations

import (
	"bytes"
	"context"
	"crypto/tls"
	stdJson "encoding/json"
	sterrors "errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/oarkflow/errors"
	"github.com/oarkflow/json"
	"github.com/oarkflow/json/jsonparser"
	"github.com/oarkflow/log"
	"github.com/oarkflow/mail"
	"github.com/oarkflow/smpp"
	"github.com/oarkflow/smpp/pdu/pdufield"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/connection"
)

type Manager struct {
	onSmppMessageReport func(manager *smpp.Manager, sms *smpp.Message, parts []*smpp.Part)
	services            ServiceStore
	credentials         CredentialStore
	circuitBreakers     map[string]*CircuitBreaker
	asyncClients        map[string]any
	httpClients         map[string]*http.Client
	dbConnections       map[string]*squealx.DB
	asyncMu             sync.RWMutex
	m                   sync.Mutex
	logger              *log.Logger
	wg                  sync.WaitGroup
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
		asyncClients:    make(map[string]any),
		httpClients:     make(map[string]*http.Client),
		dbConnections:   make(map[string]*squealx.DB),
		logger:          &log.DefaultLogger,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (is *Manager) SetOnSmppMessageReport(fn func(manager *smpp.Manager, sms *smpp.Message, parts []*smpp.Part)) {
	is.onSmppMessageReport = fn
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

func (is *Manager) DeleteService(service string) error {
	return is.services.DeleteService(service)
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

func (is *Manager) SetAsyncClient(serviceName string, client any) {
	is.asyncMu.Lock()
	defer is.asyncMu.Unlock()
	is.asyncClients[serviceName] = client
}

func (is *Manager) GetAsyncClient(serviceName string) (any, bool) {
	is.asyncMu.RLock()
	defer is.asyncMu.RUnlock()
	client, ok := is.asyncClients[serviceName]
	return client, ok
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
		if sterrors.Is(err, ErrCredentialNotFound) {
			if service.RequireAuth {
				return nil, err
			}
		} else {
			return nil, err
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
	if reader == nil && strings.TrimSpace(cfg.RequestBody) != "" {
		reader = bytes.NewReader([]byte(cfg.RequestBody))
	}
	if strings.TrimSpace(cfg.Method) == "" {
		cfg.Method = "GET"
	}
	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, reader)
	if err != nil {
		return nil, err
	}
	for k, v := range cfg.Headers {
		req.Header.Add(k, v)
	}
	if reader != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	switch cred.Type {
	case CredentialTypeAPIKey:
		var key string
		switch v := cred.Data.(type) {
		case map[string]any:
			if k, ok := v["key"].(string); ok {
				key = k
			}
		case APIKeyCredential:
			key = v.Key
		case *APIKeyCredential:
			key = v.Key
		}
		if key == "" && service.RequireAuth {
			return nil, errors.New("missing API key value")
		}
		if key != "" {
			headerConf := cfg.AuthHeaders[string(CredentialTypeAPIKey)]
			if headerConf.Header == "" {
				headerConf.Header = "X-API-KEY"
			}
			if headerConf.Header != "" {
				req.Header.Add(headerConf.Header, headerConf.Prefix+key)
			}
		}
	case CredentialTypeBearer:
		var token string
		switch v := cred.Data.(type) {
		case map[string]any:
			if t, ok := v["token"].(string); ok {
				token = t
			}
		case BearerCredential:
			token = v.Token
		case *BearerCredential:
			token = v.Token
		}
		if token == "" && service.RequireAuth {
			return nil, errors.New("missing bearer token value")
		}
		if token != "" {
			headerConf := cfg.AuthHeaders[string(CredentialTypeBearer)]
			if headerConf.Header == "" {
				headerConf.Header = "Authorization"
			}
			if headerConf.Prefix == "" {
				headerConf.Prefix = "Bearer "
			}
			if headerConf.Header != "" {
				req.Header.Add(headerConf.Header, headerConf.Prefix+token)
			}
		}
	case CredentialTypeBasicAuth:
		var user, pass string
		switch v := cred.Data.(type) {
		case map[string]any:
			user, _ = v["username"].(string)
			pass, _ = v["password"].(string)
		case BasicAuthCredential:
			user, pass = v.Username, v.Password
		case *BasicAuthCredential:
			user, pass = v.Username, v.Password
		case SMTPAuthCredential:
			user, pass = v.Username, v.Password
		case *SMTPAuthCredential:
			user, pass = v.Username, v.Password
		}
		if user == "" || pass == "" {
			if service.RequireAuth {
				return nil, errors.New("missing basic auth credentials")
			}
		} else {
			req.SetBasicAuth(user, pass)
		}
	case CredentialTypeOAuth2:
		auth, ok := cred.Data.(*OAuth2Credential)
		if !ok {
			if v, ok2 := cred.Data.(OAuth2Credential); ok2 {
				auth = &v
			} else {
				return nil, errors.New("invalid OAuth2 credential")
			}
		}
		if auth.AccessToken == "" || time.Until(auth.ExpiresAt) < time.Minute {
			if err := refreshOAuth2Token(auth, is.logger); err != nil {
				return nil, fmt.Errorf("failed to refresh OAuth2 token: %w", err)
			}
		}
		headerConf := cfg.AuthHeaders[string(CredentialTypeOAuth2)]
		if headerConf.Header == "" {
			headerConf.Header = "Authorization"
		}
		if headerConf.Prefix == "" {
			headerConf.Prefix = "Bearer "
		}
		if headerConf.Header != "" {
			req.Header.Add(headerConf.Header, headerConf.Prefix+auth.AccessToken)
		}
	default:
		if service.RequireAuth {
			return nil, fmt.Errorf("unsupported credential type for API: %s", cred.Type)
		}
	}
	client := is.getHTTPClient(cfg.Timeout, cfg.TLSInsecureSkipVerify)
	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start)
	if err != nil {
		is.logger.Error().Err(err).Str("service", serviceName).Dur("duration", duration).Msg("API request failed")
		return nil, err
	}
	is.logger.Info().Str("service", serviceName).Int("status_code", resp.StatusCode).Dur("duration", duration).Msg("API request executed")
	return resp, nil
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
	client := is.getHTTPClient(cfg.Timeout, false)
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
	client := is.getHTTPClient(cfg.Timeout, false)
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
	client := is.getHTTPClient(cfg.Timeout, false)
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

func (is *Manager) getHTTPClient(timeout string, insecure bool) *http.Client {
	key := fmt.Sprintf("%s-%v", timeout, insecure)
	is.asyncMu.RLock()
	if client, ok := is.httpClients[key]; ok {
		is.asyncMu.RUnlock()
		return client
	}
	is.asyncMu.RUnlock()

	is.asyncMu.Lock()
	defer is.asyncMu.Unlock()
	// Double check
	if client, ok := is.httpClients[key]; ok {
		return client
	}

	client := &http.Client{
		Timeout: parseDuration(timeout),
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	is.httpClients[key] = client
	return client
}

func (is *Manager) getDBConnection(serviceName string, cfg DatabaseConfig, username, password string) (*squealx.DB, error) {
	is.m.Lock()
	defer is.m.Unlock()
	if db, ok := is.dbConnections[serviceName]; ok {
		return db, nil
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
	if err != nil {
		return nil, err
	}
	is.dbConnections[serviceName] = db
	return db, nil
}

type SMSPayload struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Message string `json:"message"`
	Test    bool   `json:"test"`
}

type ServiceResponse struct {
	StatusCode int                 `json:"status_code"`
	Body       stdJson.RawMessage  `json:"body"`
	Headers    map[string][]string `json:"headers"`
	Service    string              `json:"service"`
	Type       string              `json:"type"`
	Success    bool                `json:"success"`
	Error      string              `json:"error,omitempty"`
	Duration   time.Duration       `json:"duration,omitempty"`
}

// ExecuteAsync runs a service execution in a goroutine and returns a channel for the ServiceResponse.
// Example usage:
//
//	ch := manager.ExecuteAsync(ctx, "svc1", payload)
//	result := <-ch
func (is *Manager) ExecuteAsync(ctx context.Context, serviceName string, payload any) <-chan *ServiceResponse {
	ch := make(chan *ServiceResponse, 1)
	go func() {
		start := time.Now()
		res, err := is.Execute(ctx, serviceName, payload)
		duration := time.Since(start)
		sr := &ServiceResponse{Service: serviceName, Duration: duration}
		if err != nil {
			sr.Success = false
			sr.Error = err.Error()
		} else {
			sr.Success = true
			if r, ok := res.(*ServiceResponse); ok {
				*sr = *r
				sr.Service = serviceName
				sr.Duration = duration
			} else {
				sr.Body, _ = json.Marshal(res)
			}
		}
		ch <- sr
	}()
	return ch
}

// BulkExecute runs multiple services in parallel and returns their results as ServiceResponse.
// Example usage:
//
//	results := manager.BulkExecute(ctx, map[string]any{"svc1": payload1, "svc2": payload2})
//	for name, resp := range results { ... }
func (is *Manager) BulkExecute(ctx context.Context, requests map[string]any) map[string]*ServiceResponse {
	results := make(map[string]*ServiceResponse)
	var wg sync.WaitGroup
	mu := sync.Mutex{}
	for svc, payload := range requests {
		wg.Add(1)
		go func(svc string, payload any) {
			defer wg.Done()
			start := time.Now()
			res, err := is.Execute(ctx, svc, payload)
			duration := time.Since(start)
			sr := &ServiceResponse{Service: svc, Duration: duration}
			if err != nil {
				sr.Success = false
				sr.Error = err.Error()
			} else {
				sr.Success = true
				if r, ok := res.(*ServiceResponse); ok {
					*sr = *r
					sr.Service = svc
					sr.Duration = duration
				} else {
					sr.Body, _ = json.Marshal(res)
				}
			}
			mu.Lock()
			results[svc] = sr
			mu.Unlock()
		}(svc, payload)
	}
	wg.Wait()
	return results
}

// ListServiceTypes returns all available service types in the manager
func (is *Manager) ListServiceTypes() []string {
	svcList, err := is.ServiceList()
	if err != nil {
		return nil
	}
	types := make(map[string]struct{})
	for _, svc := range svcList {
		types[string(svc.Type)] = struct{}{}
	}
	out := make([]string, 0, len(types))
	for t := range types {
		out = append(out, t)
	}
	return out
}

// Helper to extract credential fields
func extractCredentialFields(cred Credential, serviceType string) (map[string]string, error) {
	fields := make(map[string]string)
	switch cred.Type {
	case CredentialTypeAPIKey:
		switch v := cred.Data.(type) {
		case map[string]any:
			if k, ok := v["key"].(string); ok {
				fields["key"] = k
			}
		case APIKeyCredential:
			fields["key"] = v.Key
		case *APIKeyCredential:
			fields["key"] = v.Key
		}
	case CredentialTypeBearer:
		switch v := cred.Data.(type) {
		case map[string]any:
			if t, ok := v["token"].(string); ok {
				fields["token"] = t
			}
		case BearerCredential:
			fields["token"] = v.Token
		case *BearerCredential:
			fields["token"] = v.Token
		}
	case CredentialTypeBasicAuth:
		switch v := cred.Data.(type) {
		case map[string]any:
			if u, ok := v["username"].(string); ok {
				fields["username"] = u
			}
			if p, ok := v["password"].(string); ok {
				fields["password"] = p
			}
		case BasicAuthCredential:
			fields["username"] = v.Username
			fields["password"] = v.Password
		case *BasicAuthCredential:
			fields["username"] = v.Username
			fields["password"] = v.Password
		case SMTPAuthCredential:
			fields["username"] = v.Username
			fields["password"] = v.Password
		case *SMTPAuthCredential:
			fields["username"] = v.Username
			fields["password"] = v.Password
		}
	}
	// Add more types as needed
	return fields, nil
}

// Execute dispatches an operation based on service type.
// Generic converter: safely converts any -> T using type assertion or JSON
func convertPayload[T any](payload any) (T, error) {
	var zero T
	if v, ok := payload.(T); ok {
		return v, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return zero, fmt.Errorf("marshal payload to JSON: %w", err)
	}
	if err := json.Unmarshal(b, &zero); err != nil {
		return zero, fmt.Errorf("unmarshal JSON into %T: %w", zero, err)
	}
	return zero, nil
}

// Execute dispatches service execution by service type.
func (is *Manager) Execute(ctx context.Context, serviceName string, payload any) (any, error) {
	service, err := is.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	if !service.Enabled {
		return nil, fmt.Errorf("service %s is disabled", serviceName)
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
		apiCfg, err := convertPayload[APIConfig](service.Config)
		if err != nil {
			execErr = fmt.Errorf("invalid API config: %w", err)
			break
		}
		apiCfg.URL = strings.TrimSpace(apiCfg.URL)
		if apiCfg.URL == "" {
			execErr = fmt.Errorf("empty URL for API request")
			break
		}
		maxRetries := 3
		if apiCfg.RetryCount > 0 {
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
					keys := strings.Split(apiCfg.DataKey, ".")
					body, _, _, err = jsonparser.Get(body, keys...)
				}
				res = &ServiceResponse{
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
		query = strings.TrimSpace(query)
		if query == "" {
			execErr = fmt.Errorf("empty query for graphQL")
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
		emailPayload, err := convertPayload[mail.Mail](payload)
		if err != nil {
			execErr = fmt.Errorf("invalid SMTP payload: %w", err)
			break
		}
		emailPayload.Body = strings.TrimSpace(emailPayload.Body)
		if emailPayload.Body == "" || len(emailPayload.To) == 0 {
			execErr = fmt.Errorf("invalid email body or recipients")
			break
		}
		res, execErr = is.SendEmail(ctx, serviceName, emailPayload)
	case ServiceTypeSMPP:
		msg, err := convertPayload[SMSPayload](payload)
		if err != nil {
			execErr = fmt.Errorf("invalid SMPP payload: %w", err)
			break
		}
		msg.Message = strings.TrimSpace(msg.Message)
		if msg.Message == "" || len(msg.To) == 0 {
			execErr = fmt.Errorf("invalid SMS message or recipient")
			break
		}
		res, execErr = is.SendSMSViaSMPP(ctx, serviceName, msg)
	case ServiceTypeDB:
		query, ok := payload.(string)
		if !ok {
			execErr = fmt.Errorf("invalid payload for DB service, expected query string")
			break
		}
		query = strings.TrimSpace(query)
		if query == "" {
			execErr = fmt.Errorf("empty query for DB")
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

func (is *Manager) SendEmail(ctx context.Context, serviceName string, email mail.Mail) (map[string]any, error) {
	service, err := is.GetService(serviceName)
	if err != nil {
		return nil, err
	}
	cfg, ok := service.Config.(SMTPConfig)
	if !ok {
		return nil, fmt.Errorf("not a valid SMTP configuration for service: %s", serviceName)
	}
	cred, err := is.GetCredential(service.CredentialKey, service.RequireAuth)
	if err != nil {
		return nil, err
	}
	var username, password, awsAccessKey, awsSecretKey, region, charset string
	switch auth := cred.Data.(type) {
	case SMTPAuthCredential:
		username = auth.Username
		password = auth.Password
		awsAccessKey = auth.AwsAccessKey
		awsSecretKey = auth.AwsSecretKey
		region = auth.Region
		charset = auth.Charset
	case *SMTPAuthCredential:
		username = auth.Username
		password = auth.Password
		awsAccessKey = auth.AwsAccessKey
		awsSecretKey = auth.AwsSecretKey
		region = auth.Region
		charset = auth.Charset
	case map[string]any:
		username, _ = auth["username"].(string)
		password, _ = auth["password"].(string)
		awsAccessKey, _ = auth["aws_access_key"].(string)
		awsSecretKey, _ = auth["aws_secret_key"].(string)
		region, _ = auth["aws_region"].(string)
		charset, _ = auth["aws_charset"].(string)
	}
	if cfg.Encryption == "" {
		cfg.Encryption = "tls"
	}
	if charset == "" {
		charset = "utf-8"
	}
	if region == "" {
		region = "us-east-1"
	}
	hasAWS := awsAccessKey != "" && awsSecretKey != ""
	hasBasic := username != "" && password != ""
	if !hasAWS && !hasBasic {
		return nil, errors.New("missing SMTP credentials")
	}
	mailCfg := mail.Config{
		Host:         cfg.Host,
		Username:     username,
		Password:     password,
		Encryption:   cfg.Encryption,
		FromAddress:  cfg.FromAddress,
		Port:         cfg.Port,
		Region:       region,
		AwsAccessKey: awsAccessKey,
		AwsSecretKey: awsSecretKey,
		Charset:      charset,
	}
	mailer := mail.New(mailCfg, nil)
	if email.From == "" {
		email.From = cfg.FromAddress
	}
	err = mailer.Send(email)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"message": "Email sent successfully",
	}, nil
}

func (is *Manager) SendSMSViaSMPP(ctx context.Context, serviceName string, message SMSPayload) (any, error) {
	var client *smpp.Manager
	asyncClient, clientExists := is.GetAsyncClient(serviceName)
	service, err := is.GetService(serviceName)
	if err != nil {
		return nil, err
	}

	cfg, ok := service.Config.(SMPPConfig)
	if !ok {
		return nil, fmt.Errorf("not a valid SMPP configuration for service: %s", serviceName)
	}
	var smppUser, smppPass string
	if service.CredentialKey != "" {
		cred, err := is.GetCredential(service.CredentialKey, service.RequireAuth)
		if err != nil {
			return nil, err
		}
		if cred.Type != CredentialTypeSMPP {
			return nil, fmt.Errorf("expected SMPP credential for service %s", serviceName)
		}
		data, ok := cred.Data.(map[string]any)
		if ok {
			if u, ok := data["username"].(string); ok {
				smppUser = u
			}
			if p, ok := data["password"].(string); ok {
				smppPass = p
			}
		}
	}
	if !clientExists {
		setting := smpp.Setting{
			URL: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
			Auth: smpp.Auth{
				SystemID: smppUser,
				Password: smppPass,
			},
			Register:        pdufield.FinalDeliveryReceipt,
			OnMessageReport: is.onSmppMessageReport,
		}
		manager, err := smpp.NewManager(setting)
		if err != nil {
			return nil, err
		}
		if !message.Test {
			is.SetAsyncClient(serviceName, manager)
		}
		asyncClient = manager
	}
	client, ok = asyncClient.(*smpp.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid async client type for service %s", serviceName)
	}
	from := cfg.SourceAddr
	if message.From != "" {
		from = message.From
	}
	msg := smpp.Message{
		Message: message.Message,
		To:      message.To,
		From:    from,
	}
	resp, err := client.Send(msg)
	if err != nil {
		return nil, err
	}
	is.logger.Info().
		Str("service", serviceName).
		Str("message", message.Message).
		Str("to", message.To).
		Str("username", smppUser).
		Msg("Sending SMS with SMPP credentials")
	return resp, nil
}

func (is *Manager) ExecuteDatabaseQuery(ctx context.Context, serviceName, query string, args ...any) (any, error) {
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
	db, err := is.getDBConnection(serviceName, cfg, username, password)
	if err != nil {
		return nil, err
	}
	var dest []map[string]any
	start := time.Now()
	if len(args) > 0 {
		err = db.SelectContext(ctx, &dest, query, args...)
	} else {
		err = db.SelectContext(ctx, &dest, query)
	}
	duration := time.Since(start)
	if err != nil {
		is.logger.Error().Err(err).Str("service_name", serviceName).Str("driver", cfg.Driver).Dur("duration", duration).Msg("Database query failed")
		return nil, err
	}
	is.logger.Info().Str("service_name", serviceName).Str("driver", cfg.Driver).Dur("duration", duration).Int("rows", len(dest)).Msg("Database query executed")
	return dest, nil
}

func refreshOAuth2Token(auth *OAuth2Credential, logger *log.Logger) error {
	auth.mu.Lock()
	defer auth.mu.Unlock()

	// Double-check validity after acquiring lock
	if auth.AccessToken != "" && time.Until(auth.ExpiresAt) > time.Minute {
		return nil
	}

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
			if !s.Enabled {
				return
			}
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
				client := is.getHTTPClient(cfg.Timeout, cfg.TLSInsecureSkipVerify)
				resp, err := client.Do(req)
				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
				if err != nil || (resp != nil && resp.StatusCode >= 400) {
					errCh <- fmt.Errorf("health check failed for API service %s", s.Name)
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
		return sterrors.New(errMsg)
	}
	return nil
}
