package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/gofiber/fiber/v3/middleware/static"
	"github.com/google/uuid"
	"github.com/oarkflow/sql"
	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/storage"
	"github.com/oarkflow/sql/pkg/utils"
)

type Config struct {
	Version       string
	StaticPath    string
	EnableMocks   bool
	DatabasePath  string
	EncryptionKey string
}

type Server struct {
	app                *fiber.App
	etlManager         *etl.Manager
	integrationManager *integrations.Manager
	store              *storage.Store
	executions         []ExecutionSummary
	configurations     []StoredConfiguration
	config             Config
	oauthSessions      map[string]*oauthSession
	oauthMu            sync.Mutex
}

type StoredConfiguration struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Type       string    `json:"type"`
	Content    string    `json:"content"`
	CreatedAt  time.Time `json:"createdAt"`
	LastUsed   time.Time `json:"lastUsed"`
	Executions int       `json:"executions"`
}

type ExecutionSummary struct {
	ID               string          `json:"id"`
	Config           string          `json:"config"`
	Status           string          `json:"status"`
	RecordsProcessed int             `json:"recordsProcessed"`
	StartTime        time.Time       `json:"startTime"`
	EndTime          *time.Time      `json:"endTime,omitempty"`
	Error            string          `json:"error,omitempty"`
	DetailedMetrics  DetailedMetrics `json:"detailedMetrics,omitempty"`
}

type DetailedMetrics struct {
	Extracted        int64            `json:"extracted"`
	Mapped           int64            `json:"mapped"`
	Transformed      int64            `json:"transformed"`
	Loaded           int64            `json:"loaded"`
	Errors           int64            `json:"errors"`
	WorkerActivities []WorkerActivity `json:"workerActivities"`
}

type WorkerActivity struct {
	Node      string    `json:"node"`
	WorkerID  int       `json:"worker_id"`
	Processed int64     `json:"processed"`
	Failed    int64     `json:"failed"`
	Timestamp time.Time `json:"timestamp"`
	Activity  string    `json:"activity"`
}

type QueryRequest struct {
	Query        string `json:"query"`
	Integration  string `json:"integration,omitempty"`
	SavedQueryID string `json:"savedQueryId,omitempty"`
}

type QueryResponse struct {
	Columns       []string        `json:"columns"`
	Rows          [][]interface{} `json:"rows"`
	RowCount      int             `json:"rowCount"`
	ExecutionTime float64         `json:"executionTime"`
}

type ValidationResponse struct {
	Valid       bool     `json:"valid"`
	Errors      []string `json:"errors"`
	Suggestions []string `json:"suggestions"`
}

type SchemaResponse struct {
	Tables  []string                 `json:"tables"`
	Columns map[string][]utils.Field `json:"columns"`
}

type QueryHistoryEntry struct {
	ID            string  `json:"id"`
	SavedQueryID  string  `json:"savedQueryId,omitempty"`
	Query         string  `json:"query"`
	Name          string  `json:"name,omitempty"`
	Integration   string  `json:"integration,omitempty"`
	Timestamp     string  `json:"timestamp"`
	RowCount      int     `json:"rowCount"`
	ExecutionTime float64 `json:"executionTime"`
	Success       bool    `json:"success"`
	Error         string  `json:"error,omitempty"`
}

type SavedQuerySummary struct {
	ID          string `json:"id"`
	Name        string `json:"name,omitempty"`
	Integration string `json:"integration,omitempty"`
	Query       string `json:"query"`
	CreatedAt   string `json:"createdAt"`
	UpdatedAt   string `json:"updatedAt"`
}

type OAuthProviderTemplate struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	AuthURL     string `json:"authUrl"`
	TokenURL    string `json:"tokenUrl"`
	Scope       string `json:"scope"`
	DocsURL     string `json:"docsUrl,omitempty"`
	Icon        string `json:"icon,omitempty"`
}

type oauthTokenPayload struct {
	AccessToken  string    `json:"accessToken"`
	RefreshToken string    `json:"refreshToken"`
	TokenType    string    `json:"tokenType"`
	Scope        string    `json:"scope"`
	ExpiresIn    int       `json:"expiresIn"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

type oauthSession struct {
	Provider     string
	ClientID     string
	ClientSecret string
	AuthURL      string
	TokenURL     string
	Scope        string
	RedirectURL  string
	CodeVerifier string
	CreatedAt    time.Time
	Token        *oauthTokenPayload
}

const oauthSessionTTL = 10 * time.Minute

var readSourcePattern = regexp.MustCompile(`(?i)\bread_[a-z0-9_]*\s*\(`)
var integrationDirectivePattern = regexp.MustCompile(`(?i)^\s*--\s*integration\s*:\s*([-A-Za-z0-9_]+)\s*;?\s*$`)

type ExecuteConfigRequest struct {
	Config string `json:"config"`
	Type   string `json:"type"` // "bcl", "yaml", "json"
}

type integrationPayload struct {
	Name        string             `json:"name"`
	Type        string             `json:"type"`
	Description string             `json:"description"`
	Config      map[string]any     `json:"config"`
	RequireAuth bool               `json:"requireAuth"`
	Credential  *credentialPayload `json:"credential"`
}

type credentialPayload struct {
	Type         string `json:"type"`
	Key          string `json:"key"`
	Token        string `json:"token"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	AuthURL      string `json:"authUrl"`
	TokenURL     string `json:"tokenUrl"`
	Scope        string `json:"scope"`
	RedirectURL  string `json:"redirectUrl"`
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    string `json:"expiresAt"`
}

type oauthStartRequest struct {
	Provider     string `json:"provider"`
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	AuthURL      string `json:"authUrl"`
	TokenURL     string `json:"tokenUrl"`
	Scope        string `json:"scope"`
	RedirectURL  string `json:"redirectUrl"`
}

type integrationResponse struct {
	ID            string                   `json:"id"`
	Name          string                   `json:"name"`
	Description   string                   `json:"description,omitempty"`
	Type          integrations.ServiceType `json:"type"`
	Status        string                   `json:"status"`
	RequireAuth   bool                     `json:"requireAuth"`
	CredentialKey string                   `json:"credentialKey,omitempty"`
	Config        any                      `json:"config"`
	CreatedAt     time.Time                `json:"createdAt"`
	UpdatedAt     time.Time                `json:"updatedAt"`
}

func newIntegrationResponse(svc integrations.Service) integrationResponse {
	return integrationResponse{
		ID:            svc.Name,
		Name:          svc.Name,
		Description:   svc.Description,
		Type:          svc.Type,
		Status:        defaultStatus(svc.Status),
		RequireAuth:   svc.RequireAuth,
		CredentialKey: svc.CredentialKey,
		Config:        svc.Config,
		CreatedAt:     svc.CreatedAt,
		UpdatedAt:     svc.UpdatedAt,
	}
}

func defaultStatus(status string) string {
	if strings.TrimSpace(status) == "" {
		return "pending"
	}
	return status
}

func defaultOAuthProviders() []OAuthProviderTemplate {
	return []OAuthProviderTemplate{
		{
			Name:        "google",
			DisplayName: "Google Workspace",
			AuthURL:     "https://accounts.google.com/o/oauth2/v2/auth",
			TokenURL:    "https://oauth2.googleapis.com/token",
			Scope:       "https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile",
			DocsURL:     "https://developers.google.com/identity/protocols/oauth2",
			Icon:        "mail",
		},
		{
			Name:        "github",
			DisplayName: "GitHub",
			AuthURL:     "https://github.com/login/oauth/authorize",
			TokenURL:    "https://github.com/login/oauth/access_token",
			Scope:       "repo read:user user:email",
			DocsURL:     "https://docs.github.com/apps/building-oauth-apps/authorizing-oauth-apps",
			Icon:        "github",
		},
		{
			Name:        "facebook",
			DisplayName: "Facebook",
			AuthURL:     "https://www.facebook.com/v18.0/dialog/oauth",
			TokenURL:    "https://graph.facebook.com/v18.0/oauth/access_token",
			Scope:       "public_profile email",
			DocsURL:     "https://developers.facebook.com/docs/facebook-login/",
			Icon:        "facebook",
		},
	}
}

func findOAuthProviderTemplate(name string) (OAuthProviderTemplate, bool) {
	for _, tpl := range defaultOAuthProviders() {
		if tpl.Name == strings.ToLower(name) {
			return tpl, true
		}
	}
	return OAuthProviderTemplate{}, false
}

func randomState() string {
	return uuid.NewString()
}

func generateCodeVerifier() (string, error) {
	const length = 64
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~"
	bytesBuf := make([]byte, length)
	if _, err := rand.Read(bytesBuf); err != nil {
		return "", err
	}
	runes := make([]byte, length)
	for i, b := range bytesBuf {
		runes[i] = charset[int(b)%len(charset)]
	}
	return string(runes), nil
}

func codeChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (s *Server) saveOAuthSession(state string, session *oauthSession) {
	s.oauthMu.Lock()
	s.oauthSessions[state] = session
	s.oauthMu.Unlock()
}

func (s *Server) getOAuthSession(state string) (*oauthSession, bool) {
	s.oauthMu.Lock()
	defer s.oauthMu.Unlock()
	sess, ok := s.oauthSessions[state]
	if !ok {
		return nil, false
	}
	if time.Since(sess.CreatedAt) > oauthSessionTTL {
		delete(s.oauthSessions, state)
		return nil, false
	}
	return sess, true
}

func (s *Server) storeOAuthToken(state string, token *oauthTokenPayload) {
	s.oauthMu.Lock()
	if sess, ok := s.oauthSessions[state]; ok {
		sess.Token = token
	}
	s.oauthMu.Unlock()
}

func (s *Server) popOAuthSession(state string) (*oauthSession, bool) {
	s.oauthMu.Lock()
	sess, ok := s.oauthSessions[state]
	if ok {
		delete(s.oauthSessions, state)
	}
	s.oauthMu.Unlock()
	return sess, ok
}

func parseExpiresIn(value any) int {
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		if v == "" {
			return 0
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

func exchangeOAuthCode(sess *oauthSession, code string) (*oauthTokenPayload, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("client_id", sess.ClientID)
	if strings.TrimSpace(sess.ClientSecret) != "" {
		form.Set("client_secret", sess.ClientSecret)
	}
	form.Set("redirect_uri", sess.RedirectURL)
	form.Set("code_verifier", sess.CodeVerifier)
	request, err := http.NewRequest(http.MethodPost, sess.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("token endpoint returned %s: %s", resp.Status, string(body))
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	accessToken, _ := raw["access_token"].(string)
	if strings.TrimSpace(accessToken) == "" {
		return nil, errors.New("token response missing access_token")
	}
	refreshToken, _ := raw["refresh_token"].(string)
	if refreshToken == "" {
		refreshToken, _ = raw["refreshToken"].(string)
	}
	scope := sess.Scope
	if v, ok := raw["scope"].(string); ok && strings.TrimSpace(v) != "" {
		scope = v
	}
	expiresIn := parseExpiresIn(raw["expires_in"])
	tokenType, _ := raw["token_type"].(string)
	token := &oauthTokenPayload{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		Scope:        scope,
		TokenType:    tokenType,
		ExpiresIn:    expiresIn,
	}
	if expiresIn > 0 {
		token.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)
	} else {
		token.ExpiresAt = time.Now().Add(90 * time.Minute)
	}
	return token, nil
}

func (p *credentialPayload) hasSecret() bool {
	if p == nil {
		return false
	}
	return strings.TrimSpace(p.Key) != "" || strings.TrimSpace(p.Token) != "" ||
		(strings.TrimSpace(p.Username) != "" && strings.TrimSpace(p.Password) != "") ||
		(strings.TrimSpace(p.ClientID) != "" && strings.TrimSpace(p.ClientSecret) != "") ||
		strings.TrimSpace(p.AccessToken) != "" || strings.TrimSpace(p.RefreshToken) != ""
}

func (p *credentialPayload) credentialType() integrations.CredentialType {
	if p == nil {
		return ""
	}
	return integrations.CredentialType(strings.ToLower(strings.TrimSpace(p.Type)))
}

func normalizeIntegrationName(name string) string {
	return strings.TrimSpace(name)
}

func normalizeServiceConfig(svc integrations.Service) (integrations.Service, error) {
	raw, err := json.Marshal(svc)
	if err != nil {
		return integrations.Service{}, err
	}
	var normalized integrations.Service
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return integrations.Service{}, err
	}
	normalized.CreatedAt = svc.CreatedAt
	normalized.UpdatedAt = svc.UpdatedAt
	return normalized, nil
}

func (s *Server) serviceFromPayload(payload integrationPayload, existing *integrations.Service) (integrations.Service, *integrations.Credential, error) {
	svc := integrations.Service{}
	if existing != nil {
		svc = *existing
	} else {
		svc.CreatedAt = time.Now().UTC()
		svc.Enabled = true
	}

	if name := normalizeIntegrationName(payload.Name); name != "" {
		svc.Name = name
	}
	if strings.TrimSpace(svc.Name) == "" {
		return svc, nil, errors.New("integration name is required")
	}
	if payload.Description != "" || existing == nil {
		svc.Description = strings.TrimSpace(payload.Description)
	}
	if payload.Type != "" {
		svc.Type = integrations.ServiceType(strings.ToLower(strings.TrimSpace(payload.Type)))
	}
	if svc.Type == "" {
		return svc, nil, errors.New("integration type is required")
	}
	if payload.Config != nil {
		svc.Config = payload.Config
	}
	if svc.Config == nil {
		svc.Config = map[string]any{}
	}
	if svc.Status == "" {
		svc.Status = "pending"
	}

	svc.RequireAuth = payload.RequireAuth
	var cred *integrations.Credential
	if svc.RequireAuth {
		if payload.Credential != nil && payload.Credential.hasSecret() {
			credential, err := s.buildCredential(svc.Name, payload.Credential, existing)
			if err != nil {
				return svc, nil, err
			}
			cred = credential
			svc.CredentialKey = credential.Key
		} else if existing == nil || existing.CredentialKey == "" {
			return svc, nil, errors.New("credential details are required for authenticated integrations")
		}
	} else {
		svc.CredentialKey = ""
	}

	normalized, err := normalizeServiceConfig(svc)
	if err != nil {
		return svc, nil, err
	}
	if err := normalized.Validate(); err != nil {
		return svc, nil, err
	}
	svc = normalized

	return svc, cred, nil
}

func (s *Server) buildCredential(serviceName string, payload *credentialPayload, existing *integrations.Service) (*integrations.Credential, error) {
	typeValue := payload.credentialType()
	if typeValue == "" {
		return nil, errors.New("credential type is required")
	}
	key := fmt.Sprintf("%s-credential", serviceName)
	if existing != nil && existing.CredentialKey != "" {
		key = existing.CredentialKey
	}
	now := time.Now().UTC()
	cred := &integrations.Credential{
		Key:         key,
		Type:        typeValue,
		Description: fmt.Sprintf("Credential for %s", serviceName),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	switch typeValue {
	case integrations.CredentialTypeAPIKey:
		if strings.TrimSpace(payload.Key) == "" {
			return nil, errors.New("api key value is required")
		}
		cred.Data = integrations.APIKeyCredential{Key: payload.Key}
	case integrations.CredentialTypeBearer:
		if strings.TrimSpace(payload.Token) == "" {
			return nil, errors.New("bearer token is required")
		}
		cred.Data = integrations.BearerCredential{Token: payload.Token}
	case integrations.CredentialTypeBasicAuth:
		if strings.TrimSpace(payload.Username) == "" || strings.TrimSpace(payload.Password) == "" {
			return nil, errors.New("username and password are required for basic auth")
		}
		cred.Data = integrations.BasicAuthCredential{Username: payload.Username, Password: payload.Password}
	case integrations.CredentialTypeOAuth2:
		if strings.TrimSpace(payload.ClientID) == "" {
			return nil, errors.New("client id is required for oauth2")
		}
		if strings.TrimSpace(payload.TokenURL) == "" {
			return nil, errors.New("token url is required for oauth2")
		}
		if strings.TrimSpace(payload.AccessToken) == "" {
			return nil, errors.New("access token is required for oauth2")
		}
		if strings.TrimSpace(payload.RefreshToken) == "" {
			return nil, errors.New("refresh token is required for oauth2")
		}
		expiresAt := time.Now().Add(1 * time.Hour)
		if strings.TrimSpace(payload.ExpiresAt) != "" {
			if parsed, err := time.Parse(time.RFC3339, payload.ExpiresAt); err == nil {
				expiresAt = parsed
			}
		}
		cred.Data = &integrations.OAuth2Credential{
			ClientID:     payload.ClientID,
			ClientSecret: payload.ClientSecret,
			AuthURL:      payload.AuthURL,
			TokenURL:     payload.TokenURL,
			Scope:        payload.Scope,
			RedirectURL:  payload.RedirectURL,
			AccessToken:  payload.AccessToken,
			RefreshToken: payload.RefreshToken,
			ExpiresAt:    expiresAt,
		}
	default:
		return nil, fmt.Errorf("unsupported credential type: %s", payload.Type)
	}

	return cred, nil
}

func NewServer(cfg Config) (*Server, error) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	// Initialize managers
	etlManager := etl.NewManager()
	integrationManager := integrations.New()

	store, err := storage.New(storage.Config{
		Path:          cfg.DatabasePath,
		EncryptionKey: cfg.EncryptionKey,
	})
	if err != nil {
		return nil, err
	}

	server := &Server{
		app:                app,
		etlManager:         etlManager,
		integrationManager: integrationManager,
		store:              store,
		executions:         []ExecutionSummary{},
		configurations:     []StoredConfiguration{},
		config:             cfg,
		oauthSessions:      make(map[string]*oauthSession),
	}

	if err := server.bootstrap(context.Background()); err != nil {
		store.Close()
		return nil, err
	}

	server.setupRoutes()
	return server, nil
}

func (s *Server) bootstrap(ctx context.Context) error {
	creds, err := s.store.ListCredentials(ctx)
	if err != nil {
		return err
	}
	for _, cred := range creds {
		if err := s.integrationManager.AddCredential(cred); err != nil {
			if err := s.integrationManager.UpdateCredential(cred); err != nil {
				return err
			}
		}
	}

	services, err := s.store.ListIntegrations(ctx)
	if err != nil {
		return err
	}
	for _, svc := range services {
		if err := s.integrationManager.AddService(svc); err != nil {
			if err := s.integrationManager.UpdateService(svc); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) setupRoutes() {
	s.app.Use(cors.New())
	s.app.Use(logger.New())

	// Health check
	s.app.Get("/api/health", s.healthHandler)

	// Query endpoints
	s.app.Post("/api/query", s.executeQueryHandler)
	s.app.Post("/api/query/validate", s.validateQueryHandler)
	s.app.Get("/api/query/history", s.getQueryHistoryHandler)
	s.app.Delete("/api/query/history", s.clearQueryHistoryHandler)
	s.app.Post("/api/query/save", s.saveQueryHandler)
	s.app.Get("/api/query/save", s.listSavedQueriesHandler)
	s.app.Put("/api/query/save/:id", s.updateSavedQueryHandler)
	s.app.Delete("/api/query/save/:id", s.deleteSavedQueryHandler)

	// Schema endpoints
	s.app.Get("/api/schema/:integration", s.getSchemaHandler)

	// OAuth endpoints
	s.app.Get("/api/oauth/providers", s.listOAuthProvidersHandler)
	s.app.Post("/api/oauth/start", s.startOAuthHandler)
	s.app.Get("/api/oauth/session/:state", s.getOAuthSessionHandler)
	s.app.Get("/oauth/callback", s.oauthCallbackHandler)

	// Integration endpoints
	s.app.Get("/api/integrations", s.getIntegrationsHandler)
	s.app.Get("/api/integrations/:id", s.getIntegrationHandler)
	s.app.Post("/api/integrations", s.createIntegrationHandler)
	s.app.Put("/api/integrations/:id", s.updateIntegrationHandler)
	s.app.Delete("/api/integrations/:id", s.deleteIntegrationHandler)

	// ETL Pipeline endpoints
	s.app.Get("/api/pipelines", s.getPipelinesHandler)
	s.app.Get("/api/pipelines/:id", s.getPipelineHandler)
	s.app.Post("/api/pipelines", s.createPipelineHandler)
	s.app.Put("/api/pipelines/:id", s.updatePipelineHandler)
	s.app.Delete("/api/pipelines/:id", s.deletePipelineHandler)
	s.app.Post("/api/pipelines/:id/run", s.runPipelineHandler)

	// ETL Run endpoints
	s.app.Get("/api/runs", s.getRunsHandler)
	s.app.Get("/api/runs/:id", s.getRunHandler)

	// Real-time updates endpoint
	s.app.Get("/api/updates", s.getUpdatesHandler)

	// Execute config endpoint
	s.app.Post("/api/execute", s.executeConfigHandler)
	s.app.Get("/api/executions", s.getExecutionsHandler)
	s.app.Get("/api/configurations", s.getConfigurationsHandler)

	// Serve HTML pages
	s.app.Get("/", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/dashboard.html")
	})
	s.app.Get("/integrations", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/integrations.html")
	})
	s.app.Get("/etl", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/etl.html")
	})
	s.app.Get("/adapters", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/adapters.html")
	})
	s.app.Get("/query", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/query.html")
	})
	s.app.Get("/scheduler", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/scheduler.html")
	})
	s.app.Get("/execute", func(c fiber.Ctx) error {
		return c.SendFile(s.config.StaticPath + "/execute.html")
	})

	// Adapter endpoints
	s.app.Get("/api/adapters", s.getAdaptersHandler)
	s.app.Get("/api/adapters/:id", s.getAdapterHandler)
	s.app.Post("/api/adapters", s.createAdapterHandler)
	s.app.Put("/api/adapters/:id", s.updateAdapterHandler)
	s.app.Delete("/api/adapters/:id", s.deleteAdapterHandler)
	s.app.Post("/api/adapters/:id/test", s.testAdapterHandler)

	// Integration test endpoints
	s.app.Post("/api/integrations/:id/test", s.testIntegrationHandler)

	// Scheduler endpoints
	s.app.Get("/api/schedules", s.getSchedulesHandler)
	s.app.Post("/api/schedules", s.createScheduleHandler)
	s.app.Get("/api/schedules/:id", s.getScheduleHandler)
	s.app.Put("/api/schedules/:id", s.updateScheduleHandler)
	s.app.Delete("/api/schedules/:id", s.deleteScheduleHandler)
	s.app.Post("/api/schedules/:id/toggle", s.toggleScheduleHandler)
	s.app.Get("/api/schedule-executions", s.getScheduleExecutionsHandler)

	// Legacy ETL endpoints (for compatibility)
	s.app.Get("/config", s.getConfigHandler)
	s.app.Post("/config", s.createConfigHandler)
	s.app.Get("/etls", s.listETLsHandler)
	s.app.Get("/etls/:id/start", s.startETLHandler)
	s.app.Post("/etls/:id/stop", s.stopETLHandler)
	s.app.Get("/etls/:id", s.getETLDetailsHandler)
	s.app.Get("/etls/:id/metrics", s.getETLMetricsHandler)

	// Serve static files
	s.app.Get("/*", static.New(s.config.StaticPath))
}

func (s *Server) healthHandler(c fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "healthy",
		"version":   s.config.Version,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (s *Server) shouldExecuteRawDatabaseQuery(integrationName, query string) (bool, error) {
	if strings.TrimSpace(integrationName) == "" || strings.TrimSpace(query) == "" {
		return false, nil
	}
	// Keep SQL runtime behavior for virtual/adapter data source functions.
	if readSourcePattern.MatchString(query) {
		return false, nil
	}
	svc, err := s.integrationManager.GetService(strings.TrimSpace(integrationName))
	if err != nil {
		return false, fmt.Errorf("integration %q not found", integrationName)
	}
	if svc.Type == integrations.ServiceTypeDB {
		return true, nil
	}
	return false, errors.New("raw SQL queries are supported only for database integrations")
}

func integrationResultToRecords(result any) ([]map[string]any, error) {
	switch val := result.(type) {
	case []map[string]any:
		return val, nil
	case map[string]any:
		return []map[string]any{val}, nil
	default:
		return nil, fmt.Errorf("database integration returned unsupported result type %T", result)
	}
}

func parseIntegrationDirective(query string) (string, string) {
	lines := strings.Split(query, "\n")
	for idx, line := range lines {
		matches := integrationDirectivePattern.FindStringSubmatch(line)
		if len(matches) != 2 {
			continue
		}
		integration := strings.TrimSpace(matches[1])
		lines = append(lines[:idx], lines[idx+1:]...)
		return integration, strings.TrimSpace(strings.Join(lines, "\n"))
	}
	return "", query
}

func resolveQueryIntegration(req QueryRequest) (integration string, query string, err error) {
	integration = strings.TrimSpace(req.Integration)
	directiveIntegration, cleanedQuery := parseIntegrationDirective(req.Query)
	if directiveIntegration != "" {
		if integration != "" && !strings.EqualFold(integration, directiveIntegration) {
			return "", "", fmt.Errorf("integration conflict: request integration %q does not match query directive %q", integration, directiveIntegration)
		}
		integration = directiveIntegration
		query = cleanedQuery
	} else {
		query = req.Query
	}
	return integration, query, nil
}

func (s *Server) executeQueryHandler(c fiber.Ctx) error {
	var req QueryRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	integrationName, resolvedQuery, resolveErr := resolveQueryIntegration(req)
	if resolveErr != nil {
		return c.Status(400).JSON(fiber.Map{"error": resolveErr.Error()})
	}

	if strings.TrimSpace(resolvedQuery) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Query cannot be empty"})
	}

	ctx := context.Background()
	start := time.Now()
	useRawDB, checkErr := s.shouldExecuteRawDatabaseQuery(integrationName, resolvedQuery)
	if checkErr != nil {
		return c.Status(400).JSON(fiber.Map{"error": checkErr.Error()})
	}

	var records []map[string]any
	var err error
	if useRawDB {
		var dbResult any
		dbResult, err = s.integrationManager.ExecuteDatabaseQuery(ctx, integrationName, resolvedQuery)
		if err == nil {
			records, err = integrationResultToRecords(dbResult)
		}
	} else {
		records, err = sql.Query(ctx, resolvedQuery)
	}
	executionTime := float64(time.Since(start).Milliseconds())

	if err != nil {
		if s.store != nil {
			if _, histErr := s.store.RecordQueryHistory(ctx, storage.QueryHistoryRecord{
				QueryID:       req.SavedQueryID,
				Query:         req.Query,
				Integration:   integrationName,
				ExecutionTime: executionTime,
				Success:       false,
				Error:         err.Error(),
			}); histErr != nil {
				log.Printf("failed to record query error: %v", histErr)
			}
		}
		return c.Status(400).JSON(fiber.Map{
			"error":         err.Error(),
			"executionTime": executionTime,
		})
	}

	// Convert records to the expected format
	var columns []string
	var rows [][]interface{}

	if len(records) > 0 {
		// Get column names from the first record
		for key := range records[0] {
			columns = append(columns, key)
		}

		// Convert records to rows
		for _, record := range records {
			var row []interface{}
			for _, col := range columns {
				row = append(row, record[col])
			}
			rows = append(rows, row)
		}
	}

	response := QueryResponse{
		Columns:       columns,
		Rows:          rows,
		RowCount:      len(rows),
		ExecutionTime: executionTime,
	}

	if s.store != nil {
		if _, histErr := s.store.RecordQueryHistory(ctx, storage.QueryHistoryRecord{
			QueryID:       req.SavedQueryID,
			Query:         req.Query,
			Integration:   integrationName,
			RowCount:      response.RowCount,
			ExecutionTime: executionTime,
			Success:       true,
		}); histErr != nil {
			log.Printf("failed to record query history: %v", histErr)
		}
	}

	return c.JSON(response)
}

func (s *Server) validateQueryHandler(c fiber.Ctx) error {
	var req QueryRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	integrationName, resolvedQuery, resolveErr := resolveQueryIntegration(req)
	if resolveErr != nil {
		return c.JSON(ValidationResponse{
			Valid:       false,
			Errors:      []string{resolveErr.Error()},
			Suggestions: []string{"Use only one integration definition, either request field or -- integration: directive"},
		})
	}

	if strings.TrimSpace(resolvedQuery) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Query cannot be empty"})
	}

	// Raw database integration queries are prevalidated by integration access rules.
	useRawDB, checkErr := s.shouldExecuteRawDatabaseQuery(integrationName, resolvedQuery)
	if checkErr != nil {
		return c.JSON(ValidationResponse{
			Valid:       false,
			Errors:      []string{checkErr.Error()},
			Suggestions: []string{"Select an existing integration or remove integration for SQL runtime queries"},
		})
	}
	if useRawDB {
		if err := s.integrationManager.ValidateDatabaseQuery(integrationName, resolvedQuery); err != nil {
			return c.JSON(ValidationResponse{
				Valid:       false,
				Errors:      []string{err.Error()},
				Suggestions: []string{"Update table/field whitelist and denylist, or adjust the query to use only allowed tables and fields"},
			})
		}
		return c.JSON(ValidationResponse{
			Valid: true,
		})
	}

	// SQL runtime validation - parse the query.
	lexer := sql.NewLexer(resolvedQuery)
	parser := sql.NewParser(lexer)
	parser.ParseQueryStatement()

	var errors []string
	var suggestions []string

	if len(parser.Errors()) > 0 {
		errors = parser.Errors()
		// Add some basic suggestions
		suggestions = []string{
			"Check for syntax errors",
			"Ensure table names are properly quoted",
			"Verify function names are correct",
		}
	}

	return c.JSON(ValidationResponse{
		Valid:       len(errors) == 0,
		Errors:      errors,
		Suggestions: suggestions,
	})
}

func (s *Server) getSchemaHandler(c fiber.Ctx) error {
	integrationName := strings.TrimSpace(c.Params("integration"))
	if integrationName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "integration is required"})
	}

	svc, err := s.integrationManager.GetService(integrationName)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
	}
	if svc.Type != integrations.ServiceTypeDB {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "schema is supported only for database integrations"})
	}

	tables, err := s.integrationManager.ListDatabaseTables(context.Background(), integrationName)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	columns := make(map[string][]utils.Field, len(tables))
	for _, table := range tables {
		columnList, colErr := s.integrationManager.ListDatabaseTableColumns(context.Background(), integrationName, table)
		if colErr != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": colErr.Error()})
		}
		columns[table] = columnList
	}

	return c.JSON(SchemaResponse{
		Tables:  tables,
		Columns: columns,
	})
}

func (s *Server) listOAuthProvidersHandler(c fiber.Ctx) error {
	return c.JSON(defaultOAuthProviders())
}

func (s *Server) startOAuthHandler(c fiber.Ctx) error {
	var req oauthStartRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if tpl, ok := findOAuthProviderTemplate(req.Provider); ok {
		if strings.TrimSpace(req.AuthURL) == "" {
			req.AuthURL = tpl.AuthURL
		}
		if strings.TrimSpace(req.TokenURL) == "" {
			req.TokenURL = tpl.TokenURL
		}
		if strings.TrimSpace(req.Scope) == "" {
			req.Scope = tpl.Scope
		}
	}
	if strings.TrimSpace(req.RedirectURL) == "" {
		req.RedirectURL = fmt.Sprintf("%s://%s/oauth/callback", c.Protocol(), c.Hostname())
	}
	if strings.TrimSpace(req.ClientID) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "clientId is required"})
	}
	if strings.TrimSpace(req.AuthURL) == "" || strings.TrimSpace(req.TokenURL) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "authUrl and tokenUrl are required"})
	}
	verifier, err := generateCodeVerifier()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to generate PKCE verifier"})
	}
	challenge := codeChallenge(verifier)
	state := randomState()
	session := &oauthSession{
		Provider:     strings.ToLower(strings.TrimSpace(req.Provider)),
		ClientID:     req.ClientID,
		ClientSecret: req.ClientSecret,
		AuthURL:      req.AuthURL,
		TokenURL:     req.TokenURL,
		Scope:        strings.TrimSpace(req.Scope),
		RedirectURL:  req.RedirectURL,
		CodeVerifier: verifier,
		CreatedAt:    time.Now(),
	}
	s.saveOAuthSession(state, session)
	authURL, err := url.Parse(req.AuthURL)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid authUrl"})
	}
	query := authURL.Query()
	query.Set("response_type", "code")
	query.Set("client_id", req.ClientID)
	query.Set("redirect_uri", req.RedirectURL)
	if session.Scope != "" {
		query.Set("scope", session.Scope)
	}
	query.Set("state", state)
	query.Set("access_type", "offline")
	query.Set("prompt", "consent")
	query.Set("code_challenge_method", "S256")
	query.Set("code_challenge", challenge)
	authURL.RawQuery = query.Encode()

	return c.JSON(fiber.Map{
		"state":            state,
		"authorizationUrl": authURL.String(),
		"redirectUrl":      req.RedirectURL,
	})
}

func (s *Server) getOAuthSessionHandler(c fiber.Ctx) error {
	state := c.Params("state")
	if strings.TrimSpace(state) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "missing state"})
	}
	s.oauthMu.Lock()
	sess, ok := s.oauthSessions[state]
	if !ok {
		s.oauthMu.Unlock()
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "oauth session not found"})
	}
	if time.Since(sess.CreatedAt) > oauthSessionTTL {
		delete(s.oauthSessions, state)
		s.oauthMu.Unlock()
		return c.Status(fiber.StatusGone).JSON(fiber.Map{"error": "oauth session expired"})
	}
	if sess.Token == nil {
		s.oauthMu.Unlock()
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "pending"})
	}
	token := *sess.Token
	delete(s.oauthSessions, state)
	s.oauthMu.Unlock()
	expiresAt := token.ExpiresAt.Format(time.RFC3339)
	return c.JSON(fiber.Map{
		"provider":     sess.Provider,
		"accessToken":  token.AccessToken,
		"refreshToken": token.RefreshToken,
		"tokenType":    token.TokenType,
		"scope":        token.Scope,
		"expiresIn":    token.ExpiresIn,
		"expiresAt":    expiresAt,
	})
}

func (s *Server) oauthCallbackHandler(c fiber.Ctx) error {
	state := c.Query("state")
	if strings.TrimSpace(state) == "" {
		return c.Status(fiber.StatusBadRequest).SendString("Missing OAuth state")
	}
	if errMsg := c.Query("error"); errMsg != "" {
		s.popOAuthSession(state)
		return c.Status(fiber.StatusBadRequest).SendString(fmt.Sprintf("Authentication failed: %s", errMsg))
	}
	code := c.Query("code")
	if strings.TrimSpace(code) == "" {
		return c.Status(fiber.StatusBadRequest).SendString("Missing authorization code")
	}
	sess, ok := s.getOAuthSession(state)
	if !ok {
		return c.Status(fiber.StatusBadRequest).SendString("OAuth session expired or invalid")
	}
	token, err := exchangeOAuthCode(sess, code)
	if err != nil {
		return c.Status(fiber.StatusBadGateway).SendString(fmt.Sprintf("Failed to exchange authorization code: %v", err))
	}
	s.storeOAuthToken(state, token)
	return c.SendString("<html><body><div style='font-family: sans-serif; text-align:center; padding:40px;'>Authentication complete. You can close this window.</div><script>setTimeout(function(){ window.close(); }, 1500);</script></body></html>")
}

func (s *Server) getIntegrationsHandler(c fiber.Ctx) error {
	services, err := s.store.ListIntegrations(context.Background())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to list integrations"})
	}
	responses := make([]integrationResponse, len(services))
	for i, svc := range services {
		responses[i] = newIntegrationResponse(svc)
	}
	return c.JSON(responses)
}

func (s *Server) getIntegrationHandler(c fiber.Ctx) error {
	name := c.Params("id")
	svc, err := s.store.GetIntegration(context.Background(), name)
	if err != nil {
		if errors.Is(err, storage.ErrIntegrationNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "integration not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load integration"})
	}
	return c.JSON(newIntegrationResponse(svc))
}

func (s *Server) createIntegrationHandler(c fiber.Ctx) error {
	var payload integrationPayload
	if err := c.Bind().Body(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	svc, cred, err := s.serviceFromPayload(payload, nil)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	ctx := context.Background()
	if cred != nil {
		if err := s.store.UpsertCredential(ctx, *cred); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to store credential"})
		}
		if err := s.integrationManager.UpdateCredential(*cred); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to register credential"})
		}
	}
	if err := s.store.CreateIntegration(ctx, svc); err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "unique") {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "integration name already exists"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to persist integration"})
	}
	saved, err := s.store.GetIntegration(ctx, svc.Name)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to reload integration"})
	}
	if err := s.integrationManager.AddService(saved); err != nil {
		if err := s.integrationManager.UpdateService(saved); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to register integration"})
		}
	}
	return c.Status(fiber.StatusCreated).JSON(newIntegrationResponse(saved))
}

func (s *Server) updateIntegrationHandler(c fiber.Ctx) error {
	name := c.Params("id")
	ctx := context.Background()
	existing, err := s.store.GetIntegration(ctx, name)
	if err != nil {
		if errors.Is(err, storage.ErrIntegrationNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "integration not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load integration"})
	}
	var payload integrationPayload
	if err := c.Bind().Body(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if payload.Name != "" && normalizeIntegrationName(payload.Name) != existing.Name {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "renaming integrations is not supported yet"})
	}
	payload.Name = existing.Name
	svc, cred, err := s.serviceFromPayload(payload, &existing)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if cred != nil {
		if err := s.store.UpsertCredential(ctx, *cred); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to store credential"})
		}
		if err := s.integrationManager.UpdateCredential(*cred); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to register credential"})
		}
	}
	if !svc.RequireAuth && existing.RequireAuth && existing.CredentialKey != "" {
		_ = s.store.DeleteCredential(ctx, existing.CredentialKey)
		_ = s.integrationManager.DeleteCredential(existing.CredentialKey)
	}
	if err := s.store.UpdateIntegration(ctx, svc); err != nil {
		if errors.Is(err, storage.ErrIntegrationNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "integration not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update integration"})
	}
	saved, err := s.store.GetIntegration(ctx, svc.Name)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to reload integration"})
	}
	if err := s.integrationManager.UpdateService(saved); err != nil {
		if err := s.integrationManager.AddService(saved); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to register integration"})
		}
	}
	return c.JSON(newIntegrationResponse(saved))
}

func (s *Server) deleteIntegrationHandler(c fiber.Ctx) error {
	name := c.Params("id")
	ctx := context.Background()
	svc, err := s.store.GetIntegration(ctx, name)
	if err != nil {
		if errors.Is(err, storage.ErrIntegrationNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "integration not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load integration"})
	}
	if err := s.store.DeleteIntegration(ctx, name); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to delete integration"})
	}
	if svc.CredentialKey != "" {
		_ = s.store.DeleteCredential(ctx, svc.CredentialKey)
		_ = s.integrationManager.DeleteCredential(svc.CredentialKey)
	}
	_ = s.integrationManager.DeleteService(name)
	return c.JSON(fiber.Map{"id": name, "message": "integration deleted successfully"})
}

// Placeholder implementations for other endpoints
func (s *Server) getQueryHistoryHandler(c fiber.Ctx) error {
	ctx := context.Background()
	records, err := s.store.ListQueryHistory(ctx, 50)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load query history"})
	}

	history := make([]QueryHistoryEntry, len(records))
	for i, rec := range records {
		history[i] = QueryHistoryEntry{
			ID:            rec.ID,
			SavedQueryID:  rec.QueryID,
			Query:         rec.Query,
			Name:          rec.Name,
			Integration:   rec.Integration,
			Timestamp:     rec.CreatedAt.Format(time.RFC3339),
			RowCount:      rec.RowCount,
			ExecutionTime: rec.ExecutionTime,
			Success:       rec.Success,
			Error:         rec.Error,
		}
	}

	return c.JSON(history)
}

func (s *Server) clearQueryHistoryHandler(c fiber.Ctx) error {
	if err := s.store.ClearQueryHistory(context.Background()); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to clear query history"})
	}
	return c.JSON(fiber.Map{"message": "query history cleared"})
}

func (s *Server) saveQueryHandler(c fiber.Ctx) error {
	var req struct {
		Query       string `json:"query"`
		Name        string `json:"name,omitempty"`
		Integration string `json:"integration,omitempty"`
	}
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if strings.TrimSpace(req.Query) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Query cannot be empty"})
	}

	rec, err := s.store.SaveQuery(context.Background(), req.Name, req.Query, req.Integration)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to save query"})
	}

	return c.Status(fiber.StatusCreated).JSON(SavedQuerySummary{
		ID:          rec.ID,
		Name:        rec.Name,
		Integration: rec.Integration,
		Query:       rec.Query,
		CreatedAt:   rec.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   rec.UpdatedAt.Format(time.RFC3339),
	})
}

func (s *Server) listSavedQueriesHandler(c fiber.Ctx) error {
	records, err := s.store.ListSavedQueries(context.Background())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to list saved queries"})
	}

	resp := make([]SavedQuerySummary, len(records))
	for i, rec := range records {
		resp[i] = SavedQuerySummary{
			ID:          rec.ID,
			Name:        rec.Name,
			Integration: rec.Integration,
			Query:       rec.Query,
			CreatedAt:   rec.CreatedAt.Format(time.RFC3339),
			UpdatedAt:   rec.UpdatedAt.Format(time.RFC3339),
		}
	}
	return c.JSON(resp)
}

func (s *Server) updateSavedQueryHandler(c fiber.Ctx) error {
	id := c.Params("id")
	if strings.TrimSpace(id) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "missing query id"})
	}

	var req struct {
		Name        *string `json:"name"`
		Query       *string `json:"query"`
		Integration *string `json:"integration"`
	}
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx := context.Background()
	rec, err := s.store.GetSavedQuery(ctx, id)
	if err != nil {
		if errors.Is(err, storage.ErrSavedQueryNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "saved query not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load saved query"})
	}

	if req.Name != nil {
		rec.Name = *req.Name
	}
	if req.Query != nil {
		rec.Query = *req.Query
	}
	if req.Integration != nil {
		rec.Integration = *req.Integration
	}

	if strings.TrimSpace(rec.Query) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "query cannot be empty"})
	}

	updated, err := s.store.UpdateSavedQuery(ctx, rec)
	if err != nil {
		if errors.Is(err, storage.ErrSavedQueryNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "saved query not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update saved query"})
	}

	return c.JSON(SavedQuerySummary{
		ID:          updated.ID,
		Name:        updated.Name,
		Integration: updated.Integration,
		Query:       updated.Query,
		CreatedAt:   updated.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   updated.UpdatedAt.Format(time.RFC3339),
	})
}

func (s *Server) deleteSavedQueryHandler(c fiber.Ctx) error {
	id := c.Params("id")
	if strings.TrimSpace(id) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "missing query id"})
	}
	if err := s.store.DeleteSavedQuery(context.Background(), id); err != nil {
		if errors.Is(err, storage.ErrSavedQueryNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "saved query not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to delete saved query"})
	}
	return c.JSON(fiber.Map{"id": id, "message": "saved query deleted"})
}

func (s *Server) getPipelinesHandler(c fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON([]map[string]interface{}{})
	}
	// Mock pipelines
	pipelines := []map[string]interface{}{
		{
			"id":          "pipeline-1",
			"name":        "Customer Data Pipeline",
			"description": "Process customer data from CSV to database",
			"status":      "running",
			"nodes":       []interface{}{map[string]interface{}{"id": 1, "label": "Source", "type": "source"}, map[string]interface{}{"id": 2, "label": "Transform", "type": "transform"}, map[string]interface{}{"id": 3, "label": "Load", "type": "load"}},
			"schedule":    map[string]interface{}{"type": "cron", "value": "0 */6 * * *"},
			"lastRun":     "2025-01-21T08:00:00Z",
			"createdAt":   "2025-01-15T10:30:00Z",
		},
		{
			"id":          "pipeline-2",
			"name":        "Order Processing Pipeline",
			"description": "Transform and load order data",
			"status":      "completed",
			"nodes":       []interface{}{map[string]interface{}{"id": 1, "label": "Source", "type": "source"}, map[string]interface{}{"id": 2, "label": "Filter", "type": "filter"}, map[string]interface{}{"id": 3, "label": "Load", "type": "load"}},
			"schedule":    map[string]interface{}{"type": "manual", "value": ""},
			"lastRun":     "2025-01-21T07:30:00Z",
			"createdAt":   "2025-01-16T08:00:00Z",
		},
		{
			"id":          "pipeline-3",
			"name":        "Product Sync Pipeline",
			"description": "Sync product data from API to database",
			"status":      "failed",
			"nodes":       []interface{}{map[string]interface{}{"id": 1, "label": "API Source", "type": "source"}, map[string]interface{}{"id": 2, "label": "Transform", "type": "transform"}, map[string]interface{}{"id": 3, "label": "Load", "type": "load"}},
			"schedule":    map[string]interface{}{"type": "interval", "value": "30m"},
			"lastRun":     "2025-01-21T06:45:00Z",
			"createdAt":   "2025-01-17T12:00:00Z",
		},
	}
	return c.JSON(pipelines)
}

func (s *Server) getPipelineHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation with more detailed data
	return c.JSON(map[string]interface{}{
		"id":          id,
		"name":        "Customer Data Pipeline",
		"description": "Process customer data from CSV to database",
		"status":      "running",
		"config":      "source:\n  type: file\n  path: customers.csv\ntransform:\n  - type: filter\n    condition: \"status == 'active'\"\nload:\n  type: database\n  table: customers",
		"nodes":       []interface{}{map[string]interface{}{"id": 1, "label": "Source", "type": "source"}, map[string]interface{}{"id": 2, "label": "Transform", "type": "transform"}, map[string]interface{}{"id": 3, "label": "Load", "type": "load"}},
		"edges":       []interface{}{map[string]interface{}{"from": 1, "to": 2}, map[string]interface{}{"from": 2, "to": 3}},
		"schedule":    map[string]interface{}{"type": "cron", "value": "0 */6 * * *", "timezone": "UTC", "retry": true, "maxRetries": 3, "retryDelay": 60},
		"lastRun":     "2025-01-21T08:00:00Z",
		"createdAt":   "2025-01-15T10:30:00Z",
	})
}

func (s *Server) createPipelineHandler(c fiber.Ctx) error {
	var pipeline map[string]interface{}
	if err := c.Bind().Body(&pipeline); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}
	return c.Status(201).JSON(pipeline)
}

func (s *Server) updatePipelineHandler(c fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) deletePipelineHandler(c fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) runPipelineHandler(c fiber.Ctx) error {
	id := c.Params("id")

	// Create a new execution
	executionID := fmt.Sprintf("run-%d", time.Now().Unix())

	// Add to executions list
	execution := ExecutionSummary{
		ID:        executionID,
		Config:    fmt.Sprintf("Pipeline %s execution", id),
		Status:    "running",
		StartTime: time.Now(),
	}
	s.executions = append(s.executions, execution)

	// Simulate pipeline execution in background
	go func() {
		// Simulate work
		time.Sleep(5 * time.Second)

		// Update execution status
		for i := range s.executions {
			if s.executions[i].ID == executionID {
				s.executions[i].Status = "completed"
				s.executions[i].RecordsProcessed = int(1000 + (time.Now().Unix() % 5000))
				s.executions[i].DetailedMetrics = DetailedMetrics{
					Extracted:   1000 + (time.Now().Unix() % 1000),
					Mapped:      900 + (time.Now().Unix() % 100),
					Transformed: 850 + (time.Now().Unix() % 100),
					Loaded:      800 + (time.Now().Unix() % 200),
					Errors:      time.Now().Unix() % 10,
					WorkerActivities: []WorkerActivity{
						{Node: "source", WorkerID: 1, Processed: 1000, Failed: 0, Timestamp: time.Now(), Activity: "Extraction completed"},
						{Node: "transform", WorkerID: 1, Processed: 950, Failed: 5, Timestamp: time.Now(), Activity: "Transformation completed"},
						{Node: "load", WorkerID: 1, Processed: 900, Failed: 2, Timestamp: time.Now(), Activity: "Load completed"},
					},
				}
				now := time.Now()
				s.executions[i].EndTime = &now
				break
			}
		}
	}()

	return c.JSON(map[string]interface{}{
		"runId":   executionID,
		"status":  "running",
		"message": "Pipeline execution started",
	})
}

func (s *Server) getRunsHandler(c fiber.Ctx) error {
	return c.JSON([]map[string]interface{}{})
}

func (s *Server) getRunHandler(c fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id})
}

func (s *Server) executeConfigHandler(c fiber.Ctx) error {
	var req ExecuteConfigRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if strings.TrimSpace(req.Config) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Config cannot be empty"})
	}

	// Generate execution ID
	executionID := fmt.Sprintf("exec-%d", time.Now().Unix())

	// Store configuration if not already stored
	configID := fmt.Sprintf("config-%d", time.Now().Unix())
	configName := fmt.Sprintf("Config %s", time.Now().Format("2006-01-02 15:04:05"))

	// Check if this exact config already exists
	configExists := false
	for i, storedConfig := range s.configurations {
		if storedConfig.Content == req.Config && storedConfig.Type == req.Type {
			configID = storedConfig.ID
			configName = storedConfig.Name
			s.configurations[i].LastUsed = time.Now()
			s.configurations[i].Executions++
			configExists = true
			break
		}
	}

	if !configExists {
		storedConfig := StoredConfiguration{
			ID:         configID,
			Name:       configName,
			Type:       req.Type,
			Content:    req.Config,
			CreatedAt:  time.Now(),
			LastUsed:   time.Now(),
			Executions: 1,
		}
		s.configurations = append(s.configurations, storedConfig)
	}

	// Add to executions list
	execution := ExecutionSummary{
		ID:        executionID,
		Config:    req.Config,
		Status:    "running",
		StartTime: time.Now(),
	}
	s.executions = append(s.executions, execution)

	// Parse config based on type
	var cfg *config.Config
	var err error
	switch strings.ToLower(req.Type) {
	case "bcl":
		cfg, err = config.LoadBCLFromString(req.Config)
	case "yaml", "yml":
		cfg, err = config.LoadYamlFromString(req.Config)
	case "json":
		cfg, err = config.LoadJsonFromString(req.Config)
	default:
		err = fmt.Errorf("unsupported config type: %s", req.Type)
	}

	if err != nil {
		// Update execution status
		for i := range s.executions {
			if s.executions[i].ID == executionID {
				s.executions[i].Status = "failed"
				s.executions[i].Error = err.Error()
				now := time.Now()
				s.executions[i].EndTime = &now
				break
			}
		}
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// Run ETL in goroutine
	go func() {
		recordsProcessed := 0
		var allMetrics etl.Metrics
		ids, err := s.etlManager.Prepare(cfg)
		if err != nil {
			// Update execution status
			for i := range s.executions {
				if s.executions[i].ID == executionID {
					s.executions[i].Status = "failed"
					s.executions[i].Error = err.Error()
					now := time.Now()
					s.executions[i].EndTime = &now
					break
				}
			}
			return
		}

		for _, id := range ids {
			if err := s.etlManager.Start(context.Background(), id); err != nil {
				// Update execution status
				for i := range s.executions {
					if s.executions[i].ID == executionID {
						s.executions[i].Status = "failed"
						s.executions[i].Error = err.Error()
						now := time.Now()
						s.executions[i].EndTime = &now
						break
					}
				}
				return
			}
		}

		// Collect final metrics after all ETL jobs complete
		time.Sleep(2 * time.Second) // Give time for final metrics to be recorded

		// Collect all metrics from running ETL instances
		for _, id := range ids {
			if etlInstance, exists := s.etlManager.GetETL(id); exists && etlInstance != nil {
				metrics := etlInstance.GetMetrics()
				recordsProcessed += int(metrics.Loaded)
				allMetrics.Extracted += metrics.Extracted
				allMetrics.Mapped += metrics.Mapped
				allMetrics.Transformed += metrics.Transformed
				allMetrics.Loaded += metrics.Loaded
				allMetrics.Errors += metrics.Errors
				// Collect WorkerActivities from all instances
				allMetrics.WorkerActivities = append(allMetrics.WorkerActivities, metrics.WorkerActivities...)
			}
		}

		// Update execution status with detailed metrics
		for i := range s.executions {
			if s.executions[i].ID == executionID {
				s.executions[i].Status = "completed"
				s.executions[i].RecordsProcessed = recordsProcessed
				s.executions[i].DetailedMetrics = DetailedMetrics{
					Extracted:        allMetrics.Extracted,
					Mapped:           allMetrics.Mapped,
					Transformed:      allMetrics.Transformed,
					Loaded:           allMetrics.Loaded,
					Errors:           allMetrics.Errors,
					WorkerActivities: convertWorkerActivities(allMetrics.WorkerActivities),
				}
				now := time.Now()
				s.executions[i].EndTime = &now
				break
			}
		}
	}()

	return c.JSON(fiber.Map{
		"executionId": executionID,
		"message":     "ETL execution started",
	})
}

func (s *Server) getExecutionsHandler(c fiber.Ctx) error {
	return c.JSON(s.executions)
}

func (s *Server) getConfigurationsHandler(c fiber.Ctx) error {
	// Return stored configurations
	configurations := make([]map[string]interface{}, len(s.configurations))
	for i, config := range s.configurations {
		configurations[i] = map[string]interface{}{
			"id":          config.ID,
			"name":        config.Name,
			"type":        config.Type,
			"description": fmt.Sprintf("Executed %d times, last used %s", config.Executions, config.LastUsed.Format("2006-01-02 15:04:05")),
			"path":        fmt.Sprintf("stored-%s", config.Type),
			"createdAt":   config.CreatedAt.Format(time.RFC3339),
			"lastUsed":    config.LastUsed.Format(time.RFC3339),
			"executions":  config.Executions,
		}
	}

	return c.JSON(configurations)
}

func convertWorkerActivities(activities []etl.WorkerActivity) []WorkerActivity {
	result := make([]WorkerActivity, len(activities))
	for i, activity := range activities {
		result[i] = WorkerActivity{
			Node:      activity.Node,
			WorkerID:  activity.WorkerID,
			Processed: activity.Processed,
			Failed:    activity.Failed,
			Timestamp: activity.Timestamp,
			Activity:  activity.Activity,
		}
	}
	return result
}

// Legacy ETL handlers
func (s *Server) getConfigHandler(c fiber.Ctx) error {
	return c.JSON(config.Config{})
}

func (s *Server) createConfigHandler(c fiber.Ctx) error {
	var cfg config.Config
	if err := c.Bind().Body(&cfg); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid config"})
	}
	return c.JSON(map[string]interface{}{"message": "Config created"})
}

func (s *Server) listETLsHandler(c fiber.Ctx) error {
	return c.SendString("<h1>ETL Jobs</h1><p>Mock ETL list</p>")
}

func (s *Server) startETLHandler(c fiber.Ctx) error {
	id := c.Params("id")
	return c.Redirect().To(fmt.Sprintf("/etls/%s", id))
}

func (s *Server) stopETLHandler(c fiber.Ctx) error {
	id := c.Params("id")
	return c.JSON(map[string]interface{}{"id": id, "message": "ETL stopped"})
}

func (s *Server) getETLDetailsHandler(c fiber.Ctx) error {
	id := c.Params("id")
	return c.SendString(fmt.Sprintf("<h1>ETL %s Details</h1><p>Mock details</p>", id))
}

func (s *Server) getETLMetricsHandler(c fiber.Ctx) error {
	id := c.Params("id")
	if !s.config.EnableMocks {
		return c.JSON(map[string]interface{}{
			"id":      id,
			"metrics": map[string]interface{}{},
		})
	}
	return c.JSON(map[string]interface{}{
		"id": id,
		"metrics": map[string]interface{}{
			"processed": 1000,
			"duration":  60.5,
		},
	})
}

// Adapter handlers
func (s *Server) getAdaptersHandler(c fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON([]map[string]interface{}{})
	}
	// Mock adapters
	adapters := []map[string]interface{}{
		{
			"id":          "1",
			"name":        "Customer CSV Source",
			"type":        "source",
			"kind":        "file",
			"description": "Read customer data from CSV",
			"config":      map[string]interface{}{"format": "csv", "path": "/data/customers.csv", "hasHeader": true},
			"enabled":     true,
			"createdAt":   "2025-01-15T10:30:00Z",
			"lastUsed":    "2025-01-20T14:20:00Z",
		},
		{
			"id":          "2",
			"name":        "Order Transform",
			"type":        "transform",
			"kind":        "mapper",
			"description": "Transform order data",
			"config":      map[string]interface{}{"mappings": []interface{}{map[string]interface{}{"from": "order_id", "to": "id"}, map[string]interface{}{"from": "customer_name", "to": "customer"}}},
			"enabled":     true,
			"createdAt":   "2025-01-16T08:00:00Z",
			"lastUsed":    "2025-01-21T09:15:00Z",
		},
	}
	return c.JSON(adapters)
}

func (s *Server) getAdapterHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":          id,
		"name":        "Mock Adapter",
		"type":        "source",
		"kind":        "file",
		"description": "Mock adapter",
		"config":      map[string]interface{}{},
		"enabled":     true,
	})
}

func (s *Server) createAdapterHandler(c fiber.Ctx) error {
	var adapter map[string]interface{}
	if err := c.Bind().Body(&adapter); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Mock implementation
	adapter["id"] = "adapter-" + fmt.Sprintf("%d", time.Now().Unix())
	adapter["createdAt"] = time.Now().Format(time.RFC3339)

	return c.Status(201).JSON(adapter)
}

func (s *Server) updateAdapterHandler(c fiber.Ctx) error {
	id := c.Params("id")
	var updates map[string]interface{}
	if err := c.Bind().Body(&updates); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Adapter updated successfully",
	})
}

func (s *Server) deleteAdapterHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Adapter deleted successfully",
	})
}

func (s *Server) testAdapterHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation - simulate test
	time.Sleep(1 * time.Second)
	return c.JSON(map[string]interface{}{
		"id":      id,
		"status":  "success",
		"message": "Adapter test completed successfully",
		"metrics": map[string]interface{}{
			"latency": "150ms",
			"records": 100,
			"success": true,
		},
	})
}

func (s *Server) testIntegrationHandler(c fiber.Ctx) error {
	name := c.Params("id")
	svc, err := s.integrationManager.GetService(name)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "integration not found"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch svc.Type {
	case integrations.ServiceTypeAPI:
		resp, err := s.integrationManager.ExecuteAPIRequest(ctx, svc.Name, nil)
		if err != nil {
			_ = s.store.UpdateIntegrationStatus(context.Background(), svc.Name, "error")
			return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		_ = s.store.UpdateIntegrationStatus(context.Background(), svc.Name, "connected")
		return c.JSON(fiber.Map{
			"id":      svc.Name,
			"status":  "success",
			"message": "HTTP endpoint responded successfully",
			"metrics": fiber.Map{
				"latency":     "unknown",
				"status":      "connected",
				"lastChecked": time.Now().Format(time.RFC3339),
			},
		})
	default:
		return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{"error": "testing not implemented for this integration type"})
	}
}

// Real-time updates endpoint for polling
func (s *Server) getUpdatesHandler(c fiber.Ctx) error {
	lastUpdate := c.Query("since")

	// Filter executions based on last update time
	var filteredExecutions []ExecutionSummary
	if lastUpdate != "" {
		lastTime, err := time.Parse(time.RFC3339, lastUpdate)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid timestamp format"})
		}

		for _, exec := range s.executions {
			if exec.StartTime.After(lastTime) {
				filteredExecutions = append(filteredExecutions, exec)
			}
		}
	} else {
		filteredExecutions = s.executions
	}

	return c.JSON(map[string]interface{}{
		"timestamp":  time.Now().Format(time.RFC3339),
		"executions": filteredExecutions,
	})
}

// Scheduler handlers
func (s *Server) getSchedulesHandler(c fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON([]map[string]interface{}{})
	}
	// Mock schedules
	schedules := []map[string]interface{}{
		{
			"id":           "schedule-1",
			"name":         "Daily Customer Sync",
			"pipelineId":   "pipeline-1",
			"pipelineName": "Customer Data Pipeline",
			"type":         "cron",
			"schedule":     map[string]interface{}{"cron": "0 2 * * *"},
			"timezone":     "UTC",
			"enabled":      true,
			"nextRun":      time.Now().Add(24 * time.Hour).Format(time.RFC3339),
			"retry":        map[string]interface{}{"maxRetries": 3, "retryDelay": 60},
			"createdAt":    "2025-01-15T10:30:00Z",
			"lastRun":      "2025-01-21T02:00:00Z",
		},
		{
			"id":           "schedule-2",
			"name":         "Hourly Order Processing",
			"pipelineId":   "pipeline-2",
			"pipelineName": "Order Processing Pipeline",
			"type":         "interval",
			"schedule":     map[string]interface{}{"interval": "1 hours"},
			"timezone":     "America/New_York",
			"enabled":      true,
			"nextRun":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
			"retry":        nil,
			"createdAt":    "2025-01-16T08:00:00Z",
			"lastRun":      "2025-01-21T08:00:00Z",
		},
		{
			"id":           "schedule-3",
			"name":         "Weekly Product Sync",
			"pipelineId":   "pipeline-3",
			"pipelineName": "Product Sync Pipeline",
			"type":         "cron",
			"schedule":     map[string]interface{}{"cron": "0 3 * * 0"},
			"timezone":     "Europe/London",
			"enabled":      false,
			"nextRun":      "",
			"retry":        map[string]interface{}{"maxRetries": 5, "retryDelay": 120},
			"createdAt":    "2025-01-17T12:00:00Z",
			"lastRun":      "2025-01-14T03:00:00Z",
		},
	}
	return c.JSON(schedules)
}

func (s *Server) createScheduleHandler(c fiber.Ctx) error {
	var schedule map[string]interface{}
	if err := c.Bind().Body(&schedule); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Mock implementation
	schedule["id"] = "schedule-" + fmt.Sprintf("%d", time.Now().Unix())
	schedule["createdAt"] = time.Now().Format(time.RFC3339)
	schedule["lastRun"] = ""
	schedule["nextRun"] = calculateNextRun(schedule["type"].(string), schedule["schedule"].(map[string]interface{}))

	return c.Status(201).JSON(schedule)
}

func (s *Server) getScheduleHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":           id,
		"name":         "Mock Schedule",
		"pipelineId":   "pipeline-1",
		"pipelineName": "Customer Data Pipeline",
		"type":         "cron",
		"schedule":     map[string]interface{}{"cron": "0 2 * * *"},
		"timezone":     "UTC",
		"enabled":      true,
		"nextRun":      time.Now().Add(24 * time.Hour).Format(time.RFC3339),
		"retry":        map[string]interface{}{"maxRetries": 3, "retryDelay": 60},
		"createdAt":    "2025-01-15T10:30:00Z",
		"lastRun":      "2025-01-21T02:00:00Z",
	})
}

func (s *Server) updateScheduleHandler(c fiber.Ctx) error {
	id := c.Params("id")
	var updates map[string]interface{}
	if err := c.Bind().Body(&updates); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Schedule updated successfully",
	})
}

func (s *Server) deleteScheduleHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Schedule deleted successfully",
	})
}

func (s *Server) toggleScheduleHandler(c fiber.Ctx) error {
	id := c.Params("id")
	// Mock implementation
	return c.JSON(map[string]interface{}{
		"id":      id,
		"message": "Schedule toggled successfully",
		"enabled": true,
	})
}

func (s *Server) getScheduleExecutionsHandler(c fiber.Ctx) error {
	if !s.config.EnableMocks {
		return c.JSON([]map[string]interface{}{})
	}
	// Mock execution history
	executions := []map[string]interface{}{
		{
			"id":           "exec-1",
			"scheduleId":   "schedule-1",
			"scheduleName": "Daily Customer Sync",
			"status":       "completed",
			"startTime":    "2025-01-21T02:00:00Z",
			"endTime":      "2025-01-21T02:05:30Z",
			"error":        "",
		},
		{
			"id":           "exec-2",
			"scheduleId":   "schedule-2",
			"scheduleName": "Hourly Order Processing",
			"status":       "failed",
			"startTime":    "2025-01-21T08:00:00Z",
			"endTime":      "2025-01-21T08:01:15Z",
			"error":        "Connection timeout",
		},
		{
			"id":           "exec-3",
			"scheduleId":   "schedule-2",
			"scheduleName": "Hourly Order Processing",
			"status":       "running",
			"startTime":    "2025-01-21T09:00:00Z",
			"endTime":      "",
			"error":        "",
		},
	}
	return c.JSON(executions)
}

// Helper function to calculate next run time
func calculateNextRun(scheduleType string, schedule map[string]interface{}) string {
	now := time.Now()

	switch scheduleType {
	case "cron":
		// Mock calculation - in real implementation, use a cron library
		return now.Add(24 * time.Hour).Format(time.RFC3339)
	case "interval":
		// Mock calculation
		interval := schedule["interval"].(string)
		if strings.Contains(interval, "hours") {
			return now.Add(1 * time.Hour).Format(time.RFC3339)
		} else if strings.Contains(interval, "minutes") {
			return now.Add(30 * time.Minute).Format(time.RFC3339)
		} else if strings.Contains(interval, "days") {
			return now.Add(24 * time.Hour).Format(time.RFC3339)
		}
	case "once":
		// Return the scheduled time
		onceTime := schedule["once"].(string)
		return onceTime
	}

	return now.Add(24 * time.Hour).Format(time.RFC3339)
}

func (s *Server) Start(addr string) error {
	log.Printf("Starting API server on %s", addr)
	return s.app.Listen(addr)
}

func (s *Server) Shutdown() error {
	log.Println("Shutting down API server gracefully")
	appErr := s.app.Shutdown()
	storeErr := s.store.Close()
	if appErr != nil {
		return appErr
	}
	return storeErr
}
