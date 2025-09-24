package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"github.com/oarkflow/sql/pkg/parsers"
)

// WebhookServer represents the webhook server
type WebhookServer struct {
	config  *Config
	jobs    chan Job
	parsers []parsers.Parser
	mu      sync.Mutex
	// Add more fields as needed
}

// Job represents a webhook processing job
type Job struct {
	Body    []byte
	Headers http.Header
}

// NewWebhookServer creates a new webhook server instance
func NewWebhookServer(config *Config) *WebhookServer {
	if config == nil {
		config = DefaultConfig()
	}
	return &WebhookServer{
		config: config,
		jobs:   make(chan Job, 100), // buffered channel for jobs
		parsers: []parsers.Parser{
			parsers.NewJSONParser(),
			parsers.NewXMLParser(),
			parsers.NewHL7Parser(),
			parsers.NewSMPPParser(),
			parsers.NewPlainTextParser(),
		},
	}
}

// Start starts the webhook server
func (ws *WebhookServer) Start() error {
	// Start worker goroutines
	for i := 0; i < ws.config.MaxWorkers; i++ {
		go ws.worker()
	}

	http.HandleFunc("/webhook", ws.handleWebhook)
	http.HandleFunc("/health", ws.healthHandler)
	log.Printf("Starting webhook server on port %s", ws.config.Port)
	return http.ListenAndServe(":"+ws.config.Port, nil)
}

// handleWebhook handles incoming webhook requests
func (ws *WebhookServer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Verify signature if secret is set
	if ws.config.Secret != "" {
		sig := r.Header.Get("X-Hub-Signature")
		if sig == "" {
			http.Error(w, "Missing signature", http.StatusUnauthorized)
			return
		}
		expected := "sha256=" + computeHmac(body, ws.config.Secret)
		if !hmac.Equal([]byte(sig), []byte(expected)) {
			http.Error(w, "Invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Respond immediately to make it non-blocking
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))

	// Send job to workers
	select {
	case ws.jobs <- Job{Body: body, Headers: r.Header.Clone()}:
		// Job queued
	default:
		log.Printf("Job queue full, dropping webhook")
	}
}

// worker processes jobs from the queue
func (ws *WebhookServer) worker() {
	for job := range ws.jobs {
		ws.processWebhook(job.Body, job.Headers)
	}
}

// processWebhook processes the webhook payload
func (ws *WebhookServer) processWebhook(body []byte, headers http.Header) {
	// Lock if needed for shared resources
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// Log the received data
	log.Printf("Processing webhook payload: %s", string(body))
	log.Printf("Headers: %v", headers)

	// Try to parse with available parsers
	for _, parser := range ws.parsers {
		if parser.Detect(body) {
			parsed, err := parser.Parse(body)
			if err == nil {
				log.Printf("Successfully parsed with %s: %v", parser.Name(), parsed)
				// Process the parsed data: validate, trigger actions, etc.
				// Add integration with ETL, integrations, etc.
				return
			}
		}
	}

	// If no parser succeeded, log as unknown
	log.Printf("Failed to parse with any known parser, treating as raw data")
}

// healthHandler handles health check requests
func (ws *WebhookServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// computeHmac computes the HMAC-SHA256 signature
func computeHmac(data []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
