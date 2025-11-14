package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// Message represents a message flowing through the pipeline
type Message struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      interface{}            `json:"data"`
	Metadata  map[string]interface{} `json:"metadata"`
	Error     error                  `json:"error,omitempty"`
}

// Connector interface for source and destination connectors
type Connector interface {
	Initialize(config map[string]interface{}) error
	Start(ctx context.Context) error
	Stop() error
	GetType() string
}

// SourceConnector reads messages from a source
type SourceConnector interface {
	Connector
	Read(ctx context.Context) (<-chan Message, error)
}

// DestinationConnector writes messages to a destination
type DestinationConnector interface {
	Connector
	Write(ctx context.Context, msg Message) error
}

// Transformer transforms messages
type Transformer interface {
	Initialize(config map[string]interface{}) error
	Transform(ctx context.Context, msg Message) (Message, error)
	GetName() string
}

// Helper provides utility functions
type Helper interface {
	Initialize(config map[string]interface{}) error
	Execute(ctx context.Context, data interface{}) (interface{}, error)
	GetName() string
}

// Channel represents an ETL pipeline
type Channel struct {
	ID            string              `yaml:"id" json:"id"`
	Name          string              `yaml:"name" json:"name"`
	Description   string              `yaml:"description" json:"description"`
	Enabled       bool                `yaml:"enabled" json:"enabled"`
	Source        ConnectorConfig     `yaml:"source" json:"source"`
	Transformers  []TransformerConfig `yaml:"transformers" json:"transformers"`
	Destinations  []ConnectorConfig   `yaml:"destinations" json:"destinations"`
	ErrorHandling ErrorHandlingConfig `yaml:"error_handling" json:"error_handling"`
	Performance   PerformanceConfig   `yaml:"performance" json:"performance"`

	// Runtime components
	sourceConn   SourceConnector
	destConns    []DestinationConnector
	transformers []Transformer
	helpers      map[string]Helper
}

type ConnectorConfig struct {
	Type   string                 `yaml:"type" json:"type"`
	Config map[string]interface{} `yaml:"config" json:"config"`
}

type TransformerConfig struct {
	Type   string                 `yaml:"type" json:"type"`
	Config map[string]interface{} `yaml:"config" json:"config"`
}

type ErrorHandlingConfig struct {
	OnError       string `yaml:"on_error" json:"on_error"` // continue, stop, retry
	RetryAttempts int    `yaml:"retry_attempts" json:"retry_attempts"`
	RetryDelay    string `yaml:"retry_delay" json:"retry_delay"`
}

type PerformanceConfig struct {
	Workers    int `yaml:"workers" json:"workers"`
	BatchSize  int `yaml:"batch_size" json:"batch_size"`
	BufferSize int `yaml:"buffer_size" json:"buffer_size"`
}

// ConnectorRegistry manages connector types
type ConnectorRegistry struct {
	sources      map[string]func() SourceConnector
	destinations map[string]func() DestinationConnector
	transformers map[string]func() Transformer
	helpers      map[string]func() Helper
	mu           sync.RWMutex
}

func NewConnectorRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{
		sources:      make(map[string]func() SourceConnector),
		destinations: make(map[string]func() DestinationConnector),
		transformers: make(map[string]func() Transformer),
		helpers:      make(map[string]func() Helper),
	}
}

func (r *ConnectorRegistry) RegisterSource(name string, factory func() SourceConnector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources[name] = factory
}

func (r *ConnectorRegistry) RegisterDestination(name string, factory func() DestinationConnector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.destinations[name] = factory
}

func (r *ConnectorRegistry) RegisterTransformer(name string, factory func() Transformer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transformers[name] = factory
}

func (r *ConnectorRegistry) RegisterHelper(name string, factory func() Helper) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.helpers[name] = factory
}

func (r *ConnectorRegistry) CreateSource(name string) (SourceConnector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.sources[name]
	if !ok {
		return nil, fmt.Errorf("source connector %s not found", name)
	}
	return factory(), nil
}

func (r *ConnectorRegistry) CreateDestination(name string) (DestinationConnector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.destinations[name]
	if !ok {
		return nil, fmt.Errorf("destination connector %s not found", name)
	}
	return factory(), nil
}

func (r *ConnectorRegistry) CreateTransformer(name string) (Transformer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.transformers[name]
	if !ok {
		return nil, fmt.Errorf("transformer %s not found", name)
	}
	return factory(), nil
}

func (r *ConnectorRegistry) CreateHelper(name string) (Helper, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.helpers[name]
	if !ok {
		return nil, fmt.Errorf("helper %s not found", name)
	}
	return factory(), nil
}

// Engine manages channels
type Engine struct {
	registry *ConnectorRegistry
	channels map[string]*Channel
	mu       sync.RWMutex
}

func NewEngine(registry *ConnectorRegistry) *Engine {
	return &Engine{
		registry: registry,
		channels: make(map[string]*Channel),
	}
}

func (e *Engine) LoadChannelFromFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var channel Channel

	// Try YAML first
	if err := yaml.Unmarshal(data, &channel); err != nil {
		// Try JSON
		if err := json.Unmarshal(data, &channel); err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}
	}

	return e.AddChannel(&channel)
}

func (e *Engine) AddChannel(ch *Channel) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Initialize source connector
	source, err := e.registry.CreateSource(ch.Source.Type)
	if err != nil {
		return fmt.Errorf("failed to create source: %w", err)
	}
	if err := source.Initialize(ch.Source.Config); err != nil {
		return fmt.Errorf("failed to initialize source: %w", err)
	}
	ch.sourceConn = source

	// Initialize transformers
	for _, tConfig := range ch.Transformers {
		t, err := e.registry.CreateTransformer(tConfig.Type)
		if err != nil {
			return fmt.Errorf("failed to create transformer %s: %w", tConfig.Type, err)
		}
		if err := t.Initialize(tConfig.Config); err != nil {
			return fmt.Errorf("failed to initialize transformer: %w", err)
		}
		ch.transformers = append(ch.transformers, t)
	}

	// Initialize destination connectors
	for _, dConfig := range ch.Destinations {
		dest, err := e.registry.CreateDestination(dConfig.Type)
		if err != nil {
			return fmt.Errorf("failed to create destination: %w", err)
		}
		if err := dest.Initialize(dConfig.Config); err != nil {
			return fmt.Errorf("failed to initialize destination: %w", err)
		}
		ch.destConns = append(ch.destConns, dest)
	}

	e.channels[ch.ID] = ch
	return nil
}

func (e *Engine) StartChannel(ctx context.Context, channelID string) error {
	e.mu.RLock()
	ch, ok := e.channels[channelID]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("channel %s not found", channelID)
	}

	if !ch.Enabled {
		return fmt.Errorf("channel %s is disabled", channelID)
	}

	log.Printf("Starting channel: %s (%s)", ch.Name, ch.ID)

	// Start source connector
	if err := ch.sourceConn.Start(ctx); err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	// Start destination connectors
	for _, dest := range ch.destConns {
		if err := dest.Start(ctx); err != nil {
			return fmt.Errorf("failed to start destination: %w", err)
		}
	}

	// Read messages from source
	msgChan, err := ch.sourceConn.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to read from source: %w", err)
	}

	// Process messages
	workers := ch.Performance.Workers
	if workers == 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			e.processMessages(ctx, ch, msgChan, workerID)
		}(i)
	}

	// Wait for context cancellation
	go func() {
		<-ctx.Done()
		log.Printf("Stopping channel: %s", ch.Name)
		ch.sourceConn.Stop()
		for _, dest := range ch.destConns {
			dest.Stop()
		}
		wg.Wait()
		log.Printf("Channel stopped: %s", ch.Name)
	}()

	return nil
}

func (e *Engine) processMessages(ctx context.Context, ch *Channel, msgChan <-chan Message, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgChan:
			if !ok {
				return
			}

			// Transform message
			transformedMsg := msg
			for _, transformer := range ch.transformers {
				var err error
				transformedMsg, err = transformer.Transform(ctx, transformedMsg)
				if err != nil {
					log.Printf("Worker %d: Transform error: %v", workerID, err)
					if ch.ErrorHandling.OnError == "stop" {
						return
					}
					continue
				}
			}

			// Send to destinations
			for _, dest := range ch.destConns {
				if err := dest.Write(ctx, transformedMsg); err != nil {
					log.Printf("Worker %d: Destination write error: %v", workerID, err)
					if ch.ErrorHandling.OnError == "stop" {
						return
					}
				}
			}
		}
	}
}

func (e *Engine) StartAll(ctx context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for id := range e.channels {
		if err := e.StartChannel(ctx, id); err != nil {
			log.Printf("Failed to start channel %s: %v", id, err)
		}
	}

	return nil
}

func main() {
	// Create registry and register built-in connectors
	registry := NewConnectorRegistry()

	// Register HL7 connectors
	registry.RegisterSource("hl7_mllp", func() SourceConnector { return NewHL7MLLPSource() })
	registry.RegisterTransformer("hl7_to_json", func() Transformer { return NewHL7ToJSONTransformer() })
	registry.RegisterDestination("json_file", func() DestinationConnector { return NewJSONFileDestination() })
	registry.RegisterDestination("console", func() DestinationConnector { return NewConsoleDestination() })

	// Create engine
	engine := NewEngine(registry)

	// Load channels from config files
	configFiles := []string{"channel_hl7.yaml"}
	for _, file := range configFiles {
		if err := engine.LoadChannelFromFile(file); err != nil {
			log.Printf("Warning: Failed to load %s: %v", file, err)
		}
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start all channels
	if err := engine.StartAll(ctx); err != nil {
		log.Fatalf("Failed to start engine: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("BridgeLink engine started. Press Ctrl+C to stop.")
	<-sigChan
	log.Println("Shutting down...")
	cancel()

	// Give channels time to cleanup
	time.Sleep(2 * time.Second)
	log.Println("Shutdown complete")
}
