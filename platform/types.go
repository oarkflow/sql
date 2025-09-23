package platform

import (
	"time"

	"github.com/oarkflow/sql/pkg/utils"
)

// IngestMessage represents the raw payload with metadata
type IngestMessage struct {
	ID          string         `json:"id"`
	RawID       string         `json:"raw_id"`  // Unique ID for raw blob
	Payload     []byte         `json:"payload"` // Raw bytes
	Source      string         `json:"source"`  // Source identifier
	Origin      string         `json:"origin"`  // Origin (URL, file path, etc.)
	Timestamp   time.Time      `json:"timestamp"`
	ContentType string         `json:"content_type"` // MIME type or format
	Metadata    map[string]any `json:"metadata"`     // Additional metadata
}

// Record is the parsed/structured unit (alias from utils)
type Record = utils.Record

// CanonicalModel represents the platform-standard schema
type CanonicalModel struct {
	SchemaName string                 `json:"schema_name"`
	Version    string                 `json:"version"`
	Fields     map[string]FieldSchema `json:"fields"`
}

// FieldSchema defines a field in the canonical model
type FieldSchema struct {
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
	Validation  string `json:"validation,omitempty"` // JSON schema validation rules
}

// Adapter interface for components that acquire data
type Adapter interface {
	Start() error
	Stop() error
	Name() string
}

// Parser interface for translating bytes into Records
type Parser interface {
	Parse(data []byte, contentType string) ([]Record, error)
	Name() string
}

// Normalizer interface for mapping Records to canonical model
type Normalizer interface {
	Normalize(record Record, schemaName string) (Record, error)
	Name() string
}

// Validator interface for schema and business validation
type Validator interface {
	Validate(record Record, schemaName string) error
	Name() string
}

// Sink interface for storing or forwarding normalized payload
type Sink interface {
	Store(record Record, schemaName string) error
	Forward(record Record, topic string) error
	Name() string
}

// Enricher interface for adding additional data to records
type Enricher interface {
	Enrich(record Record) (Record, error)
	Name() string
}

// MessageBus interface for decoupling ingestion from processing
type MessageBus interface {
	Publish(topic string, message any) error
	Subscribe(topic string, handler func(message any)) error
	Close() error
}

// WorkerPool interface for scalable processing
type WorkerPool interface {
	Start() error
	Stop() error
	Scale(workers int) error
	Process(job func()) error
}

// SchemaRegistry interface for managing schemas
type SchemaRegistry interface {
	Register(schema CanonicalModel) error
	Get(schemaName string) (CanonicalModel, error)
	Validate(record Record, schemaName string) error
	List() []string
}

// PipelineStage represents a stage in the processing pipeline
type PipelineStage struct {
	Name   string
	Type   string // parse, normalize, validate, enrich, store, forward
	Config map[string]any
}

// ProcessingPipeline represents the configurable chain
type ProcessingPipeline struct {
	Name   string
	Stages []PipelineStage
}

// IngestGateway represents HTTP endpoints / file upload / connectors
type IngestGateway struct {
	Name    string
	Type    string // http, file, ftp, etc.
	Config  map[string]any
	Adapter Adapter
}

// PlatformConfig holds the overall platform configuration
type PlatformConfig struct {
	Gateways         []IngestGateway      `json:"gateways"`
	Pipelines        []ProcessingPipeline `json:"pipelines"`
	Schemas          []CanonicalModel     `json:"schemas"`
	MessageBusConfig map[string]any       `json:"message_bus"`
	WorkerPoolConfig map[string]any       `json:"worker_pool"`
	StorageConfig    map[string]any       `json:"storage"`
	MonitoringConfig map[string]any       `json:"monitoring"`
}
