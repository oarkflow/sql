package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/json"
	"gopkg.in/yaml.v3"
)

// BridgLinkConfig represents the top-level configuration used to describe
// how BridgLink should extract, transform, and load messages across connectors.
type BridgLinkConfig struct {
	Version    string                   `json:"version" yaml:"version"`
	Metadata   map[string]string        `json:"metadata" yaml:"metadata"`
	Defaults   PipelineDefaults         `json:"defaults" yaml:"defaults"`
	Connectors map[string]ConnectorSpec `json:"connectors" yaml:"connectors"`
	Pipelines  []PipelineSpec           `json:"pipelines" yaml:"pipelines"`
}

// PipelineDefaults define global fallbacks for pipelines so repeated values can be DRY.
type PipelineDefaults struct {
	Parser   string            `json:"parser" yaml:"parser"`
	Encoding string            `json:"encoding" yaml:"encoding"`
	Schedule *ScheduleSpec     `json:"schedule" yaml:"schedule"`
	Retry    *RetryPolicy      `json:"retry" yaml:"retry"`
	Dedup    *Deduplication    `json:"dedup" yaml:"dedup"`
	Metadata map[string]string `json:"metadata" yaml:"metadata"`
}

// ConnectorSpec describes a reusable source or destination endpoint.
type ConnectorSpec struct {
	Type        string            `json:"type" yaml:"type"`
	Protocol    string            `json:"protocol" yaml:"protocol"`
	Format      string            `json:"format" yaml:"format"`
	Parser      string            `json:"parser" yaml:"parser"`
	Transform   string            `json:"transform" yaml:"transform"`
	Config      map[string]any    `json:"config" yaml:"config"`
	Credentials map[string]any    `json:"credentials" yaml:"credentials"`
	Headers     map[string]string `json:"headers" yaml:"headers"`
	Options     map[string]any    `json:"options" yaml:"options"`
	Batch       *BatchSpec        `json:"batch" yaml:"batch"`
	Retry       *RetryPolicy      `json:"retry" yaml:"retry"`
	Enabled     *bool             `json:"enabled" yaml:"enabled"`
}

// BatchSpec defines batching behaviour for a connector.
type BatchSpec struct {
	Size       int `json:"size" yaml:"size"`
	IntervalMs int `json:"interval_ms" yaml:"interval_ms"`
	TimeoutMs  int `json:"timeout_ms" yaml:"timeout_ms"`
}

// PipelineSpec ties together connectors, parsers, and transformations in order.
type PipelineSpec struct {
	ID           string            `json:"id" yaml:"id"`
	Name         string            `json:"name" yaml:"name"`
	Description  string            `json:"description" yaml:"description"`
	Source       string            `json:"source" yaml:"source"`
	Destinations []string          `json:"destinations" yaml:"destinations"`
	Parsers      []ParserSpec      `json:"parsers" yaml:"parsers"`
	Transforms   []TransformSpec   `json:"transforms" yaml:"transforms"`
	Routes       []RouteSpec       `json:"routes" yaml:"routes"`
	Schedule     *ScheduleSpec     `json:"schedule" yaml:"schedule"`
	Retry        *RetryPolicy      `json:"retry" yaml:"retry"`
	Dedup        *Deduplication    `json:"dedup" yaml:"dedup"`
	Metadata     map[string]string `json:"metadata" yaml:"metadata"`
	Enabled      *bool             `json:"enabled" yaml:"enabled"`
}

// ParserSpec declares how a specific parser should run for a message.
type ParserSpec struct {
	Name    string         `json:"name" yaml:"name"`
	Format  string         `json:"format" yaml:"format"`
	Options map[string]any `json:"options" yaml:"options"`
}

// TransformSpec declares a reusable transformation that can be chained.
type TransformSpec struct {
	Name    string         `json:"name" yaml:"name"`
	Type    string         `json:"type" yaml:"type"`
	Input   string         `json:"input" yaml:"input"`
	Output  string         `json:"output" yaml:"output"`
	When    string         `json:"when" yaml:"when"`
	Options map[string]any `json:"options" yaml:"options"`
}

// RouteSpec allows advanced routing to multiple destinations.
type RouteSpec struct {
	Condition   string `json:"condition" yaml:"condition"`
	Destination string `json:"destination" yaml:"destination"`
}

// ScheduleSpec describes when a pipeline should run.
type ScheduleSpec struct {
	Cron      string `json:"cron" yaml:"cron"`
	Timezone  string `json:"timezone" yaml:"timezone"`
	StartAt   string `json:"start_at" yaml:"start_at"`
	EndAt     string `json:"end_at" yaml:"end_at"`
	Frequency string `json:"frequency" yaml:"frequency"`
}

// RetryPolicy controls retry behaviour for connectors/pipelines.
type RetryPolicy struct {
	Attempts     int `json:"attempts" yaml:"attempts"`
	BackoffMs    int `json:"backoff_ms" yaml:"backoff_ms"`
	MaxBackoffMs int `json:"max_backoff_ms" yaml:"max_backoff_ms"`
	JitterMs     int `json:"jitter_ms" yaml:"jitter_ms"`
}

// Deduplication mirrors existing dedup struct but keeps JSON/YAML tags for BridgLink.
// LoadBridgLinkConfig loads a config file based on its extension.
func LoadBridgLinkConfig(path string) (*BridgLinkConfig, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		return loadBridgLinkConfig(path, yaml.Unmarshal)
	case ".json":
		return loadBridgLinkConfig(path, func(data []byte, v any) error {
			return json.Unmarshal(data, v)
		})
	case ".bcl":
		return loadBridgLinkConfig(path, func(data []byte, v any) error {
			_, err := bcl.Unmarshal(data, v)
			return err
		})
	default:
		return nil, fmt.Errorf("unsupported BridgLink config format: %s", ext)
	}
}

// LoadBridgLinkConfigFromString loads the config from raw text, useful for tests.
func LoadBridgLinkConfigFromString(content, format string) (*BridgLinkConfig, error) {
	switch strings.ToLower(format) {
	case "yaml", "yml":
		return decodeBridgLinkConfig([]byte(content), yaml.Unmarshal)
	case "json":
		return decodeBridgLinkConfig([]byte(content), func(data []byte, v any) error {
			return json.Unmarshal(data, v)
		})
	case "bcl":
		return decodeBridgLinkConfig([]byte(content), func(data []byte, v any) error {
			_, err := bcl.Unmarshal(data, v)
			return err
		})
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

// Validate ensures that pipelines reference defined connectors and basic invariants hold.
func (cfg *BridgLinkConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if len(cfg.Pipelines) == 0 {
		return fmt.Errorf("at least one pipeline must be defined")
	}
	for idx, pipeline := range cfg.Pipelines {
		if pipeline.ID == "" {
			return fmt.Errorf("pipeline at index %d is missing an id", idx)
		}
		if pipeline.Source == "" {
			return fmt.Errorf("pipeline %s missing source", pipeline.ID)
		}
		if _, ok := cfg.Connectors[pipeline.Source]; !ok {
			return fmt.Errorf("pipeline %s references unknown source connector %s", pipeline.ID, pipeline.Source)
		}
		if len(pipeline.Destinations) == 0 {
			return fmt.Errorf("pipeline %s must declare at least one destination", pipeline.ID)
		}
		for _, dest := range pipeline.Destinations {
			if _, ok := cfg.Connectors[dest]; !ok {
				return fmt.Errorf("pipeline %s references unknown destination connector %s", pipeline.ID, dest)
			}
		}
		for _, route := range pipeline.Routes {
			if route.Destination != "" {
				if _, ok := cfg.Connectors[route.Destination]; !ok {
					return fmt.Errorf("pipeline %s route references unknown destination %s", pipeline.ID, route.Destination)
				}
			}
		}
	}
	return nil
}

func loadBridgLinkConfig(path string, fn func([]byte, any) error) (*BridgLinkConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeBridgLinkConfig(raw, fn)
}

func decodeBridgLinkConfig(data []byte, fn func([]byte, any) error) (*BridgLinkConfig, error) {
	var cfg BridgLinkConfig
	if err := fn(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Connectors == nil {
		cfg.Connectors = make(map[string]ConnectorSpec)
	}
	return &cfg, cfg.Validate()
}
