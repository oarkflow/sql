package webhook

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/json"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
)

// Config holds the configuration for the webhook server
type Config struct {
	Port         string                  `json:"port" yaml:"port"`
	MaxWorkers   int                     `json:"max_workers" yaml:"max_workers"`
	Secret       string                  `json:"secret" yaml:"secret"` // For HMAC verification
	DataTypes    []string                `json:"data_types" yaml:"data_types"`
	ETLConfig    string                  `json:"etl_config" yaml:"etl_config"`     // Path to ETL config file
	Integrations string                  `json:"integrations" yaml:"integrations"` // Path to integrations config file
	ETLPipelines map[string]*ETLPipeline `json:"etl_pipelines,omitempty" yaml:"etl_pipelines,omitempty"`
	Parsers      []string                `json:"parsers" yaml:"parsers"` // List of parser names to enable
}

// ETLPipeline defines an ETL pipeline configuration for webhook processing
type ETLPipeline struct {
	Name        string              `json:"name" yaml:"name"`
	Description string              `json:"description" yaml:"description"`
	DataType    string              `json:"data_type" yaml:"data_type"` // hl7, json, xml, etc.
	Source      config.DataConfig   `json:"source" yaml:"source"`
	Destination config.DataConfig   `json:"destination" yaml:"destination"`
	Mapping     config.TableMapping `json:"mapping" yaml:"mapping"`
	Options     []etl.Option        `json:"options,omitempty" yaml:"options,omitempty"`
	Enabled     bool                `json:"enabled" yaml:"enabled"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Port:         "8080",
		MaxWorkers:   10,
		Secret:       "",
		DataTypes:    []string{"hl7", "json", "xml", "plain"},
		Parsers:      []string{"hl7", "json", "xml", "plain", "smpp"},
		ETLPipelines: make(map[string]*ETLPipeline),
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (*Config, error) {
	ext := filepath.Ext(path)
	switch ext {
	case ".yaml", ".yml":
		return LoadYaml(path)
	case ".json":
		return LoadJson(path)
	case ".bcl":
		return LoadBCL(path)
	}
	return nil, fmt.Errorf("unsupported config format: %s", ext)
}

// LoadYaml loads configuration from a YAML file
func LoadYaml(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	return &cfg, err
}

// LoadJson loads configuration from a JSON file
func LoadJson(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = json.Unmarshal(data, &cfg)
	return &cfg, err
}

// LoadBCL loads configuration from a BCL file
func LoadBCL(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	_, err = bcl.Unmarshal(data, &cfg)
	return &cfg, err
}
