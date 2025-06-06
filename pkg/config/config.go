package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/oarkflow/bcl"
	"github.com/oarkflow/json"
	"github.com/oarkflow/squealx"

	"gopkg.in/yaml.v3"
)

type DataConfig struct {
	Key           string `yaml:"key" json:"key"`
	Type          string `yaml:"type" json:"type"`
	Host          string `yaml:"host,omitempty" json:"host,omitempty"`
	Port          int    `yaml:"port,omitempty" json:"port,omitempty"`
	Driver        string `yaml:"driver,omitempty" json:"driver,omitempty"`
	Username      string `yaml:"username,omitempty" json:"username,omitempty"`
	Password      string `yaml:"password,omitempty" json:"password,omitempty"`
	Database      string `yaml:"database,omitempty" json:"database,omitempty"`
	File          string `yaml:"file,omitempty" json:"file,omitempty"`
	DisableLogger bool   `yaml:"disablelogger,omitempty" json:"disablelogger,omitempty"`
	Table         string `yaml:"table" json:"table"`
	Source        string `yaml:"source" json:"source"`
	Format        string `yaml:"format" json:"format"`
}

type TableMapping struct {
	OldName             string            `yaml:"old_name" json:"old_name"`
	NewName             string            `yaml:"new_name" json:"new_name"`
	Migrate             bool              `yaml:"migrate" json:"migrate"`
	CloneSource         bool              `yaml:"clone_source" json:"clone_source"`
	BatchSize           int               `yaml:"batch_size" json:"batch_size"`
	SkipStoreError      bool              `yaml:"skip_store_error" json:"skip_store_error"`
	UpdateSequence      bool              `yaml:"update_sequence" json:"update_sequence"`
	TruncateDestination bool              `yaml:"truncate_destination" json:"truncate_destination"`
	Mapping             map[string]string `yaml:"mapping" json:"mapping"`
	Query               string            `yaml:"query,omitempty" json:"query,omitempty"`
	KeyValueTable       bool              `yaml:"key_value_table,omitempty" json:"key_value_table,omitempty"`
	KeyField            string            `yaml:"key_field,omitempty" json:"key_field,omitempty"`
	ValueField          string            `yaml:"value_field,omitempty" json:"value_field,omitempty"`
	ExtraValues         map[string]any    `yaml:"extra_values,omitempty" json:"extra_values,omitempty"`
	IncludeFields       []string          `yaml:"include_fields,omitempty" json:"include_fields,omitempty"`
	ExcludeFields       []string          `yaml:"exclude_fields,omitempty" json:"exclude_fields,omitempty"`
	AutoCreateTable     bool              `yaml:"auto_create_table,omitempty" json:"auto_create_table,omitempty"`
	Update              bool              `yaml:"update" json:"update"`
	Delete              bool              `yaml:"delete" json:"delete"`
	Aggregator          *AggregatorConfig `yaml:"aggregator" json:"aggregator"`
	NormalizeSchema     map[string]string `yaml:"normalize_schema" json:"normalize_schema"`
	DestinationKey      string            `yaml:"destination_key,omitempty" json:"destination_key,omitempty"`
	EnableBatch         bool              `yaml:"enable_batch" json:"enable_batch"`
}

type Checkpoint struct {
	File  string `json:"file" yaml:"file"`
	Field string `json:"field" yaml:"field"`
}

type Deduplication struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Field   string `json:"field" yaml:"field"`
}

type Config struct {
	Source          DataConfig     `yaml:"source" json:"source"`
	Sources         []DataConfig   `yaml:"sources" json:"sources"`
	Destinations    []DataConfig   `yaml:"destinations" json:"destinations"`
	Lookups         []DataConfig   `yaml:"lookups" json:"lookups"`
	Tables          []TableMapping `yaml:"tables" json:"tables"`
	WorkerCount     int            `json:"worker_count" yaml:"worker_count"`
	Buffer          int            `json:"buffer" yaml:"buffer"`
	DistributedMode bool           `json:"distributed_mode" yaml:"distributed_mode"`
	StreamingMode   bool           `json:"streaming_mode" yaml:"streaming_mode"`
	Checkpoint      Checkpoint     `json:"checkpoints" yaml:"checkpoints"`
	Deduplication   Deduplication  `json:"deduplication" yaml:"deduplication"`
}

type AggregationDefinition struct {
	SourceField string `json:"source_field" yaml:"source_field"`
	Func        string `json:"func" yaml:"func"`
	OutputField string `json:"output_field" yaml:"output_field"`
}

type AggregatorConfig struct {
	GroupBy      []string                `yaml:"group_by"`
	Aggregations []AggregationDefinition `yaml:"aggregations"`
}

func Load(path string) (*Config, error) {
	ext := filepath.Ext(path)
	switch ext {
	case ".yaml":
		return LoadYaml(path)
	case ".json":
		return LoadJson(path)
	case ".bcl":
		return LoadBCL(path)
	}
	return nil, errors.New("unsupported file format")
}

type UnmarshalFunc func(data []byte, v any) error

func loadConfig(path string, unmarshal UnmarshalFunc) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func LoadYaml(path string) (*Config, error) {
	return loadConfig(path, yaml.Unmarshal)
}

func LoadJson(path string) (*Config, error) {
	return loadConfig(path, func(data []byte, v any) error {
		return json.Unmarshal(data, v)
	})
}

func LoadBCL(path string) (*Config, error) {
	return loadConfig(path, func(data []byte, v any) error {
		_, err := bcl.Unmarshal(data, v)
		return err
	})
}

func OpenDB(cfg DataConfig) (*squealx.DB, error) {
	var dsn string
	if cfg.Driver == "mysql" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	} else if cfg.Driver == "postgres" {
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)
	} else {
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Driver)
	}
	db, err := squealx.Open(cfg.Driver, dsn, cfg.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	return db, nil
}
