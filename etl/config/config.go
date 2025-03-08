package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type DataConfig struct {
	Type          string `yaml:"type"`
	Host          string `yaml:"host,omitempty"`
	Port          int    `yaml:"port,omitempty"`
	Driver        string `yaml:"driver,omitempty"`
	Username      string `yaml:"username,omitempty"`
	Password      string `yaml:"password,omitempty"`
	Database      string `yaml:"database,omitempty"`
	File          string `yaml:"file,omitempty"`
	DisableLogger bool   `yaml:"disablelogger,omitempty"`
}

type TableMapping struct {
	OldName             string            `yaml:"old_name"`
	NewName             string            `yaml:"new_name"`
	Migrate             bool              `yaml:"migrate"`
	CloneSource         bool              `yaml:"clone_source"`
	BatchSize           int               `yaml:"batch_size"`
	SkipStoreError      bool              `yaml:"skip_store_error"`
	UpdateSequence      bool              `yaml:"update_sequence"`
	TruncateDestination bool              `yaml:"truncate_destination"`
	Mapping             map[string]string `yaml:"mapping"`
	Query               string            `yaml:"query,omitempty"`
	KeyValueTable       bool              `yaml:"key_value_table,omitempty"`
	KeyField            string            `yaml:"key_field,omitempty"`
	ValueField          string            `yaml:"value_field,omitempty"`
	ExtraValues         map[string]any    `yaml:"extra_values,omitempty"`
	IncludeFields       []string          `yaml:"include_fields,omitempty"`
	ExcludeFields       []string          `yaml:"exclude_fields,omitempty"`
	AutoCreateTable     bool              `yaml:"auto_create_table,omitempty"`
	Update              bool              `json:"update" yaml:"update"`
	Delete              bool              `json:"delete" yaml:"delete"`
}

type Config struct {
	Source      DataConfig     `yaml:"source"`
	Destination DataConfig     `yaml:"destination"`
	Tables      []TableMapping `yaml:"tables"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
