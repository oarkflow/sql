package bridglink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
)

// Manager orchestrates BridgLink pipelines using the existing ETL manager.
type Manager struct {
	cfg           *config.BridgLinkConfig
	etlManager    *etl.Manager
	connectorData map[string]config.DataConfig
}

// NewManager constructs a BridgLink manager from configuration and an optional ETL manager.
func NewManager(cfg *config.BridgLinkConfig, etlManager *etl.Manager) (*Manager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("bridglink: config cannot be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if etlManager == nil {
		etlManager = etl.NewManager()
	}
	manager := &Manager{
		cfg:           cfg,
		etlManager:    etlManager,
		connectorData: make(map[string]config.DataConfig),
	}
	if err := manager.buildConnectorCache(); err != nil {
		return nil, err
	}
	return manager, nil
}

// NewManagerFromFile loads config from disk and constructs a manager instance.
func NewManagerFromFile(path string, etlManager *etl.Manager) (*Manager, error) {
	cfg, err := config.LoadBridgLinkConfig(path)
	if err != nil {
		return nil, err
	}
	return NewManager(cfg, etlManager)
}

// RunPipeline prepares and starts a pipeline by ID and returns created ETL job IDs.
func (m *Manager) RunPipeline(ctx context.Context, pipelineID string) ([]string, error) {
	cfg, err := m.BuildETLConfig(pipelineID)
	if err != nil {
		return nil, err
	}
	jobIDs, err := m.etlManager.Prepare(cfg)
	if err != nil {
		return nil, err
	}
	for _, id := range jobIDs {
		if err := m.etlManager.Start(ctx, id); err != nil {
			return nil, err
		}
	}
	return jobIDs, nil
}

// BuildETLConfig constructs a legacy ETL config for a given BridgLink pipeline.
func (m *Manager) BuildETLConfig(pipelineID string) (*config.Config, error) {
	pipeline, err := m.getPipeline(pipelineID)
	if err != nil {
		return nil, err
	}
	if pipeline.Enabled != nil && !*pipeline.Enabled {
		return nil, fmt.Errorf("pipeline %s is disabled", pipelineID)
	}

	sourceCfg, err := m.getConnector(pipeline.Source)
	if err != nil {
		return nil, err
	}

	destCfgs := make([]config.DataConfig, 0, len(pipeline.Destinations))
	for _, destKey := range pipeline.Destinations {
		dest, err := m.getConnector(destKey)
		if err != nil {
			return nil, err
		}
		destCfgs = append(destCfgs, dest)
	}

	tableTransformers := append([]config.TransformerConfig{}, buildTransformerConfigs(pipeline.Parsers)...)
	tableTransformers = append(tableTransformers, buildTransformConfigs(pipeline.Transforms)...)

	tables := make([]config.TableMapping, 0, len(destCfgs))
	for idx, dest := range destCfgs {
		table := config.TableMapping{
			OldName:        pipeline.Name,
			NewName:        fmt.Sprintf("%s_%d", pipeline.Name, idx+1),
			EnableBatch:    true,
			BatchSize:      1,
			DestinationKey: dest.Key,
			Transformers:   cloneTransformers(tableTransformers),
		}
		if dest.File != "" {
			table.NewName = dest.File
		}
		tables = append(tables, table)
	}

	etlCfg := &config.Config{
		Source:       sourceCfg,
		Sources:      []config.DataConfig{sourceCfg},
		Destinations: destCfgs,
		Tables:       tables,
		WorkerCount:  1,
		Buffer:       10,
	}

	if pipeline.Dedup != nil {
		etlCfg.Deduplication.Enabled = pipeline.Dedup.Enabled
		etlCfg.Deduplication.Field = pipeline.Dedup.Field
	} else {
		etlCfg.Deduplication = config.Deduplication{Enabled: false}
	}

	if m.cfg.Defaults.Dedup != nil && !etlCfg.Deduplication.Enabled {
		etlCfg.Deduplication = *m.cfg.Defaults.Dedup
	}

	return etlCfg, nil
}

func (m *Manager) buildConnectorCache() error {
	for key, spec := range m.cfg.Connectors {
		dc := config.DataConfig{
			Key:      key,
			Type:     strings.ToLower(spec.Protocol),
			Format:   spec.Format,
			Host:     getString(spec.Config, "host"),
			Username: getString(spec.Config, "username"),
			Password: getString(spec.Config, "password"),
			Database: getString(spec.Config, "database"),
			Table:    getString(spec.Config, "table"),
			File:     getString(spec.Config, "file"),
			Source:   getString(spec.Config, "source"),
			DataPath: getString(spec.Config, "path"),
			Settings: mergeMaps(spec.Config, spec.Options),
		}
		if dc.File == "" {
			if alt := getString(spec.Config, "path"); alt != "" {
				dc.File = alt
			} else if alt := getString(spec.Config, "output"); alt != "" {
				dc.File = alt
			}
		}
		if dc.Source == "" {
			dc.Source = getString(spec.Config, "url")
		}
		if dc.Source == "" {
			dc.Source = getString(spec.Config, "endpoint")
		}
		if dc.Source == "" && dc.File != "" {
			dc.Source = dc.File
		}
		if dc.Source == "" {
			dc.Source = spec.Format
		}
		m.connectorData[key] = dc
	}
	return nil
}

func (m *Manager) getConnector(key string) (config.DataConfig, error) {
	dc, ok := m.connectorData[key]
	if !ok {
		return config.DataConfig{}, fmt.Errorf("connector %s not defined", key)
	}
	dc.Key = key
	return dc, nil
}

func (m *Manager) getPipeline(id string) (*config.PipelineSpec, error) {
	for _, pipeline := range m.cfg.Pipelines {
		if pipeline.ID == id {
			return &pipeline, nil
		}
	}
	return nil, fmt.Errorf("pipeline %s not found", id)
}

func buildTransformerConfigs(parsers []config.ParserSpec) []config.TransformerConfig {
	transformers := make([]config.TransformerConfig, 0, len(parsers))
	for _, parserSpec := range parsers {
		transformers = append(transformers, config.TransformerConfig{
			Name:    parserSpec.Name,
			Type:    strings.ToLower(parserSpec.Name),
			Options: cloneMap(parserSpec.Options),
		})
	}
	return transformers
}

func buildTransformConfigs(specs []config.TransformSpec) []config.TransformerConfig {
	transformers := make([]config.TransformerConfig, 0, len(specs))
	for _, spec := range specs {
		transformers = append(transformers, config.TransformerConfig{
			Name:    spec.Name,
			Type:    strings.ToLower(spec.Type),
			Options: cloneMap(spec.Options),
		})
	}
	return transformers
}

func cloneTransformers(in []config.TransformerConfig) []config.TransformerConfig {
	clones := make([]config.TransformerConfig, len(in))
	for i, cfg := range in {
		clones[i] = config.TransformerConfig{
			Name:    cfg.Name,
			Type:    cfg.Type,
			Options: cloneMap(cfg.Options),
		}
	}
	return clones
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeMaps(primary, secondary map[string]any) map[string]any {
	if primary == nil && secondary == nil {
		return nil
	}
	merged := make(map[string]any)
	for k, v := range primary {
		merged[k] = v
	}
	for k, v := range secondary {
		merged[k] = v
	}
	return merged
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case string:
			return v
		case fmt.Stringer:
			return v.String()
		}
	}
	return ""
}

// WithSchedule applies schedule defaults to ETL configs via metadata (placeholder for cron integrations).
func WithSchedule(spec *config.ScheduleSpec) time.Duration {
	// Placeholder for future advanced scheduling support.
	if spec == nil {
		return 0
	}
	if spec.Frequency != "" {
		if dur, err := time.ParseDuration(spec.Frequency); err == nil {
			return dur
		}
	}
	return 0
}
