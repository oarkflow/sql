package etl

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/adapters/fileadapter"
	"github.com/oarkflow/sql/pkg/adapters/ioadapter"
	"github.com/oarkflow/sql/pkg/adapters/serviceadapter"
	"github.com/oarkflow/sql/pkg/adapters/sqladapter"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/transformers"
	utils2 "github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/connection"
)

type Option func(*ETL) error

func WithNormalizeSchema(schema map[string]string) Option {
	return func(e *ETL) error {
		e.normalizeSchema = schema
		return nil
	}
}

func WithPipelineConfig(pc *PipelineConfig) Option {
	return func(e *ETL) error {
		e.pipelineConfig = pc
		return nil
	}
}

func NewSource(sourceType string, sourceDB *squealx.DB, sourceFile, sourceTable, sourceQuery, format string) (contracts.Source, error) {
	var src contracts.Source
	if utils2.IsSQLType(sourceType) {
		if sourceDB == nil {
			return nil, fmt.Errorf("source database is nil")
		}
		src = sqladapter.NewSource(sourceDB, sourceTable, sourceQuery)
	} else if sourceType == "csv" || sourceType == "json" {
		fileSrc := sourceTable
		if fileSrc == "" {
			fileSrc = sourceFile
		}
		src = fileadapter.New(fileSrc, "source", false)
	} else if sourceType == "stdin" {
		return ioadapter.NewSource(os.Stdin, format), nil
	} else if sourceType == "service" {
		// Service type requires integration manager - this will be handled by WithServiceSource
		return nil, fmt.Errorf("service type must be used with WithServiceSource option")
	} else {
		return nil, fmt.Errorf("unsupported source type: %s", sourceType)
	}
	return src, nil
}

func WithSources(sources ...contracts.Source) Option {
	return func(e *ETL) error {
		e.sources = append(e.sources, sources...)
		return nil
	}
}

func WithSource(sourceType string, sourceDB *squealx.DB, sourceFile, sourceTable, sourceQuery, format string) Option {
	return func(e *ETL) error {
		src, err := NewSource(sourceType, sourceDB, sourceFile, sourceTable, sourceQuery, format)
		if err != nil {
			return err
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

// selectDestination returns a destination based on TableMapping.DestinationKey.
// If DestinationKey is set, it looks for a config with that key (matching DataConfig.Key).
// Otherwise, if there is only one destination, it returns that; else returns the first.
func selectDestination(dests []config.DataConfig, tm config.TableMapping) (config.DataConfig, error) {
	if len(dests) == 0 {
		return config.DataConfig{}, fmt.Errorf("no destinations provided")
	}
	if tm.DestinationKey != "" {
		for _, d := range dests {
			if d.Key == tm.DestinationKey {
				return d, nil
			}
		}
		return config.DataConfig{}, fmt.Errorf("destination with key %s not found", tm.DestinationKey)
	}
	// No key provided: if only one, or default to the first.
	return dests[0], nil
}

func WithDestination(dest config.DataConfig, destDB *squealx.DB, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var destination contracts.Loader
		if utils2.IsSQLType(dest.Type) {
			if destDB == nil {
				dbConfig := dest.ToSquealxConfig()
				db, _, err := connection.FromConfig(dbConfig)
				if err != nil {
					return fmt.Errorf("failed to connect to destination database: %w", err)
				}
				destDB = db
			}
			destination = sqladapter.NewLoader(destDB, dest.Type, dest.Driver, cfg, cfg.NormalizeSchema)
		} else if dest.Type == "csv" || dest.Type == "json" {
			fileLoader := cfg.NewName
			if fileLoader == "" {
				fileLoader = dest.File
			}
			appendMode := true
			if cfg.TruncateDestination {
				appendMode = false
			}
			destination = fileadapter.New(fileLoader, "loader", appendMode)
		} else if dest.Type == "stdout" {
			destination = ioadapter.NewLoader(os.Stdout, dest.Format)
		} else {
			return fmt.Errorf("unsupported destination type: %s", dest.Type)
		}
		e.loaders = append(e.loaders, destination)
		return nil
	}
}

func WithMappers(mapperList ...contracts.Mapper) Option {
	return func(e *ETL) error {
		e.mappers = append(e.mappers, mapperList...)
		return nil
	}
}

func WithTransformers(list ...contracts.Transformer) Option {
	return func(e *ETL) error {
		e.transformers = append(e.transformers, list...)
		return nil
	}
}

func WithKeyValueTransformer(extraValues map[string]any, includeFields, excludeFields []string, keyField, valueField string) Option {
	return func(e *ETL) error {
		e.transformers = append(e.transformers, transformers.NewKeyValue(keyField, valueField, includeFields, excludeFields, extraValues))
		return nil
	}
}

func WithWorkerCount(count int) Option {
	return func(e *ETL) error {
		e.workerCount = count
		return nil
	}
}

func WithBatchSize(size int) Option {
	return func(e *ETL) error {
		e.batchSize = size
		return nil
	}
}

func WithRawChanBuffer(buffer int) Option {
	return func(e *ETL) error {
		e.rawChanBuffer = buffer
		return nil
	}
}

func WithCheckpoint(store contracts.CheckpointStore, cpFunc func(rec utils2.Record) string) Option {
	return func(e *ETL) error {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
		return nil
	}
}

func WithLifecycleHooks(hooks *LifecycleHooks) Option {
	return func(e *ETL) error {
		e.hooks = hooks
		return nil
	}
}

func WithValidations(val *Validations) Option {
	return func(e *ETL) error {
		e.validations = val
		return nil
	}
}

func WithEventBus(eb *EventBus) Option {
	return func(e *ETL) error {
		e.eventBus = eb
		return nil
	}
}

func WithDeduplication(dedupField string) Option {
	return func(e *ETL) error {
		e.dedupEnabled = true
		e.dedupField = dedupField
		return nil
	}
}

func WithPlugins(plugins ...Plugin) Option {
	return func(e *ETL) error {
		for _, p := range plugins {
			if err := p.Init(e); err != nil {
				return err
			}
			e.plugins = append(e.plugins, p)
		}

		return nil
	}
}

func WithDashboardAuth(user, pass string) Option {
	return func(e *ETL) error {
		e.dashboardUser = user
		e.dashboardPass = pass
		return nil
	}
}

// WithLogger Option to inject a custom logger.
func WithLogger(logger *log.Logger) Option {
	return func(e *ETL) error {
		if logger == nil {
			return fmt.Errorf("WithLogger: provided logger is nil")
		}
		e.Logger = logger
		return nil
	}
}

// WithMaxErrorThreshold Option to set a maximum error threshold.
func WithMaxErrorThreshold(threshold int) Option {
	return func(e *ETL) error {
		if threshold <= 0 {
			return fmt.Errorf("WithMaxErrorThreshold: threshold must be positive")
		}
		e.maxErrorCount = threshold
		return nil
	}
}

// SourceConfig type to represent source configuration.
type SourceConfig struct {
	Type   string      // e.g., "mongodb", "rest", "kafka", etc.
	DB     *squealx.DB // used for SQL sources
	File   string      // file path or endpoint URL
	Table  string      // table name for file or DB source
	Query  string      // query for SQL sources
	Format string      // data format (e.g. "json", "csv", etc.)
}

// WithMultipleSources creates and adds multiple sources from a slice of SourceConfig.
func WithMultipleSources(sourceConfs []SourceConfig) Option {
	return func(e *ETL) error {
		for _, conf := range sourceConfs {
			src, err := NewSource(conf.Type, conf.DB, conf.File, conf.Table, conf.Query, conf.Format)
			if err != nil {
				return fmt.Errorf("failed to create source of type %s: %w", conf.Type, err)
			}
			e.sources = append(e.sources, src)
		}
		return nil
	}
}

// Enhanced ETL configuration options

// WithStateManager sets a custom state manager
func WithStateManager(sm *StateManager) Option {
	return func(e *ETL) error {
		e.stateManager = sm
		return nil
	}
}

// WithDeadLetterQueue sets a custom dead letter queue
func WithDeadLetterQueue(dlq *DeadLetterQueue) Option {
	return func(e *ETL) error {
		e.deadLetterQueue = dlq
		return nil
	}
}

// WithIdempotencyManager sets a custom idempotency manager
func WithIdempotencyManager(im *IdempotencyManager) Option {
	return func(e *ETL) error {
		e.idempotencyMgr = im
		return nil
	}
}

// WithStateFile sets the state file path
func WithStateFile(file string) Option {
	return func(e *ETL) error {
		e.stateFile = file
		return nil
	}
}

// WithDLQFile sets the dead letter queue file path
func WithDLQFile(file string) Option {
	return func(e *ETL) error {
		e.dlqFile = file
		return nil
	}
}

// WithIdempotencyFile sets the idempotency file path
func WithIdempotencyFile(file string) Option {
	return func(e *ETL) error {
		e.idempotencyFile = file
		return nil
	}
}

// WithIdempotencyFields sets the fields to use for idempotency keys
func WithIdempotencyFields(fields []string) Option {
	return func(e *ETL) error {
		if e.idempotencyMgr != nil {
			// Store fields in metadata for later use
			e.stateManager.SetMetadata("idempotency_fields", fields)
		}
		return nil
	}
}

// WithDLQConfig configures the dead letter queue parameters
func WithDLQConfig(maxSize, maxRetries int, baseDelay, maxDelay time.Duration) Option {
	return func(e *ETL) error {
		if e.deadLetterQueue != nil {
			// Update existing DLQ configuration
			// Note: This would require additional methods in DeadLetterQueue
		}
		return nil
	}
}

// WithAutoSaveInterval sets the auto-save interval for state
func WithAutoSaveInterval(interval time.Duration) Option {
	return func(e *ETL) error {
		if e.stateManager != nil {
			// This would require a method to update the auto-save interval
		}
		return nil
	}
}

// WithServiceSource creates and adds a service source using integrations
func WithServiceSource(integrationManager *integrations.Manager, serviceName, query, table, key string, credentials map[string]interface{}) Option {
	return func(e *ETL) error {
		config := serviceadapter.Config{
			ServiceName: serviceName,
			ServiceType: "database", // Currently only database services are supported
			Query:       query,
			Table:       table,
			Key:         key,
			Credentials: credentials,
		}

		src := serviceadapter.New(integrationManager, config)
		e.sources = append(e.sources, src)
		return nil
	}
}
