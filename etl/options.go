package etl

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/oarkflow/sql/pkg/adapters"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/transformers"
	utils2 "github.com/oarkflow/sql/pkg/utils"
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

func NewSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery, format string) (contracts.Source, error) {
	var src contracts.Source
	if utils2.IsSQLType(sourceType) {
		if sourceDB == nil {
			return nil, fmt.Errorf("source database is nil")
		}
		src = adapters.NewSQLAdapterAsSource(sourceDB, sourceTable, sourceQuery)
	} else if sourceType == "csv" || sourceType == "json" {
		file := sourceTable
		if file == "" {
			file = sourceFile
		}
		src = adapters.NewFileAdapter(file, "source", false)
	} else if sourceType == "stdin" {
		return adapters.NewIOAdapterSource(os.Stdin, format), nil
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

func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery, format string) Option {
	return func(e *ETL) error {
		src, err := NewSource(sourceType, sourceDB, sourceFile, sourceTable, sourceQuery, format)
		if err != nil {
			return err
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

func WithDestination(dest config.DataConfig, destDB *sql.DB, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var destination contracts.Loader
		if utils2.IsSQLType(dest.Type) {
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			destination = adapters.NewSQLAdapterAsLoader(destDB, dest.Type, dest.Driver, cfg, cfg.NormalizeSchema)
		} else if dest.Type == "csv" || dest.Type == "json" {
			file := cfg.NewName
			if file == "" {
				file = dest.File
			}
			appendMode := true
			if cfg.TruncateDestination {
				appendMode = false
			}
			destination = adapters.NewFileAdapter(file, "loader", appendMode)
		} else if dest.Type == "stdout" {
			destination = adapters.NewIOAdapterLoader(os.Stdout, dest.Format)
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

func WithKeyValueTransformer(extraValues map[string]interface{}, includeFields, excludeFields []string, keyField, valueField string) Option {
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

func WithDistributedMode(enabled bool) Option {
	return func(e *ETL) error {
		e.distributedMode = enabled
		if enabled {
			log.Println("[ETL] Distributed mode enabled.")
		}
		return nil
	}
}

func WithStreamingMode(enabled bool) Option {
	return func(e *ETL) error {
		e.streamingMode = enabled
		if enabled {
			log.Println("[ETL] Streaming mode enabled.")
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

// Enhancement: Option to inject a custom logger.
func WithLogger(logger *log.Logger) Option {
	return func(e *ETL) error {
		if logger == nil {
			return fmt.Errorf("WithLogger: provided logger is nil")
		}
		e.Logger = logger
		return nil
	}
}

// Enhancement: Option to set a maximum error threshold.
func WithMaxErrorThreshold(threshold int) Option {
	return func(e *ETL) error {
		if threshold <= 0 {
			return fmt.Errorf("WithMaxErrorThreshold: threshold must be positive")
		}
		e.maxErrorCount = threshold
		return nil
	}
}

// New type to represent source configuration.
type SourceConfig struct {
	Type   string  // e.g., "mongodb", "rest", "kafka", etc.
	DB     *sql.DB // used for SQL sources
	File   string  // file path or endpoint URL
	Table  string  // table name for file or DB source
	Query  string  // query for SQL sources
	Format string  // data format (e.g. "json", "csv", etc.)
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
