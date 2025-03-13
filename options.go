package etl

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/oarkflow/etl/pkg/adapter"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/contract"
	"github.com/oarkflow/etl/pkg/transformer"
	utils2 "github.com/oarkflow/etl/pkg/utils"
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

func NewSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery, format string) (contract.Source, error) {
	var src contract.Source
	if utils2.IsSQLType(sourceType) {
		if sourceDB == nil {
			return nil, fmt.Errorf("source database is nil")
		}
		src = adapter.NewSQLAdapterAsSource(sourceDB, sourceTable, sourceQuery)
	} else if sourceType == "csv" || sourceType == "json" {
		file := sourceTable
		if file == "" {
			file = sourceFile
		}
		src = adapter.NewFileAdapter(file, "source", false)
	} else if sourceType == "stdin" {
		return adapter.NewIOAdapterSource(os.Stdin, format), nil
	} else {
		return nil, fmt.Errorf("unsupported source type: %s", sourceType)
	}
	return src, nil
}

func WithSources(sources ...contract.Source) Option {
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
		var destination contract.Loader
		if utils2.IsSQLType(dest.Type) {
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			destination = adapter.NewSQLAdapterAsLoader(destDB, dest.Type, dest.Driver, cfg, cfg.NormalizeSchema)
		} else if dest.Type == "csv" || dest.Type == "json" {
			file := cfg.NewName
			if file == "" {
				file = dest.File
			}
			appendMode := true
			if cfg.TruncateDestination {
				appendMode = false
			}
			destination = adapter.NewFileAdapter(file, "loader", appendMode)
		} else if dest.Type == "stdout" {
			destination = adapter.NewIOAdapterLoader(os.Stdout, dest.Format)
		} else {
			return fmt.Errorf("unsupported destination type: %s", dest.Type)
		}
		e.loaders = append(e.loaders, destination)
		return nil
	}
}

func WithMappers(mapperList ...contract.Mapper) Option {
	return func(e *ETL) error {
		e.mappers = append(e.mappers, mapperList...)
		return nil
	}
}

func WithTransformers(list ...contract.Transformer) Option {
	return func(e *ETL) error {
		e.transformers = append(e.transformers, list...)
		return nil
	}
}

func WithKeyValueTransformer(extraValues map[string]interface{}, includeFields, excludeFields []string, keyField, valueField string) Option {
	return func(e *ETL) error {
		e.transformers = append(e.transformers, transformer.NewKeyValue(keyField, valueField, includeFields, excludeFields, extraValues))
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

func WithCheckpoint(store contract.CheckpointStore, cpFunc func(rec utils2.Record) string) Option {
	return func(e *ETL) error {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
		return nil
	}
}
