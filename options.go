package etl

import (
	"database/sql"
	"fmt"

	"github.com/oarkflow/etl/adapters"
	"github.com/oarkflow/etl/config"
	"github.com/oarkflow/etl/contract"
	"github.com/oarkflow/etl/transformers"
	"github.com/oarkflow/etl/utils"
)

type Option func(*ETL) error

func NewSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) (contract.Source, error) {
	var src contract.Source
	if utils.IsSQLType(sourceType) {
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

func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) Option {
	return func(e *ETL) error {
		src, err := NewSource(sourceType, sourceDB, sourceFile, sourceTable, sourceQuery)
		if err != nil {
			return err
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

func WithDestination(destType string, destDB *sql.DB, destFile string, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var destination contract.Loader
		if utils.IsSQLType(destType) {
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			destination = adapters.NewSQLAdapterAsLoader(destDB, destType, cfg)
		} else if destType == "csv" || destType == "json" {
			file := cfg.NewName
			if file == "" {
				file = destFile
			}
			appendMode := true
			if cfg.TruncateDestination {
				appendMode = false
			}
			destination = adapters.NewFileAdapter(file, "loader", appendMode)
		} else {
			return fmt.Errorf("unsupported destination type: %s", destType)
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
		kt := &transformers.KeyValueTransformer{
			ExtraValues:   extraValues,
			IncludeFields: includeFields,
			ExcludeFields: excludeFields,
			KeyField:      keyField,
			ValueField:    valueField,
		}
		e.transformers = append(e.transformers, kt)
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

func WithCheckpoint(store contract.CheckpointStore, cpFunc func(rec utils.Record) string) Option {
	return func(e *ETL) error {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
		return nil
	}
}
