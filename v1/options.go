package v1

import (
	"database/sql"
	"fmt"

	"github.com/oarkflow/sql/utils"
	"github.com/oarkflow/sql/v1/config"
	"github.com/oarkflow/sql/v1/contracts"
	"github.com/oarkflow/sql/v1/loaders"
	"github.com/oarkflow/sql/v1/mappers"
	"github.com/oarkflow/sql/v1/sources"
	"github.com/oarkflow/sql/v1/transformers"
)

// Option is a function that configures an ETL.
type Option func(*ETL) error

// WithSource configures the source based on the provided parameters.
func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) Option {
	return func(e *ETL) error {
		var src contracts.Source
		switch sourceType {
		case "mysql", "postgresql":
			if sourceDB == nil {
				return fmt.Errorf("source database is nil")
			}
			src = sources.NewSQLSource(sourceDB, sourceTable, sourceQuery)
		case "csv":
			file := sourceTable
			if file == "" {
				file = sourceFile
			}
			src = sources.NewCSVSource(file)
		case "json":
			file := sourceTable
			if file == "" {
				file = sourceFile
			}
			src = sources.NewJSONSource(file)
		default:
			return fmt.Errorf("unsupported source type: %s", sourceType)
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

// WithDestination configures the loader based on the provided parameters.
func WithDestination(destType string, destDB *sql.DB, destFile string, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var loader contracts.Loader
		switch destType {
		case "mysql", "postgresql":
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			if cfg.KeyValueTable {
				loader = loaders.NewKeyValueLoader(destDB, destType, cfg)
			} else {
				loader = loaders.NewSQLLoader(destDB, destType, cfg)
			}
		case "csv":
			file := cfg.NewName
			if file == "" {
				file = destFile
			}
			loader = loaders.NewCSVLoader(file)
		case "json":
			file := cfg.NewName
			if file == "" {
				file = destFile
			}
			loader = loaders.NewJSONLoader(file)
		default:
			return fmt.Errorf("unsupported destination type: %s", destType)
		}
		e.loaders = append(e.loaders, loader)
		return nil
	}
}

// WithMapping configures the field mappers.
func WithMapping(mapping map[string]string) Option {
	return func(e *ETL) error {
		var mappersList []contracts.Mapper
		if len(mapping) > 0 {
			mappersList = append(mappersList, mappers.NewFieldMapper(mapping))
		}
		// Always add a default mapper to convert field names to lowercase.
		mappersList = append(mappersList, &mappers.LowercaseMapper{})
		e.mappers = append(e.mappers, mappersList...)
		return nil
	}
}

// WithTransformers configures the transformers.
func WithTransformers() Option {
	return func(e *ETL) error {
		transformersList := []contracts.Transformer{
			&transformers.LookupTransformer{
				LookupData:  map[string]string{"key1": "value1"},
				Field:       "some_field",
				TargetField: "lookup_result",
			},
		}
		e.transformers = transformersList
		return nil
	}
}

// WithWorkerCount sets the number of concurrent workers.
func WithWorkerCount(count int) Option {
	return func(e *ETL) error {
		e.workerCount = count
		return nil
	}
}

// WithBatchSize sets the batch size for loading.
func WithBatchSize(size int) Option {
	return func(e *ETL) error {
		e.batchSize = size
		return nil
	}
}

// WithRawChanBuffer sets the buffer size for the raw channel.
func WithRawChanBuffer(buffer int) Option {
	return func(e *ETL) error {
		e.rawChanBuffer = buffer
		return nil
	}
}

func WithCheckpoint(store contracts.CheckpointStore, cpFunc func(rec utils.Record) string) Option {
	return func(e *ETL) error {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
		return nil
	}
}
