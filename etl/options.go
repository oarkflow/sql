package etl

import (
	"database/sql"
	"fmt"

	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/etl/contract"
	"github.com/oarkflow/sql/etl/loader"
	"github.com/oarkflow/sql/etl/mapper"
	"github.com/oarkflow/sql/etl/source"
	"github.com/oarkflow/sql/etl/transformers"
	"github.com/oarkflow/sql/utils"
)

type Option func(*ETL) error

func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) Option {
	return func(e *ETL) error {
		var src contract.Source
		switch sourceType {
		case "mysql", "postgresql":
			if sourceDB == nil {
				return fmt.Errorf("source database is nil")
			}
			src = source.NewSQLSource(sourceDB, sourceTable, sourceQuery)
		case "csv", "json":
			file := sourceTable
			if file == "" {
				file = sourceFile
			}
			src = source.NewFileSource(file)
		default:
			return fmt.Errorf("unsupported source type: %s", sourceType)
		}
		e.sources = append(e.sources, src)
		return nil
	}
}

func WithDestination(destType string, destDB *sql.DB, destFile string, cfg config.TableMapping) Option {
	return func(e *ETL) error {
		var destination contract.Loader
		switch destType {
		case "mysql", "postgresql":
			if destDB == nil {
				return fmt.Errorf("destination database is nil")
			}
			if cfg.KeyValueTable {
				destination = loader.NewKeyValueLoader(destDB, destType, cfg)
			} else {
				destination = loader.NewSQLLoader(destDB, destType, cfg)
			}
		case "csv", "json":
			file := cfg.NewName
			if file == "" {
				file = destFile
			}
			destination = loader.NewFileLoader(file, true)
		default:
			return fmt.Errorf("unsupported destination type: %s", destType)
		}
		e.loaders = append(e.loaders, destination)
		return nil
	}
}

func WithMapping(mapping map[string]string) Option {
	return func(e *ETL) error {
		var mappersList []contract.Mapper
		if len(mapping) > 0 {
			mappersList = append(mappersList, mapper.NewFieldMapper(mapping))
		}
		mappersList = append(mappersList, &mapper.LowercaseMapper{})
		e.mappers = append(e.mappers, mappersList...)
		return nil
	}
}

func WithTransformers() Option {
	return func(e *ETL) error {
		transformersList := []contract.Transformer{
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
