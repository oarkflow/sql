package etl

import (
	"time"

	"github.com/oarkflow/sql/etl/contracts"
	"github.com/oarkflow/sql/utils"
)

type Option func(*ETL)

func WithWorkerCount(n int) Option {
	return func(e *ETL) {
		e.workerCount = n
	}
}

func WithBatchSize(size int) Option {
	return func(e *ETL) {
		e.batchSize = size
	}
}

func WithRetryCount(count int) Option {
	return func(e *ETL) {
		e.retryCount = count
	}
}

func WithRetryDelay(delay time.Duration) Option {
	return func(e *ETL) {
		e.retryDelay = delay
	}
}

func WithSources(sources ...contracts.Source) Option {
	return func(e *ETL) {
		e.sources = sources
	}
}

func WithMappers(mappers ...contracts.Mapper) Option {
	return func(e *ETL) {
		e.mappers = mappers
	}
}

func WithTransformers(transformers ...contracts.Transformer) Option {
	return func(e *ETL) {
		e.transformers = transformers
	}
}

func WithValidators(validators ...contracts.Validator) Option {
	return func(e *ETL) {
		e.validators = validators
	}
}

func WithRawChanBuffer(size int) Option {
	return func(e *ETL) {
		e.rawChanBuffer = size
	}
}

func WithLoaders(loaders ...contracts.Loader) Option {
	return func(e *ETL) {
		e.loaders = loaders
	}
}

func WithCheckpointStore(store contracts.CheckpointStore, cpFunc func(rec utils.Record) string) Option {
	return func(e *ETL) {
		e.checkpointStore = store
		e.checkpointFunc = cpFunc
	}
}
