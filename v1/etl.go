package v1

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"

	"github.com/oarkflow/sql/v1/config"
	"github.com/oarkflow/sql/v1/contracts"
	"github.com/oarkflow/sql/v1/loaders"
	"github.com/oarkflow/sql/v1/mappers"
	"github.com/oarkflow/sql/v1/sources"
	"github.com/oarkflow/sql/v1/transformers"
)

// ETLJob encapsulates the ETL pipeline components.
type ETLJob struct {
	source        contracts.Source
	loader        contracts.Loader
	mappers       []contracts.Mapper
	transformers  []contracts.Transformer
	workerCount   int
	batchSize     int
	rawChanBuffer int
}

// Option is a function that configures an ETLJob.
type Option func(*ETLJob) error

// WithSource configures the source based on the provided parameters.
func WithSource(sourceType string, sourceDB *sql.DB, sourceFile, sourceTable, sourceQuery string) Option {
	return func(e *ETLJob) error {
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
		e.source = src
		return nil
	}
}

// WithDestination configures the loader based on the provided parameters.
func WithDestination(destType string, destDB *sql.DB, destFile string, cfg config.TableMapping) Option {
	return func(e *ETLJob) error {
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
		e.loader = loader
		return nil
	}
}

// WithMapping configures the field mappers.
func WithMapping(mapping map[string]string) Option {
	return func(e *ETLJob) error {
		mappersList := []contracts.Mapper{}
		if len(mapping) > 0 {
			mappersList = append(mappersList, mappers.NewFieldMapper(mapping))
		}
		// Always add a default mapper to convert field names to lowercase.
		mappersList = append(mappersList, &mappers.LowercaseMapper{})
		e.mappers = mappersList
		return nil
	}
}

// WithTransformers configures the transformers.
func WithTransformers() Option {
	return func(e *ETLJob) error {
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
	return func(e *ETLJob) error {
		e.workerCount = count
		return nil
	}
}

// WithBatchSize sets the batch size for loading.
func WithBatchSize(size int) Option {
	return func(e *ETLJob) error {
		e.batchSize = size
		return nil
	}
}

// WithRawChanBuffer sets the buffer size for the raw channel.
func WithRawChanBuffer(buffer int) Option {
	return func(e *ETLJob) error {
		e.rawChanBuffer = buffer
		return nil
	}
}

// NewETLJob creates an ETLJob with default settings and applies all provided options.
func NewETLJob(opts ...Option) (*ETLJob, error) {
	// Set default values.
	job := &ETLJob{
		workerCount:   2,
		batchSize:     100, // default batch size if not provided
		rawChanBuffer: 50,
	}
	for _, opt := range opts {
		if err := opt(job); err != nil {
			return nil, err
		}
	}
	// Validate required components.
	if job.source == nil {
		return nil, fmt.Errorf("source is not configured")
	}
	if job.loader == nil {
		return nil, fmt.Errorf("destination (loader) is not configured")
	}
	return job, nil
}

// Run executes the ETL pipeline.
func (e *ETLJob) Run(ctx context.Context) error {
	rawChan := make(chan contracts.Record, e.rawChanBuffer)
	var srcWG sync.WaitGroup

	if err := e.source.Setup(ctx); err != nil {
		return err
	}
	srcWG.Add(1)
	go func() {
		defer srcWG.Done()
		ch, err := e.source.Extract(ctx)
		if err != nil {
			log.Printf("Source extraction error: %v", err)
			return
		}
		for rec := range ch {
			rawChan <- rec
		}
	}()
	go func() {
		srcWG.Wait()
		close(rawChan)
	}()

	processedChan := make(chan contracts.Record, e.workerCount*2)
	var procWG sync.WaitGroup
	for i := 0; i < e.workerCount; i++ {
		procWG.Add(1)
		go func(workerID int) {
			defer procWG.Done()
			for raw := range rawChan {
				rec := raw
				for _, mapper := range e.mappers {
					var err error
					rec, err = mapper.Map(ctx, rec)
					if err != nil {
						log.Printf("[Worker %d] Mapper (%s) error: %v", workerID, mapper.Name(), err)
						rec = nil
						break
					}
				}
				if rec == nil {
					continue
				}
				for _, transformer := range e.transformers {
					var err error
					rec, err = transformer.Transform(ctx, rec)
					if err != nil {
						log.Printf("[Worker %d] Transformer error: %v", workerID, err)
						rec = nil
						break
					}
				}
				if rec != nil {
					processedChan <- rec
				}
			}
		}(i)
	}
	go func() {
		procWG.Wait()
		close(processedChan)
	}()

	batchChan := make(chan []contracts.Record, 10)
	var batchWG sync.WaitGroup
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		batch := make([]contracts.Record, 0, e.batchSize)
		for rec := range processedChan {
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				batchChan <- batch
				batch = make([]contracts.Record, 0, e.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()

	var loaderWG sync.WaitGroup
	loaderWG.Add(1)
	go func() {
		defer loaderWG.Done()
		if err := e.loader.Setup(ctx); err != nil {
			log.Printf("Loader setup error: %v", err)
			return
		}
		for batch := range batchChan {
			if err := e.loader.LoadBatch(ctx, batch); err != nil {
				log.Printf("LoadBatch error: %v", err)
			}
		}
	}()
	batchWG.Wait()
	loaderWG.Wait()
	return nil
}

// Close releases resources used by the ETL job.
func (e *ETLJob) Close() error {
	if err := e.source.Close(); err != nil {
		return err
	}
	if err := e.loader.Close(); err != nil {
		return err
	}
	return nil
}
