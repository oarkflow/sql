package v1

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/oarkflow/sql/v1/contracts"
)

// ETL encapsulates the ETL pipeline components.
type ETL struct {
	source        contracts.Source
	loader        contracts.Loader
	mappers       []contracts.Mapper
	transformers  []contracts.Transformer
	workerCount   int
	batchSize     int
	rawChanBuffer int
}

// NewETL creates an ETL with default settings and applies all provided options.
func NewETL(opts ...Option) (*ETL, error) {
	// Set default values.
	job := &ETL{
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
func (e *ETL) Run(ctx context.Context) error {
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
func (e *ETL) Close() error {
	if err := e.source.Close(); err != nil {
		return err
	}
	if err := e.loader.Close(); err != nil {
		return err
	}
	return nil
}
