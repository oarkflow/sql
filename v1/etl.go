package v1

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/oarkflow/sql/utils"
	"github.com/oarkflow/sql/v1/contracts"
)

// retry executes fn up to retryCount times with an exponential backoff and jitter.
func retry(retryCount int, retryDelay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("Retry attempt %d failed: %v", i+1, err)
		jitter := 0.8 + rand.Float64()*0.4
		time.Sleep(time.Duration(float64(retryDelay) * jitter))
		retryDelay *= 2
	}
	return err
}

// ETL represents the entire pipeline configuration.
type ETL struct {
	sources         []contracts.Source
	mappers         []contracts.Mapper
	validators      []contracts.Validator
	transformers    []contracts.Transformer
	loaders         []contracts.Loader
	workerCount     int           // concurrent processing workers
	batchSize       int           // records per batch for loading
	retryCount      int           // number of retry attempts for loaders
	retryDelay      time.Duration // initial delay between retries
	loaderWorkers   int           // concurrent loader workers
	rawChanBuffer   int           // channel buffer for raw records
	checkpointStore contracts.CheckpointStore
	checkpointFunc  func(rec utils.Record) string
	lastCheckpoint  string
}

// defaultConfig returns a default configuration for the ETL pipeline.
func defaultConfig() *ETL {
	return &ETL{
		workerCount:   4,
		batchSize:     100,
		retryCount:    3,
		retryDelay:    100 * time.Millisecond,
		loaderWorkers: 2,
		rawChanBuffer: 100,
	}
}

// NewETL creates an ETL instance applying all provided options.
func NewETL(opts ...Option) *ETL {
	etl := defaultConfig()
	for _, opt := range opts {
		if err := opt(etl); err != nil {
			log.Printf("Error applying option: %v", err)
		}
	}
	return etl
}

// Run executes the ETL pipeline.
// It sets up all sources, applies mapping, transformation, and validation,
// batches records, and loads them using (optionally transactional) loaders.
// Checkpoints are retrieved and saved when configured.
func (e *ETL) Run(ctx context.Context) error {
	// Retrieve checkpoint to support resuming.
	if e.checkpointStore != nil {
		if cp, err := e.checkpointStore.GetCheckpoint(ctx); err != nil {
			log.Printf("Error retrieving checkpoint: %v", err)
		} else {
			e.lastCheckpoint = cp
			log.Printf("Resuming from checkpoint: %s", e.lastCheckpoint)
		}
	}

	rawChan := make(chan utils.Record, e.rawChanBuffer)
	var srcWG sync.WaitGroup

	// Start extraction from all configured sources.
	for _, s := range e.sources {
		if err := s.Setup(ctx); err != nil {
			return fmt.Errorf("source setup error: %v", err)
		}
		srcWG.Add(1)
		go func(src contracts.Source) {
			defer srcWG.Done()
			ch, err := src.Extract(ctx)
			if err != nil {
				log.Printf("Source extraction error: %v", err)
				return
			}
			for rec := range ch {
				// Optionally: check against e.lastCheckpoint to resume from a point.
				rawChan <- rec
			}
		}(s)
	}
	go func() {
		srcWG.Wait()
		close(rawChan)
	}()

	// Process records concurrently: mapping, transforming, and validating.
	processedChan := make(chan utils.Record, e.workerCount*2)
	var procWG sync.WaitGroup
	for i := 0; i < e.workerCount; i++ {
		procWG.Add(1)
		go func(workerID int) {
			defer procWG.Done()
			for raw := range rawChan {
				rec := raw
				// Apply mappers
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
				// Apply transformers
				for _, transformer := range e.transformers {
					var err error
					rec, err = transformer.Transform(ctx, rec)
					if err != nil {
						log.Printf("[Worker %d] Transformer error: %v", workerID, err)
						rec = nil
						break
					}
				}
				if rec == nil {
					continue
				}
				// Apply validators (if any)
				for _, validator := range e.validators {
					if err := validator.Validate(ctx, rec); err != nil {
						log.Printf("[Worker %d] Validation error: %v", workerID, err)
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

	// Batch records before loading.
	batchChan := make(chan []utils.Record, e.loaderWorkers)
	var batchWG sync.WaitGroup
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		batch := make([]utils.Record, 0, e.batchSize)
		for rec := range processedChan {
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				batchChan <- batch
				batch = make([]utils.Record, 0, e.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()

	// Load batches concurrently.
	var loaderWG sync.WaitGroup
	for i := 0; i < e.loaderWorkers; i++ {
		loaderWG.Add(1)
		go func(workerID int) {
			defer loaderWG.Done()
			for batch := range batchChan {
				for _, loader := range e.loaders {
					if err := loader.Setup(ctx); err != nil {
						log.Printf("[Loader Worker %d] Loader setup error: %v", workerID, err)
						continue
					}
					// If the loader supports transactions, wrap the batch load in a transaction.
					if txnLoader, ok := loader.(contracts.Transactional); ok {
						if err := txnLoader.Begin(ctx); err != nil {
							log.Printf("[Loader Worker %d] Error beginning transaction: %v", workerID, err)
							continue
						}
						err := retry(e.retryCount, e.retryDelay, func() error {
							return loader.LoadBatch(ctx, batch)
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Error loading batch with transaction: %v", workerID, err)
							if rbErr := txnLoader.Rollback(ctx); rbErr != nil {
								log.Printf("[Loader Worker %d] Rollback failed: %v", workerID, rbErr)
							}
							continue
						}
						if err := txnLoader.Commit(ctx); err != nil {
							log.Printf("[Loader Worker %d] Commit failed: %v", workerID, err)
							continue
						}
					} else {
						// For non-transactional loaders, wrap in a simulated transaction.
						err := RunInTransaction(ctx, func(tx *Transaction) error {
							return retry(e.retryCount, e.retryDelay, func() error {
								return loader.LoadBatch(ctx, batch)
							})
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Error loading batch: %v", workerID, err)
							continue
						}
					}
					// Update checkpoint if configured.
					if e.checkpointStore != nil && e.checkpointFunc != nil {
						cp := e.checkpointFunc(batch[len(batch)-1])
						if err := e.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
							log.Printf("[Loader Worker %d] Error saving checkpoint: %v", workerID, err)
						} else {
							e.lastCheckpoint = cp
						}
					}
				}
			}
		}(i)
	}
	batchWG.Wait()
	loaderWG.Wait()
	return nil
}

// Close releases resources held by all sources and loaders.
func (e *ETL) Close() error {
	for _, src := range e.sources {
		if err := src.Close(); err != nil {
			return fmt.Errorf("error closing source: %v", err)
		}
	}
	for _, loader := range e.loaders {
		if err := loader.Close(); err != nil {
			return fmt.Errorf("error closing loader: %v", err)
		}
	}
	return nil
}

// RunInTransaction simulates running a function within a transaction.
// Replace this placeholder with your actual transaction logic.
func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := &Transaction{}
	err := fn(tx)
	if err != nil {
		// Simulate rollback logic.
		return err
	}
	// Simulate commit logic.
	return nil
}

// Transaction is a placeholder type representing a transactional context.
// In a real implementation, this would wrap your database transaction.
type Transaction struct {
	// Add transaction fields and methods as needed.
}
