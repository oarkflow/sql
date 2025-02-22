package etl

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/oarkflow/sql/etl/contracts"
	"github.com/oarkflow/sql/utils"
)

type Transaction struct {
	mu              sync.Mutex
	rollbackActions []func() error
	committed       bool
}

func NewTransaction() *Transaction {
	return &Transaction{
		rollbackActions: make([]func() error, 0),
	}
}

func (t *Transaction) Begin(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committed = false
	t.rollbackActions = make([]func() error, 0)
	return nil
}

func (t *Transaction) RegisterRollback(fn func() error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.committed {
		t.rollbackActions = append(t.rollbackActions, fn)
	}
}

func (t *Transaction) Commit(_ context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committed = true
	t.rollbackActions = nil
	return nil
}

func (t *Transaction) Rollback(_ context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var err error
	for i := len(t.rollbackActions) - 1; i >= 0; i-- {
		if e := t.rollbackActions[i](); e != nil && err == nil {
			err = e
		}
	}
	t.rollbackActions = nil
	return err
}

func (t *Transaction) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.committed && len(t.rollbackActions) > 0 {
		return t.Rollback(context.Background())
	}
	return nil
}

func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err)
		}
		return err
	}
	return tx.Commit(ctx)
}

func robustRetry(retryCount int, retryDelay time.Duration, fn func() error) error {
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

type ETL struct {
	sources         []contracts.Source
	mappers         []contracts.Mapper
	validators      []contracts.Validator
	transformers    []contracts.Transformer
	loaders         []contracts.Loader
	workerCount     int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	loaderWorkers   int
	rawChanBuffer   int
	checkpointStore contracts.CheckpointStore
	checkpointFunc  func(rec utils.Record) string
	lastCheckpoint  string
}

func defaultConfig() *ETL {
	return &ETL{
		workerCount:   4,
		batchSize:     10,
		retryCount:    3,
		retryDelay:    100 * time.Millisecond,
		loaderWorkers: 2,
		rawChanBuffer: 100,
	}
}

func NewETL(opts ...Option) *ETL {
	etl := defaultConfig()
	for _, opt := range opts {
		opt(etl)
	}
	return etl
}

func (e *ETL) Run(ctx context.Context) error {
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
	for _, s := range e.sources {
		if err := s.Setup(ctx); err != nil {
			return err
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
				if (cap(rawChan) - len(rawChan)) < 10 {
					log.Printf("Backpressure: rawChan nearly full: %d/%d", len(rawChan), cap(rawChan))
				}
				rawChan <- rec
			}
		}(s)
	}
	go func() {
		srcWG.Wait()
		close(rawChan)
	}()
	processedChan := make(chan utils.Record, e.workerCount*2)
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
						log.Printf("[Worker %d] Mapper error: %v", workerID, err)
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
				if rec == nil {
					continue
				}
				if e.validators != nil {
					for _, validator := range e.validators {
						if err := validator.Validate(ctx, rec); err != nil {
							log.Printf("[Worker %d] Validation error: %v", workerID, err)
							rec = nil
							break
						}
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
	var loaderWG sync.WaitGroup
	for i := 0; i < e.loaderWorkers; i++ {
		loaderWG.Add(1)
		go func(workerID int) {
			defer loaderWG.Done()
			for batch := range batchChan {
				for _, loader := range e.loaders {
					if err := loader.Setup(ctx); err != nil {
						log.Printf("[Loader Worker %d] Error setting up loader: %v", workerID, err)
						continue
					}
					if txnLoader, ok := loader.(contracts.Transactional); ok {
						if err := txnLoader.Begin(ctx); err != nil {
							log.Printf("[Loader Worker %d] Error beginning transaction: %v", workerID, err)
							continue
						}
						err := robustRetry(e.retryCount, e.retryDelay, func() error {
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
						if e.checkpointStore != nil && e.checkpointFunc != nil {
							cp := e.checkpointFunc(batch[len(batch)-1])
							if err := e.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
								log.Printf("[Loader Worker %d] Error saving checkpoint: %v", workerID, err)
							} else {
								e.lastCheckpoint = cp
							}
						}
					} else {
						err := RunInTransaction(ctx, func(tx *Transaction) error {
							return robustRetry(e.retryCount, e.retryDelay, func() error {
								return loader.LoadBatch(ctx, batch)
							})
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Error loading batch: %v", workerID, err)
							continue
						}
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
			}
		}(i)
	}
	batchWG.Wait()
	loaderWG.Wait()
	return nil
}

func (e *ETL) Close() error {
	for _, src := range e.sources {
		if err := src.Close(); err != nil {
			return err
		}
	}
	for _, loader := range e.loaders {
		if err := loader.Close(); err != nil {
			return err
		}
	}
	return nil
}
