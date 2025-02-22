package etl

import (
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

func (t *Transaction) Begin() error {
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

func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committed = true
	t.rollbackActions = nil
	return nil
}

func (t *Transaction) Rollback() error {
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
		return t.Rollback()
	}
	return nil
}

func RunInTransaction(fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err)
		}
		return err
	}
	return tx.Commit()
}

type Transactional interface {
	Begin() error
	Commit() error
	Rollback() error
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
	sources       []contracts.Source
	mappers       []contracts.Mapper
	transformers  []contracts.Transformer
	loaders       []contracts.Loader
	workerCount   int
	batchSize     int
	retryCount    int
	retryDelay    time.Duration
	loaderWorkers int
}

func defaultConfig() *ETL {
	return &ETL{
		workerCount:   4,
		batchSize:     10,
		retryCount:    3,
		retryDelay:    100 * time.Millisecond,
		loaderWorkers: 2,
	}
}

func NewETL(opts ...Option) *ETL {
	etl := defaultConfig()
	for _, opt := range opts {
		opt(etl)
	}
	return etl
}

func (e *ETL) Run() error {
	rawChan := make(chan utils.Record)
	var srcWG sync.WaitGroup
	for _, s := range e.sources {
		if err := s.Setup(); err != nil {
			return err
		}
		srcWG.Add(1)
		go func(src contracts.Source) {
			defer srcWG.Done()
			ch, err := src.Extract()
			if err != nil {
				log.Printf("Source extraction error: %v", err)
				return
			}
			for rec := range ch {
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
					rec, err = mapper.Map(rec)
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
					rec, err = transformer.Transform(rec)
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
					if err := loader.Setup(); err != nil {
						log.Printf("[Loader Worker %d] Error setting up loader: %v", workerID, err)
						continue
					}
					if txnLoader, ok := loader.(Transactional); ok {
						if err := txnLoader.Begin(); err != nil {
							log.Printf("[Loader Worker %d] Error beginning transaction: %v", workerID, err)
							continue
						}
						err := robustRetry(e.retryCount, e.retryDelay, func() error {
							return loader.LoadBatch(batch)
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Error loading batch with transaction: %v", workerID, err)
							if rbErr := txnLoader.Rollback(); rbErr != nil {
								log.Printf("[Loader Worker %d] Rollback failed: %v", workerID, rbErr)
							}
							continue
						}
						if err := txnLoader.Commit(); err != nil {
							log.Printf("[Loader Worker %d] Commit failed: %v", workerID, err)
						}
					} else {
						err := RunInTransaction(func(tx *Transaction) error {
							return robustRetry(e.retryCount, e.retryDelay, func() error {
								return loader.LoadBatch(batch)
							})
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Error loading batch: %v", workerID, err)
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
