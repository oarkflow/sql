package etl

import (
	"log"
	"sync"
	"time"

	"github.com/oarkflow/sql/etl/contracts"
	"github.com/oarkflow/sql/utils"
)

func retry(retryCount int, retryDelay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("Retry attempt %d failed: %v", i+1, err)
		time.Sleep(retryDelay)
		retryDelay *= 2
	}
	return err
}

type ETL struct {
	sources      []contracts.Source
	mappers      []contracts.Mapper
	transformers []contracts.Transformer
	loaders      []contracts.Loader
	workerCount  int
	batchSize    int
	retryCount   int
	retryDelay   time.Duration
}

func defaultConfig() *ETL {
	return &ETL{
		workerCount: 4,
		batchSize:   10,
		retryCount:  3,
		retryDelay:  100 * time.Millisecond,
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
		err := s.Setup()
		if err != nil {
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
	batch := make([]utils.Record, 0, e.batchSize)
	for rec := range processedChan {
		batch = append(batch, rec)
		if len(batch) >= e.batchSize {
			for _, loader := range e.loaders {
				err := loader.Setup()
				if err == nil {
					err = retry(e.retryCount, e.retryDelay, func() error {
						return loader.LoadBatch(batch)
					})
					if err != nil {
						log.Printf("Error loading batch: %v", err)
					}
				} else {
					log.Printf("Error setting up loader: %v", err)
				}
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		for _, loader := range e.loaders {
			err := retry(e.retryCount, e.retryDelay, func() error {
				return loader.LoadBatch(batch)
			})
			if err != nil {
				log.Printf("Error loading final batch: %v", err)
			}
		}
	}
	return nil
}

func (e *ETL) Close() error {
	for _, src := range e.sources {
		err := src.Close()
		if err != nil {
			return err
		}
	}
	for _, src := range e.loaders {
		err := src.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
