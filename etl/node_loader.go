package etl

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/dgraph-io/ristretto"
	"github.com/oarkflow/transaction"
	
	"github.com/oarkflow/sql/pkg/adapters/sqladapter"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/transactions"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/sql/pkg/utils/sqlutil"
)

type LoaderNode struct {
	loaders            []contracts.Loader
	workerCount        int
	desiredWorkerCount int32
	batchSize          int
	retryCount         int
	retryDelay         time.Duration
	circuitBreaker     *transactions.CircuitBreaker
	checkpointStore    contracts.CheckpointStore
	checkpointFunc     func(rec utils.Record) string
	cpMutex            *sync.Mutex
	lastCheckpoint     *atomic.Value
	hooks              *LifecycleHooks
	validations        *Validations
	eventBus           *EventBus
	dedupEnabled       bool
	dedupField         string
	dedupCache         *ristretto.Cache
	deadLetterLock     sync.Mutex
	deadLetterQueue    []utils.Record
	metrics            *Metrics
	inChan             <-chan utils.Record
	batchChan          chan []utils.Record
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	workerProgress     map[int]int64
	NodeName           string
	checkpointInterval time.Duration
	lastCheckpointTime time.Time
	deadLetterQueueCap int
}

func (ln *LoaderNode) Process(ctx context.Context, in <-chan utils.Record, cfg config.TableMapping, args ...any) (<-chan utils.Record, error) {
	done := make(chan utils.Record)
	ln.inChan = in
	ln.batchChan = make(chan []utils.Record, ln.workerCount)
	ln.workerProgress = make(map[int]int64)
	atomic.StoreInt32(&ln.desiredWorkerCount, int32(ln.workerCount))
	go ln.batchRecords(ctx)
	for i := 0; i < ln.workerCount; i++ {
		ln.wg.Add(1)
		go ln.loaderWorker(ctx, i, cfg, args...)
	}
	go func() {
		ln.wg.Wait()
		close(done)
	}()
	return done, nil
}

func (ln *LoaderNode) fingerprint(rec utils.Record) string {
	if ln.dedupField != "" {
		if v, ok := rec[ln.dedupField]; ok {
			return fmt.Sprint(v)
		}
	}
	// FNV is faster than full SHA
	hasher := fnv.New64a()
	for k, v := range rec {
		fmt.Fprintf(hasher, "%s=%v;", k, v)
	}
	return fmt.Sprint(hasher.Sum64())
}

func (ln *LoaderNode) batchRecords(ctx context.Context) {
	batch := make([]utils.Record, 0, ln.batchSize)
	for rec := range ln.inChan {
		select {
		case <-ctx.Done():
			return
		default:
			batch = append(batch, rec)
			if len(batch) >= ln.batchSize {
				ln.batchChan <- batch
				batch = make([]utils.Record, 0, ln.batchSize)
			}
		}
	}
	if len(batch) > 0 {
		ln.batchChan <- batch
	}
	close(ln.batchChan)
}

func (ln *LoaderNode) saveCheckpoint(cp string) {
	ln.lastCheckpoint.Store(cp)
}

func (ln *LoaderNode) loaderWorker(ctx context.Context, index int, cfg config.TableMapping, args ...any) {
	defer ln.wg.Done()
	var localLoaded int64 = 0
	var localFailed int64 = 0
	enableBatch := cfg.EnableBatch
	for {
		if index >= int(atomic.LoadInt32(&ln.desiredWorkerCount)) {
			ln.mu.Lock()
			ln.workerProgress[index] = localLoaded
			ln.mu.Unlock()
			activity := WorkerActivity{
				Node:      ln.NodeName,
				WorkerID:  index,
				Processed: localLoaded,
				Failed:    localFailed,
				Timestamp: time.Now(),
				Activity:  "exited",
			}
			ln.metrics.AddWorkerActivity(activity)
			log.Printf("[LoaderNode Worker %d] exiting due to reduced worker count; loaded %d records", index, localLoaded)
			return
		}
		select {
		case batch, ok := <-ln.batchChan:
			if !ok {
				ln.mu.Lock()
				ln.workerProgress[index] = localLoaded
				ln.mu.Unlock()
				log.Printf("[LoaderNode Worker %d] finished processing; loaded %d records", index, localLoaded)
				return
			}
			if ln.dedupEnabled {
				uniqueBatch := make([]utils.Record, 0, len(batch))
				for _, rec := range batch {
					key := ln.fingerprint(rec)
					_, exists := ln.dedupCache.Get(key)
					if !exists {
						ln.dedupCache.Set(key, true, 1)
						uniqueBatch = append(uniqueBatch, rec)
						continue
					}
				}
				batch = uniqueBatch
			}
			if ln.eventBus != nil {
				ln.eventBus.Publish("BeforeLoad", batch)
			}
			if ln.validations != nil && ln.validations.ValidateBeforeLoad != nil {
				if err := ln.validations.ValidateBeforeLoad(ctx, batch); err != nil {
					log.Printf("[LoaderNode Worker %d] ValidateBeforeLoad error: %v", index, err)
					localFailed++
					atomic.AddInt64(&ln.metrics.Errors, 1)
					continue
				}
			}
			if ln.hooks != nil && ln.hooks.BeforeLoad != nil {
				if err := ln.hooks.BeforeLoad(ctx, batch); err != nil {
					log.Printf("[LoaderNode Worker %d] BeforeLoad hook error: %v", index, err)
					localFailed++
					atomic.AddInt64(&ln.metrics.Errors, 1)
					continue
				}
			}
			batchCtx := context.WithValue(ctx, "batch", index)
			storeCtx := batchCtx
			if batchCtx.Err() != nil {
				storeCtx = context.Background()
			}
			for _, loader := range ln.loaders {
				if enableBatch {
					if txnLoader, ok := loader.(contracts.Transactional); ok {
						if err := txnLoader.Begin(storeCtx); err != nil {
							log.Printf("[LoaderNode Worker %d] Begin transaction error: %v", index, err)
							localFailed++
							atomic.AddInt64(&ln.metrics.Errors, 1)
							continue
						}
						if sqlLoader, ok := loader.(*sqladapter.Adapter); ok && sqlLoader.AutoCreate && !sqlLoader.Created {
							if err := sqlutil.CreateTableFromRecord(sqlLoader.Db, sqlLoader.Driver, sqlLoader.Table, sqlLoader.NormalizeSchema); err != nil {
								log.Printf("[LoaderNode Worker %d] Table creation error: %v", index, err)
								localFailed++
								atomic.AddInt64(&ln.metrics.Errors, 1)
								continue
							}
							sqlLoader.Created = true
						}
						err := transactions.RetryWithCircuit(ln.retryCount, ln.retryDelay, ln.circuitBreaker, func() error {
							return loader.StoreBatch(storeCtx, batch)
						})
						if err != nil {
							log.Printf("[LoaderNode Worker %d] Batch load error (transaction): %v", index, err)
							localFailed++
							atomic.AddInt64(&ln.metrics.Errors, 1)
							ln.deadLetterLock.Lock()
							if len(ln.deadLetterQueue) < ln.deadLetterQueueCap {
								ln.deadLetterQueue = append(ln.deadLetterQueue, batch...)
							} else {
								log.Printf("Dead letter queue capacity reached; dropping failed batch")
							}
							ln.deadLetterLock.Unlock()
							continue
						}
						if err := txnLoader.Commit(storeCtx); err != nil {
							log.Printf("[LoaderNode Worker %d] Commit error: %v", index, err)
							localFailed++
							atomic.AddInt64(&ln.metrics.Errors, 1)
							ln.deadLetterLock.Lock()
							if len(ln.deadLetterQueue) < ln.deadLetterQueueCap {
								ln.deadLetterQueue = append(ln.deadLetterQueue, batch...)
							} else {
								log.Printf("Dead letter queue capacity reached; dropping failed batch")
							}
							ln.deadLetterLock.Unlock()
							continue
						}
					} else {
						err := transaction.RunInTransaction(storeCtx, func(tx *transaction.Transaction) error {
							return transactions.RetryWithCircuit(ln.retryCount, ln.retryDelay, ln.circuitBreaker, func() error {
								return loader.StoreBatch(storeCtx, batch)
							})
						})
						if err != nil {
							log.Printf("[LoaderNode Worker %d] Batch load error: %v", index, err)
							localFailed++
							atomic.AddInt64(&ln.metrics.Errors, 1)
							ln.deadLetterLock.Lock()
							if len(ln.deadLetterQueue) < ln.deadLetterQueueCap {
								ln.deadLetterQueue = append(ln.deadLetterQueue, batch...)
							} else {
								log.Printf("Dead letter queue capacity reached; dropping failed batch")
							}
							ln.deadLetterLock.Unlock()
							continue
						}
					}
				} else {
					for _, rec := range batch {
						err := loader.StoreSingle(storeCtx, rec)
						if err != nil {
							log.Printf("[LoaderNode Worker %d] Single record load error: %v", index, err)
							localFailed++
							atomic.AddInt64(&ln.metrics.Errors, 1)
							ln.deadLetterLock.Lock()
							if len(ln.deadLetterQueue) < ln.deadLetterQueueCap {
								ln.deadLetterQueue = append(ln.deadLetterQueue, rec)
							} else {
								log.Printf("Dead letter queue capacity reached; dropping failed record")
							}
							ln.deadLetterLock.Unlock()
							continue
						}
					}
				}
				localLoaded += int64(len(batch))
				atomic.AddInt64(&ln.metrics.Loaded, int64(len(batch)))
				if ln.checkpointStore != nil && ln.checkpointFunc != nil {
					cp := ln.checkpointFunc(batch[len(batch)-1])
					now := time.Now()
					ln.cpMutex.Lock()
					if now.Sub(ln.lastCheckpointTime) >= ln.checkpointInterval {
						currentCp, _ := ln.lastCheckpoint.Load().(string)
						if cp > currentCp {
							if err := ln.checkpointStore.SaveCheckpoint(context.Background(), cp); err != nil {
								log.Printf("[LoaderNode Worker %d] Checkpoint error: %v", index, err)
								localFailed++
								atomic.AddInt64(&ln.metrics.Errors, 1)
							} else {
								ln.lastCheckpoint.Store(cp)
								ln.lastCheckpointTime = now
							}
						}
					}
					ln.cpMutex.Unlock()
				}
			}
			if ln.eventBus != nil {
				ln.eventBus.Publish("AfterLoad", batch)
			}
			if ln.hooks != nil && ln.hooks.AfterLoad != nil {
				if err := ln.hooks.AfterLoad(ctx, batch); err != nil {
					log.Printf("[LoaderNode Worker %d] AfterLoad hook error: %v", index, err)
				}
			}
			if ln.validations != nil && ln.validations.ValidateAfterLoad != nil {
				if err := ln.validations.ValidateAfterLoad(ctx, batch); err != nil {
					log.Printf("[LoaderNode Worker %d] ValidateAfterLoad error: %v", index, err)
				}
			}
		case <-ctx.Done():
			ln.mu.Lock()
			ln.workerProgress[index] = localLoaded
			ln.mu.Unlock()
			return
		}
	}
}

func (ln *LoaderNode) AdjustWorker(newCount int) {
	ln.mu.Lock()
	oldCount := int(atomic.LoadInt32(&ln.desiredWorkerCount))
	atomic.StoreInt32(&ln.desiredWorkerCount, int32(newCount))
	ln.mu.Unlock()
	if newCount > oldCount {
		log.Printf("[LoaderNode] Increased worker count from %d to %d", oldCount, newCount)
	} else {
		log.Printf("[LoaderNode] Decreased worker count from %d to %d; extra workers will exit and their progress stored", oldCount, newCount)
	}
}
