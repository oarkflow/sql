package etl

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type TransformNode struct {
	transformers       []contracts.Transformer
	workerCount        int
	desiredWorkerCount int32
	hooks              *LifecycleHooks
	eventBus           *EventBus
	metrics            *Metrics
	inChan             <-chan utils.Record
	outChan            chan utils.Record
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	workerProgress     map[int]int64
	NodeName           string
	deadLetterQueue    chan utils.Record
	ctx                context.Context
}

func (tn *TransformNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping, args ...any) (<-chan utils.Record, error) {
	tn.ctx = ctx
	tn.inChan = in
	tn.outChan = make(chan utils.Record, tn.workerCount*2)
	tn.workerProgress = make(map[int]int64)
	atomic.StoreInt32(&tn.desiredWorkerCount, int32(tn.workerCount))
	for i := 0; i < tn.workerCount; i++ {
		tn.wg.Add(1)
		go tn.transformWorker(ctx, i)
	}
	go func() {
		tn.wg.Wait()
		for _, t := range tn.transformers {
			if flushable, ok := t.(contracts.Flushable); ok {
				flushRecords, err := flushable.Flush(ctx)
				if err != nil {
					log.Printf("[TransformNode] Flush error: %v", err)
					atomic.AddInt64(&tn.metrics.Errors, 1)
					continue
				}
				for _, r := range flushRecords {
					tn.outChan <- r
					atomic.AddInt64(&tn.metrics.Transformed, 1)
				}
			}
		}
		close(tn.outChan)
	}()
	return tn.outChan, nil
}

func (tn *TransformNode) transformWorker(ctx context.Context, index int) {
	defer tn.wg.Done()
	var localCount int64 = 0
	var localFailed int64 = 0
	for {
		if index >= int(atomic.LoadInt32(&tn.desiredWorkerCount)) {
			tn.mu.Lock()
			tn.workerProgress[index] = localCount
			tn.mu.Unlock()
			activity := WorkerActivity{
				Node:      tn.NodeName,
				WorkerID:  index,
				Processed: localCount,
				Failed:    localFailed,
				Timestamp: time.Now(),
				Activity:  "exited",
			}
			tn.metrics.AddWorkerActivity(activity)
			log.Printf("[TransformNode Worker %d] exiting due to reduced worker count; processed %d records", index, localCount)
			return
		}
		select {
		case rec, ok := <-tn.inChan:
			if !ok {
				tn.mu.Lock()
				tn.workerProgress[index] = localCount
				tn.mu.Unlock()
				return
			}
			if tn.eventBus != nil {
				tn.eventBus.Publish("BeforeTransform", rec)
			}
			if tn.hooks != nil && tn.hooks.BeforeTransform != nil {
				if err := tn.hooks.BeforeTransform(ctx, rec); err != nil {
					log.Printf("[TransformNode Worker %d] BeforeTransform hook error: %v", index, err)
				}
			}
			transformed, err := applyTransformers(ctx, rec, tn.transformers, index, tn.metrics)
			if err != nil {
				log.Printf("[TransformNode Worker %d] Error: %v", index, err)
				localFailed++
				atomic.AddInt64(&tn.metrics.Errors, 1)
				tn.deadLetterQueue <- rec
				continue
			}
			for _, r := range transformed {
				if tn.eventBus != nil {
					tn.eventBus.Publish("AfterTransform", r)
				}
				if tn.hooks != nil && tn.hooks.AfterTransform != nil {
					if err := tn.hooks.AfterTransform(ctx, r); err != nil {
						log.Printf("[TransformNode Worker %d] AfterTransform hook error: %v", index, err)
					}
				}
				if r != nil {
					tn.outChan <- r
					atomic.AddInt64(&tn.metrics.Transformed, 1)
				}
			}
			localCount++
		case <-ctx.Done():
			tn.mu.Lock()
			tn.workerProgress[index] = localCount
			tn.mu.Unlock()
			return
		}
	}
}

func (tn *TransformNode) AdjustWorker(newCount int) {
	tn.mu.Lock()
	oldCount := int(atomic.LoadInt32(&tn.desiredWorkerCount))
	atomic.StoreInt32(&tn.desiredWorkerCount, int32(newCount))
	tn.mu.Unlock()
	if newCount > oldCount {
		for i := oldCount; i < newCount; i++ {
			tn.wg.Add(1)
			go tn.transformWorker(tn.ctx, i)
			activity := WorkerActivity{
				Node:      tn.NodeName,
				WorkerID:  i,
				Processed: 0,
				Failed:    0,
				Timestamp: time.Now(),
				Activity:  "spawned",
			}
			tn.metrics.AddWorkerActivity(activity)
		}
		log.Printf("[TransformNode] Increased worker count from %d to %d", oldCount, newCount)
	} else {
		log.Printf("[TransformNode] Decreased worker count from %d to %d; extra workers will exit and their progress stored", oldCount, newCount)
	}
}
