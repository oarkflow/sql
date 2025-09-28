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

type MapNode struct {
	mappers            []contracts.Mapper
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
	ctx                context.Context
}

func (mn *MapNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping, args ...any) (<-chan utils.Record, error) {
	mn.ctx = ctx
	mn.inChan = in
	mn.outChan = make(chan utils.Record, mn.workerCount*2)
	mn.workerProgress = make(map[int]int64)
	atomic.StoreInt32(&mn.desiredWorkerCount, int32(mn.workerCount))
	for i := 0; i < mn.workerCount; i++ {
		mn.wg.Add(1)
		go mn.mapWorker(ctx, i)
	}
	go func() {
		mn.wg.Wait()
		close(mn.outChan)
	}()
	return mn.outChan, nil
}

func (mn *MapNode) mapWorker(ctx context.Context, index int) {
	defer mn.wg.Done()
	var localCount int64 = 0
	var localFailed int64 = 0
	for {
		if index >= int(atomic.LoadInt32(&mn.desiredWorkerCount)) {
			mn.mu.Lock()
			mn.workerProgress[index] = localCount
			mn.mu.Unlock()
			activity := WorkerActivity{
				Node:      mn.NodeName,
				WorkerID:  index,
				Processed: localCount,
				Failed:    localFailed,
				Timestamp: time.Now(),
				Activity:  "exited",
			}
			mn.metrics.AddWorkerActivity(activity)
			log.Printf("[MapNode Worker %d] exiting due to reduced worker count; processed %d records", index, localCount)
			return
		}
		select {
		case rec, ok := <-mn.inChan:
			if !ok {
				mn.mu.Lock()
				mn.workerProgress[index] = localCount
				mn.mu.Unlock()
				return
			}
			if mn.eventBus != nil {
				mn.eventBus.Publish("BeforeMapper", rec)
			}
			if mn.hooks != nil && mn.hooks.BeforeMapper != nil {
				if err := mn.hooks.BeforeMapper(ctx, rec); err != nil {
					log.Printf("[MapNode Worker %d] BeforeMapper hook error: %v", index, err)
				}
			}
			mapped, err := applyMappers(ctx, rec, mn.mappers, index)
			if err != nil {
				log.Printf("[MapNode Worker %d] Error: %v", index, err)
				localFailed++
				atomic.AddInt64(&mn.metrics.Errors, 1)
				continue
			}
			if mn.eventBus != nil {
				mn.eventBus.Publish("AfterMapper", mapped)
			}
			if mn.hooks != nil && mn.hooks.AfterMapper != nil {
				if err := mn.hooks.AfterMapper(ctx, mapped); err != nil {
					log.Printf("[MapNode Worker %d] AfterMapper hook error: %v", index, err)
				}
			}
			if mapped != nil {
				mn.outChan <- mapped
			}
			localCount++
			atomic.AddInt64(&mn.metrics.Mapped, 1)
		case <-ctx.Done():
			mn.mu.Lock()
			mn.workerProgress[index] = localCount
			mn.mu.Unlock()
			return
		}
	}
}

func (mn *MapNode) AdjustWorker(newCount int) {
	mn.mu.Lock()
	oldCount := int(atomic.LoadInt32(&mn.desiredWorkerCount))
	atomic.StoreInt32(&mn.desiredWorkerCount, int32(newCount))
	mn.mu.Unlock()
	if newCount > oldCount {
		for i := oldCount; i < newCount; i++ {
			mn.wg.Add(1)
			go mn.mapWorker(mn.ctx, i)
			activity := WorkerActivity{
				Node:      mn.NodeName,
				WorkerID:  i,
				Processed: 0,
				Failed:    0,
				Timestamp: time.Now(),
				Activity:  "spawned",
			}
			mn.metrics.AddWorkerActivity(activity)
		}
		log.Printf("[MapNode] Increased worker count from %d to %d", oldCount, newCount)
	} else {
		log.Printf("[MapNode] Decreased worker count from %d to %d; extra workers will exit and their progress stored", oldCount, newCount)
	}
}
