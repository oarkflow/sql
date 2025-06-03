package etl

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

type NormalizeNode struct {
	schema             map[string]string
	workerCount        int
	desiredWorkerCount int32
	inChan             <-chan utils.Record
	outChan            chan utils.Record
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	workerProgress     map[int]int64
	metrics            *Metrics
	NodeName           string
	activeWorkers      int32
	ctx                context.Context
}

func (nn *NormalizeNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	nn.ctx = ctx
	nn.inChan = in
	nn.outChan = make(chan utils.Record, nn.workerCount*2)
	nn.workerProgress = make(map[int]int64)
	atomic.StoreInt32(&nn.desiredWorkerCount, int32(nn.workerCount))
	for i := 0; i < nn.workerCount; i++ {
		nn.wg.Add(1)
		go nn.normalizeWorker(ctx, i)
	}
	go func() {
		nn.wg.Wait()
		close(nn.outChan)
	}()
	return nn.outChan, nil
}

func (nn *NormalizeNode) normalizeWorker(ctx context.Context, index int) {
	defer nn.wg.Done()
	var localCount int64 = 0
	var localFailed int64 = 0
	for {
		if index >= int(atomic.LoadInt32(&nn.desiredWorkerCount)) {
			nn.mu.Lock()
			nn.workerProgress[index] = localCount
			nn.mu.Unlock()
			activity := WorkerActivity{
				Node:      nn.NodeName,
				WorkerID:  index,
				Processed: localCount,
				Failed:    localFailed,
				Timestamp: time.Now(),
				Activity:  "exited",
			}
			nn.metrics.AddWorkerActivity(activity)
			log.Printf("[NormalizeNode Worker %d] exiting due to reduced worker count; processed %d records", index, localCount)
			return
		}
		select {
		case rec, ok := <-nn.inChan:
			if !ok {
				nn.mu.Lock()
				nn.workerProgress[index] = localCount
				nn.mu.Unlock()
				return
			}
			var nRec utils.Record
			var err error
			if nn.schema == nil {
				nRec = rec
			} else {
				nRec, err = utils.NormalizeRecord(rec, nn.schema)
				if err != nil {
					log.Printf("[NormalizeNode Worker %d] Error: %v", index, err)
					localFailed++
					continue
				}
			}
			nn.outChan <- nRec
			localCount++
		case <-ctx.Done():
			nn.mu.Lock()
			nn.workerProgress[index] = localCount
			nn.mu.Unlock()
			return
		}
	}
}

func (nn *NormalizeNode) AdjustWorker(newCount int) {
	oldCount := atomic.SwapInt32(&nn.desiredWorkerCount, int32(newCount))
	if newCount > int(oldCount) {
		for i := int(oldCount); i < newCount; i++ {
			nn.wg.Add(1)
			go nn.normalizeWorker(nn.ctx, i)
			activity := WorkerActivity{
				Node:      nn.NodeName,
				WorkerID:  i,
				Processed: 0,
				Failed:    0,
				Timestamp: time.Now(),
				Activity:  "spawned",
			}
			nn.metrics.AddWorkerActivity(activity)
		}
		log.Printf("[NormalizeNode] Increased worker count from %d to %d", oldCount, newCount)
	} else {
		log.Printf("[NormalizeNode] Decreased worker count from %d to %d; extra workers will exit", oldCount, newCount)
	}
}
