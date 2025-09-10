package etl

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type SourceNode struct {
	sources       []contracts.Source
	rawChanBuffer int
	hooks         *LifecycleHooks
	validations   *Validations
	eventBus      *EventBus
	metrics       *Metrics
	Logger        *log.Logger
}

func (sn *SourceNode) Process(ctx context.Context, _ <-chan utils.Record, tableCfg config.TableMapping) (<-chan utils.Record, error) {
	out := make(chan utils.Record, sn.rawChanBuffer)
	var wg sync.WaitGroup
	for _, src := range sn.sources {
		if err := src.Setup(ctx); err != nil {
			return nil, fmt.Errorf("source setup error: %v", err)
		}
		wg.Add(1)
		go func(source contracts.Source) {
			defer wg.Done()
			if sn.eventBus != nil {
				sn.eventBus.Publish("BeforeExtract", nil)
			}
			if sn.validations != nil && sn.validations.ValidateBeforeExtract != nil {
				if err := sn.validations.ValidateBeforeExtract(ctx); err != nil {
					log.Printf("[SourceNode] ValidateBeforeExtract error: %v", err)
					return
				}
			}
			if sn.hooks != nil && sn.hooks.BeforeExtract != nil {
				if err := sn.hooks.BeforeExtract(ctx); err != nil {
					log.Printf("[SourceNode] BeforeExtract hook error: %v", err)
					return
				}
			}
			var opts []contracts.Option
			if tableCfg.OldName != "" {
				opts = append(opts, contracts.WithTable(tableCfg.OldName))
			} else if tableCfg.Query != "" {
				opts = append(opts, contracts.WithQuery(tableCfg.Query))
			}
			ch, err := source.Extract(ctx, opts...)
			if err != nil {
				log.Printf("Source extraction error: %v", err)
				return
			}
			count := 0
			for rec := range ch {
				select {
				case <-ctx.Done():
					return
				default:
					out <- rec
					atomic.AddInt64(&sn.metrics.Extracted, 1)
					count++
				}
			}
			if sn.eventBus != nil {
				sn.eventBus.Publish("AfterExtract", count)
			}
			if sn.hooks != nil && sn.hooks.AfterExtract != nil {
				if err := sn.hooks.AfterExtract(ctx, count); err != nil {
					log.Printf("[SourceNode] AfterExtract hook error: %v", err)
				}
			}
			if sn.validations != nil && sn.validations.ValidateAfterExtract != nil {
				if err := sn.validations.ValidateAfterExtract(ctx, count); err != nil {
					log.Printf("[SourceNode] ValidateAfterExtract error: %v", err)
				}
			}
			if sn.Logger != nil {
				sn.Logger.Printf("[Source] %T extracted %d records", source, count)
			} else {
				log.Printf("[Source] %T extracted %d records", source, count)
			}
		}(src)
	}
	go func() {
		wg.Wait()
		select {
		case <-ctx.Done():
		default:
			close(out)
		}
	}()
	return out, nil
}
