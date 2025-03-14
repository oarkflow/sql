package etl

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oarkflow/convert"
	"github.com/oarkflow/expr"

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/checkpoints"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/contracts"
	"github.com/oarkflow/etl/pkg/mappers"
	"github.com/oarkflow/etl/pkg/transactions"
	"github.com/oarkflow/etl/pkg/transformers"
	"github.com/oarkflow/etl/pkg/utils"
	"github.com/oarkflow/etl/pkg/utils/sqlutil"
)

type LifecycleHooks struct {
	BeforeExtract   func(ctx context.Context) error
	AfterExtract    func(ctx context.Context, recordCount int) error
	BeforeMapper    func(ctx context.Context, rec utils.Record) error
	AfterMapper     func(ctx context.Context, rec utils.Record) error
	BeforeTransform func(ctx context.Context, rec utils.Record) error
	AfterTransform  func(ctx context.Context, rec utils.Record) error
	BeforeLoad      func(ctx context.Context, batch []utils.Record) error
	AfterLoad       func(ctx context.Context, batch []utils.Record) error
}

type Validations struct {
	ValidateBeforeExtract func(ctx context.Context) error
	ValidateAfterExtract  func(ctx context.Context, recordCount int) error
	ValidateBeforeLoad    func(ctx context.Context, batch []utils.Record) error
	ValidateAfterLoad     func(ctx context.Context, batch []utils.Record) error
}

type Event struct {
	Name    string
	Payload interface{}
}

type EventHandler func(Event)

type EventBus struct {
	handlers map[string][]EventHandler
	mu       sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[string][]EventHandler),
	}
}

func (eb *EventBus) Subscribe(eventName string, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.handlers[eventName] = append(eb.handlers[eventName], handler)
}

func (eb *EventBus) Publish(eventName string, payload interface{}) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if hs, ok := eb.handlers[eventName]; ok {
		for _, h := range hs {

			go h(Event{Name: eventName, Payload: payload})
		}
	}
}

type Plugin interface {
	Name() string
	Init(e *ETL) error
}

type Metrics struct {
	Extracted   int64 `json:"extracted"`
	Mapped      int64 `json:"mapped"`
	Transformed int64 `json:"transformed"`
	Loaded      int64 `json:"loaded"`
	Errors      int64 `json:"errors"`
}

func Shutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Initiating graceful shutdown...", sig)
		cancel()
	}()
}

type ETL struct {
	sources         []contracts.Source
	mappers         []contracts.Mapper
	transformers    []contracts.Transformer
	loaders         []contracts.Loader
	lookups         []contracts.LookupLoader
	checkpointStore contracts.CheckpointStore
	circuitBreaker  *transactions.CircuitBreaker
	workerCount     int
	loaderWorkers   int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	rawChanBuffer   int
	checkpointFunc  func(rec utils.Record) string
	lastCheckpoint  string
	cpMutex         sync.Mutex
	maxErrorCount   int
	errorCount      int
	cancelFunc      context.CancelFunc
	lookupStore     map[string][]utils.Record
	lookupInCache   sync.Map
	pipelineConfig  *PipelineConfig
	normalizeSchema map[string]string

	hooks           *LifecycleHooks
	validations     *Validations
	eventBus        *EventBus
	plugins         []Plugin
	distributedMode bool
	streamingMode   bool
	metrics         *Metrics

	dashboardUser string
	dashboardPass string

	dedupEnabled bool
	dedupField   string
}

func defaultConfig() *ETL {
	return &ETL{
		workerCount:    4,
		batchSize:      100,
		retryCount:     3,
		retryDelay:     100 * time.Millisecond,
		loaderWorkers:  2,
		rawChanBuffer:  100,
		maxErrorCount:  10,
		lookupStore:    make(map[string][]utils.Record),
		circuitBreaker: transactions.NewCircuitBreaker(5, 5*time.Second),
		metrics:        &Metrics{},
	}
}

func NewETL(opts ...Option) *ETL {
	e := defaultConfig()
	for _, opt := range opts {
		if err := opt(e); err != nil {
			log.Printf("Error applying option: %v", err)
		}
	}

	for _, p := range e.plugins {
		if err := p.Init(e); err != nil {
			log.Printf("Error initializing plugin %s: %v", p.Name(), err)
		} else {
			log.Printf("Plugin %s initialized successfully", p.Name())
		}
	}
	return e
}

func Run(cfg *config.Config, options ...Option) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	Shutdown(cancel)
	var destDB *sql.DB
	var err error
	if utils.IsSQLType(cfg.Destination.Type) {
		destDB, err = config.OpenDB(cfg.Destination)
		if err != nil {
			log.Printf("Error connecting to destination DB: %v\n", err)
			return err
		}
		defer func() { _ = destDB.Close() }()
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = 50
	}
	if cfg.WorkerCount == 0 {
		minCPU := runtime.NumCPU()
		if minCPU <= 1 {
			cfg.WorkerCount = 1
		} else {
			cfg.WorkerCount = minCPU - 1
		}
	}
	var sourceFile string
	var sources []contracts.Source
	var sourcesToMigrate []string
	if len(cfg.Sources) == 0 && !utils.IsEmpty(cfg.Source) {
		cfg.Sources = append(cfg.Sources, cfg.Source)
	}
	for _, sourceCfg := range cfg.Sources {
		if sourceCfg.File != "" {
			sourceFile = sourceCfg.File
		}
		var tmp []string
		if sourceCfg.File != "" {
			tmp = append(tmp, sourceCfg.File)
		}
		if sourceCfg.Table != "" {
			tmp = append(tmp, sourceCfg.Table)
		}
		if sourceCfg.Source != "" {
			tmp = append(tmp, sourceCfg.Source)
		}
		if len(tmp) > 0 {
			sourcesToMigrate = append(sourcesToMigrate, strings.Join(tmp, ", "))
		}
		var sourceDB *sql.DB
		if utils.IsSQLType(sourceCfg.Type) {
			sourceDB, err = config.OpenDB(sourceCfg)
			if err != nil {
				log.Printf("Error connecting to source DB: %v\n", err)
				return err
			}
		}
		src, err := NewSource(sourceCfg.Type, sourceDB, sourceCfg.File, sourceCfg.Table, sourceCfg.Source, sourceCfg.Format)
		if err != nil {
			return err
		}
		sources = append(sources, src)
	}
	checkpointFile := cfg.Checkpoint.File
	checkpointField := cfg.Checkpoint.Field
	if checkpointFile == "" {
		checkpointFile = "checkpoints.txt"
	}
	if checkpointField == "" {
		checkpointField = "id"
	}
	for _, tableCfg := range cfg.Tables {
		if utils.IsSQLType(cfg.Destination.Type) && !tableCfg.Migrate {
			continue
		}
		if tableCfg.OldName == "" && sourceFile != "" {
			tableCfg.OldName = sourceFile
		}
		if tableCfg.NewName == "" && cfg.Destination.File != "" {
			tableCfg.NewName = cfg.Destination.File
		}
		if utils.IsSQLType(cfg.Destination.Type) && tableCfg.AutoCreateTable && tableCfg.KeyValueTable {
			if err := sqlutil.CreateKeyValueTable(
				destDB, tableCfg.NewName,
				tableCfg.KeyField, tableCfg.ValueField, tableCfg.TruncateDestination, tableCfg.ExtraValues,
			); err != nil {
				log.Printf("Error creating key-value table %s: %v", tableCfg.NewName, err)
				return err
			}
		}
		opts := []Option{
			WithSources(sources...),
			WithDestination(cfg.Destination, destDB, tableCfg),
			WithCheckpoint(checkpoints.NewFileCheckpointStore(checkpointFile), func(rec utils.Record) string {
				val, ok := rec[checkpointField]
				if !ok {
					return ""
				}
				v, _ := convert.ToString(val)
				return v
			}),
			WithWorkerCount(cfg.WorkerCount),
			WithBatchSize(tableCfg.BatchSize),
			WithRawChanBuffer(cfg.Buffer),
			WithStreamingMode(cfg.StreamingMode),
			WithDistributedMode(cfg.DistributedMode),
		}
		for _, opt := range options {
			opts = append(opts, opt)
		}
		if cfg.Deduplication.Enabled {
			if cfg.Deduplication.Field == "" {
				cfg.Deduplication.Field = "id"
			}
			opts = append(opts, WithDeduplication(cfg.Deduplication.Field))
		}

		if tableCfg.NormalizeSchema != nil {
			opts = append(opts, WithNormalizeSchema(tableCfg.NormalizeSchema))
		}
		var mapperList []contracts.Mapper
		if len(tableCfg.Mapping) > 0 {
			mapperList = append(mapperList, mappers.NewFieldMapper(tableCfg.Mapping))
		}
		mapperList = append(mapperList, &mappers.LowercaseMapper{})
		opts = append(opts, WithMappers(mapperList...))
		if tableCfg.Aggregator != nil {
			aggTransformer := transformers.NewAggregatorTransformer(
				tableCfg.Aggregator.GroupBy,
				tableCfg.Aggregator.Aggregations,
			)
			opts = append(opts, WithTransformers(aggTransformer))
		}
		if tableCfg.KeyValueTable {
			opts = append(opts, WithKeyValueTransformer(
				tableCfg.ExtraValues,
				tableCfg.IncludeFields,
				tableCfg.ExcludeFields,
				tableCfg.KeyField,
				tableCfg.ValueField,
			))
		}
		etlJob := NewETL(opts...)
		go etlJob.StartDashboard(":8080")
		var lookups []contracts.LookupLoader
		if len(cfg.Lookups) > 0 {
			for _, lkup := range cfg.Lookups {
				lookup, err := adapters.NewLookupLoader(lkup)
				if err != nil {
					log.Printf("Unsupported lookup type: %s", lkup.Type)
					return err
				}
				err = lookup.Setup(ctx)
				if err != nil {
					log.Printf("Unable to setup lookup: %s", lkup.Type)
					return err
				}
				data, err := lookup.LoadData()
				if err != nil {
					log.Printf("Failed to load lookup data for %s: %v", lkup.Key, err)
					return err
				}
				lookups = append(lookups, lookup)
				etlJob.lookupStore[lkup.Key] = data
			}
		}
		etlJob.lookups = append(etlJob.lookups, lookups...)
		expr.AddFunction("lookupIn", etlJob.lookupIn)
		for _, loader := range etlJob.loaders {
			err = loader.Setup(ctx)
			if err != nil {
				log.Printf("Setting up loader failed: %v", err)
				return err
			}
		}
		log.Printf("Starting migration: %s -> %s", tableCfg.OldName, tableCfg.NewName)
		if err := etlJob.Run(ctx, tableCfg); err != nil {
			log.Printf("ETL DAG job failed: %v", err)
			return err
		}
		if err := etlJob.Close(); err != nil {
			log.Printf("Error closing ETL job: %v", err)
			return err
		}
		var dst string
		if tableCfg.NewName != "" {
			dst = tableCfg.NewName
		} else if cfg.Destination.File != "" {
			dst = cfg.Destination.File
		} else if cfg.Destination.Table != "" {
			dst = cfg.Destination.Table
		} else if cfg.Destination.Source != "" {
			dst = cfg.Destination.Source
		}
		log.Printf("Migration for %s to %s completed", "["+strings.Join(sourcesToMigrate, ", ")+"]", dst)
	}
	log.Println("All migrations complete.")
	return nil
}

func (e *ETL) Close() error {
	for _, src := range e.sources {
		if err := src.Close(); err != nil {
			return fmt.Errorf("error closing source: %v", err)
		}
	}
	for _, src := range e.lookups {
		if err := src.Close(); err != nil {
			return fmt.Errorf("error closing lookups: %v", err)
		}
	}
	for _, loader := range e.loaders {
		if err := loader.Close(); err != nil {
			return fmt.Errorf("error closing loader: %v", err)
		}
	}
	return nil
}

func applyMappers(ctx context.Context, rec utils.Record, mappers []contracts.Mapper, workerID int) (utils.Record, error) {
	for _, mapper := range mappers {
		var err error
		rec, err = mapper.Map(ctx, rec)
		if err != nil {
			log.Printf("[Mapper Worker %d] Mapper (%s) error: %v", workerID, mapper.Name(), err)
			return nil, err
		}
	}
	return rec, nil
}

func (e *ETL) Run(ctx context.Context, tableCfg config.TableMapping) error {
	if e.streamingMode {
		log.Println("[ETL] Streaming mode is enabled.")
	}
	if e.distributedMode {
		log.Println("[ETL] Distributed mode is enabled.")
	}
	overallStart := time.Now()
	if e.pipelineConfig == nil {
		e.pipelineConfig = e.buildDefaultPipeline()
	}
	err := e.runPipeline(ctx, e.pipelineConfig, tableCfg)
	elapsed := time.Since(overallStart)
	log.Printf("[ETL] Total pipeline execution time: %v", elapsed)
	return err
}

func applyTransformers(ctx context.Context, rec utils.Record, transformers []contracts.Transformer, workerID int) ([]utils.Record, error) {
	records := []utils.Record{rec}
	for _, transformer := range transformers {
		var nextRecords []utils.Record
		if mt, ok := transformer.(contracts.MultiTransformer); ok {
			for _, r := range records {
				recs, err := mt.TransformMany(ctx, r)
				if err != nil {
					log.Printf("[Transformer Worker %d] MultiTransformer error: %v", workerID, err)
					continue
				}
				nextRecords = append(nextRecords, recs...)
			}
		} else {
			for _, r := range records {
				r2, err := transformer.Transform(ctx, r)
				if err != nil {
					log.Printf("[Transformer Worker %d] Transformer error: %v", workerID, err)
					continue
				}
				nextRecords = append(nextRecords, r2)
			}
		}
		records = nextRecords
	}
	return records, nil
}

func (e *ETL) StartDashboard(addr string) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != e.dashboardUser || pass != e.dashboardPass {
			w.Header().Set("WWW-Authenticate", `Basic realm="ETL Dashboard"`)
			http.Error(w, "Unauthorized.", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		data, _ := json.MarshalIndent(e.metrics, "", "  ")
		w.Write(data)
	})
	log.Printf("Starting dashboard on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Dashboard server error: %v", err)
	}
}

type SourceNode struct {
	sources       []contracts.Source
	rawChanBuffer int
	hooks         *LifecycleHooks
	validations   *Validations
	eventBus      *EventBus
	metrics       *Metrics
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
			startTime := time.Now()
			recordCount := 0

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
			for rec := range ch {
				out <- rec
				recordCount++
				atomic.AddInt64(&sn.metrics.Extracted, 1)
			}
			if sn.eventBus != nil {
				sn.eventBus.Publish("AfterExtract", recordCount)
			}
			if sn.hooks != nil && sn.hooks.AfterExtract != nil {
				if err := sn.hooks.AfterExtract(ctx, recordCount); err != nil {
					log.Printf("[SourceNode] AfterExtract hook error: %v", err)
				}
			}
			if sn.validations != nil && sn.validations.ValidateAfterExtract != nil {
				if err := sn.validations.ValidateAfterExtract(ctx, recordCount); err != nil {
					log.Printf("[SourceNode] ValidateAfterExtract error: %v", err)
				}
			}
			elapsed := time.Since(startTime)
			log.Printf("[Source] %T extracted %d records in %v", source, recordCount, elapsed)
		}(src)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

type NormalizeNode struct {
	schema      map[string]string
	workerCount int
}

func (nn *NormalizeNode) Process(_ context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	out := make(chan utils.Record, nn.workerCount*2)
	var wg sync.WaitGroup
	var totalNormalized int64
	startTime := time.Now()
	for i := 0; i < nn.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localCount := 0
			for rec := range in {
				if nn.schema == nil {
					out <- rec
					localCount++
					continue
				}
				nRec, err := utils.NormalizeRecord(rec, nn.schema)
				if err != nil {
					log.Printf("[Normalize Worker %d] Error: %v", workerID, err)
					continue
				}
				out <- nRec
				localCount++
			}
			atomic.AddInt64(&totalNormalized, int64(localCount))
			log.Printf("[Normalize Worker %d] processed %d records", workerID, localCount)
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
		elapsed := time.Since(startTime)
		log.Printf("[Normalize] Total processed records: %d in %v", totalNormalized, elapsed)
	}()
	return out, nil
}

type MapNode struct {
	mappers     []contracts.Mapper
	workerCount int
	hooks       *LifecycleHooks
	eventBus    *EventBus
	metrics     *Metrics
}

func (mn *MapNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	out := make(chan utils.Record, mn.workerCount*2)
	var wg sync.WaitGroup
	var totalMapped int64
	startTime := time.Now()
	for i := 0; i < mn.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localCount := 0
			for rec := range in {
				if mn.eventBus != nil {
					mn.eventBus.Publish("BeforeMapper", rec)
				}
				if mn.hooks != nil && mn.hooks.BeforeMapper != nil {
					if err := mn.hooks.BeforeMapper(ctx, rec); err != nil {
						log.Printf("[MapNode Worker %d] BeforeMapper hook error: %v", workerID, err)
					}
				}
				mapped, err := applyMappers(ctx, rec, mn.mappers, workerID)
				if err != nil {
					log.Printf("[MapNode Worker %d] Error: %v", workerID, err)
					atomic.AddInt64(&mn.metrics.Errors, 1)
					continue
				}
				if mn.eventBus != nil {
					mn.eventBus.Publish("AfterMapper", mapped)
				}
				if mn.hooks != nil && mn.hooks.AfterMapper != nil {
					if err := mn.hooks.AfterMapper(ctx, mapped); err != nil {
						log.Printf("[MapNode Worker %d] AfterMapper hook error: %v", workerID, err)
					}
				}
				if mapped != nil {
					out <- mapped
					localCount++
					atomic.AddInt64(&mn.metrics.Mapped, 1)
				}
			}
			atomic.AddInt64(&mn.metrics.Mapped, int64(localCount))
			log.Printf("[MapNode Worker %d] mapped %d records", workerID, localCount)
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
		elapsed := time.Since(startTime)
		log.Printf("[MapNode] Total mapped records: %d in %v", totalMapped, elapsed)
	}()
	return out, nil
}

type TransformNode struct {
	transformers []contracts.Transformer
	workerCount  int
	hooks        *LifecycleHooks
	eventBus     *EventBus
	metrics      *Metrics
}

func (tn *TransformNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	out := make(chan utils.Record, tn.workerCount*2)
	var wg sync.WaitGroup
	var totalTransformed int64
	startTime := time.Now()
	for i := 0; i < tn.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localCount := 0
			for rec := range in {
				if tn.eventBus != nil {
					tn.eventBus.Publish("BeforeTransform", rec)
				}
				if tn.hooks != nil && tn.hooks.BeforeTransform != nil {
					if err := tn.hooks.BeforeTransform(ctx, rec); err != nil {
						log.Printf("[TransformNode Worker %d] BeforeTransform hook error: %v", workerID, err)
					}
				}
				transformed, err := applyTransformers(ctx, rec, tn.transformers, workerID)
				if err != nil {
					log.Printf("[TransformNode Worker %d] Error: %v", workerID, err)
					atomic.AddInt64(&tn.metrics.Errors, 1)
					continue
				}
				for _, r := range transformed {
					if tn.eventBus != nil {
						tn.eventBus.Publish("AfterTransform", r)
					}
					if tn.hooks != nil && tn.hooks.AfterTransform != nil {
						if err := tn.hooks.AfterTransform(ctx, r); err != nil {
							log.Printf("[TransformNode Worker %d] AfterTransform hook error: %v", workerID, err)
						}
					}
					if r != nil {
						out <- r
						localCount++
						atomic.AddInt64(&tn.metrics.Transformed, 1)
					}
				}
			}
			atomic.AddInt64(&tn.metrics.Transformed, int64(localCount))
			log.Printf("[TransformNode Worker %d] transformed %d records", workerID, localCount)
		}(i)
	}
	go func() {
		wg.Wait()

		for _, t := range tn.transformers {
			if flushable, ok := t.(contracts.Flushable); ok {
				flushRecords, err := flushable.Flush(ctx)
				if err != nil {
					log.Printf("[TransformNode] Flush error: %v", err)
					continue
				}
				for _, r := range flushRecords {
					out <- r
				}
			}
		}
		close(out)
		elapsed := time.Since(startTime)
		log.Printf("[TransformNode] Total transformed records: %d in %v", totalTransformed, elapsed)
	}()
	return out, nil
}

type LoaderNode struct {
	loaders         []contracts.Loader
	workerCount     int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	circuitBreaker  *transactions.CircuitBreaker
	checkpointStore contracts.CheckpointStore
	checkpointFunc  func(rec utils.Record) string
	cpMutex         *sync.Mutex
	lastCheckpoint  string
	hooks           *LifecycleHooks
	validations     *Validations
	eventBus        *EventBus

	dedupEnabled bool
	dedupField   string
	dedupCache   map[string]struct{}
	dedupLock    sync.Mutex

	deadLetterQueue []utils.Record

	metrics *Metrics
}

func (ln *LoaderNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	done := make(chan utils.Record)
	batchChan := make(chan []utils.Record, ln.workerCount)
	startTime := time.Now()

	go func() {
		batch := make([]utils.Record, 0, ln.batchSize)
		for rec := range in {
			batch = append(batch, rec)
			if len(batch) >= ln.batchSize {
				batchChan <- batch
				batch = make([]utils.Record, 0, ln.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()

	if ln.dedupEnabled && ln.dedupCache == nil {
		ln.dedupCache = make(map[string]struct{})
	}

	var wg sync.WaitGroup
	for i := 0; i < ln.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLoaded := 0
			batchID := 1
			for batch := range batchChan {
				if ln.dedupEnabled {
					uniqueBatch := make([]utils.Record, 0, len(batch))
					for _, rec := range batch {
						var key string
						if ln.dedupField != "" {
							if val, ok := rec[ln.dedupField]; ok {
								key = fmt.Sprintf("%v", val)
							} else {
								data, _ := json.Marshal(rec)
								key = string(data)
							}
						} else {
							data, _ := json.Marshal(rec)
							hash := sha256.Sum256(data)
							key = hex.EncodeToString(hash[:])
						}
						ln.dedupLock.Lock()
						if _, exists := ln.dedupCache[key]; !exists {
							ln.dedupCache[key] = struct{}{}
							uniqueBatch = append(uniqueBatch, rec)
						}
						ln.dedupLock.Unlock()
					}
					batch = uniqueBatch
				}

				if ln.eventBus != nil {
					ln.eventBus.Publish("BeforeLoad", batch)
				}
				if ln.validations != nil && ln.validations.ValidateBeforeLoad != nil {
					if err := ln.validations.ValidateBeforeLoad(ctx, batch); err != nil {
						log.Printf("[Loader Worker %d] ValidateBeforeLoad error: %v", workerID, err)
						continue
					}
				}
				if ln.hooks != nil && ln.hooks.BeforeLoad != nil {
					if err := ln.hooks.BeforeLoad(ctx, batch); err != nil {
						log.Printf("[Loader Worker %d] BeforeLoad hook error: %v", workerID, err)
						continue
					}
				}
				batchCtx := context.WithValue(ctx, "batch", batchID)
				batchID++
				storeCtx := batchCtx
				if batchCtx.Err() != nil {
					storeCtx = context.Background()
				}
				for _, loader := range ln.loaders {
					if txnLoader, ok := loader.(contracts.Transactional); ok {
						if err := txnLoader.Begin(storeCtx); err != nil {
							log.Printf("[Loader Worker %d] Begin transaction error: %v", workerID, err)
							continue
						}
						if sqlLoader, ok := loader.(*adapters.SQLAdapter); ok && sqlLoader.AutoCreate && !sqlLoader.Created {
							if err := sqlutil.CreateTableFromRecord(sqlLoader.Db, sqlLoader.Driver, sqlLoader.Table, sqlLoader.NormalizeSchema); err != nil {
								log.Printf("[Loader Worker %d] Table creation error: %v", workerID, err)
								continue
							}
							sqlLoader.Created = true
						}
						err := transactions.RetryWithCircuit(ln.retryCount, ln.retryDelay, ln.circuitBreaker, func() error {
							return loader.StoreBatch(storeCtx, batch)
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Batch load error (transaction): %v", workerID, err)
							ln.deadLetterQueue = append(ln.deadLetterQueue, batch...)
							continue
						}
						if err := txnLoader.Commit(storeCtx); err != nil {
							log.Printf("[Loader Worker %d] Commit error: %v", workerID, err)
							ln.deadLetterQueue = append(ln.deadLetterQueue, batch...)
							continue
						}
					} else {
						err := transactions.RunInTransaction(storeCtx, func(tx *transactions.Transaction) error {
							return transactions.RetryWithCircuit(ln.retryCount, ln.retryDelay, ln.circuitBreaker, func() error {
								return loader.StoreBatch(storeCtx, batch)
							})
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Batch load error: %v", workerID, err)
							ln.deadLetterQueue = append(ln.deadLetterQueue, batch...)
							continue
						}
					}
					localLoaded += len(batch)
					if ln.checkpointStore != nil && ln.checkpointFunc != nil {
						cp := ln.checkpointFunc(batch[len(batch)-1])
						ln.cpMutex.Lock()
						if cp > ln.lastCheckpoint {
							if err := ln.checkpointStore.SaveCheckpoint(context.Background(), cp); err != nil {
								log.Printf("[Loader Worker %d] Checkpoint error: %v", workerID, err)
							} else {
								ln.lastCheckpoint = cp
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
						log.Printf("[Loader Worker %d] AfterLoad hook error: %v", workerID, err)
					}
				}
				if ln.validations != nil && ln.validations.ValidateAfterLoad != nil {
					if err := ln.validations.ValidateAfterLoad(ctx, batch); err != nil {
						log.Printf("[Loader Worker %d] ValidateAfterLoad error: %v", workerID, err)
					}
				}
			}
			atomic.AddInt64(&ln.metrics.Loaded, int64(localLoaded))
			log.Printf("[Loader Worker %d] loaded %d records", workerID, localLoaded)
		}(i)
	}
	go func() {
		wg.Wait()
		close(done)
		elapsed := time.Since(startTime)
		log.Printf("[Loader] Total loaded records: %d in %v", ln.metrics.Loaded, elapsed)
	}()
	return done, nil
}

type dagNode struct {
	id       string
	pn       contracts.Node
	inChs    []<-chan utils.Record
	outCh    <-chan utils.Record
	indegree int
}

type dagEdge struct {
	Source string
	Target string
}

type PipelineConfig struct {
	Nodes map[string]contracts.Node
	Edges []dagEdge
}

func (e *ETL) runPipeline(ctx context.Context, pc *PipelineConfig, tableCfg config.TableMapping) error {
	nodes := make(map[string]*dagNode)
	for id, node := range pc.Nodes {
		nodes[id] = &dagNode{
			id: id,
			pn: node,
		}
	}
	for _, edge := range pc.Edges {
		if n, ok := nodes[edge.Target]; ok {
			n.indegree++
		}
	}
	var queue []string
	for id, node := range nodes {
		if node.indegree == 0 {
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]
		currentNode := nodes[currentID]
		var input <-chan utils.Record
		if len(currentNode.inChs) == 0 {
			input = nil
		} else if len(currentNode.inChs) == 1 {
			input = currentNode.inChs[0]
		} else {
			input = mergeChannels(currentNode.inChs)
		}
		outCh, err := currentNode.pn.Process(ctx, input, tableCfg)
		if err != nil {
			return fmt.Errorf("error running node %s: %v", currentID, err)
		}
		currentNode.outCh = outCh
		for _, edge := range pc.Edges {
			if edge.Source == currentID {
				targetNode := nodes[edge.Target]
				targetNode.inChs = append(targetNode.inChs, currentNode.outCh)
				targetNode.indegree--
				if targetNode.indegree == 0 {
					queue = append(queue, targetNode.id)
				}
			}
		}
	}
	if loadNode, ok := nodes["load"]; ok {
		for range loadNode.outCh {
		}
	}
	return nil
}

func mergeChannels(channels []<-chan utils.Record) <-chan utils.Record {
	var wg sync.WaitGroup
	out := make(chan utils.Record)
	output := func(c <-chan utils.Record) {
		for rec := range c {
			out <- rec
		}
		wg.Done()
	}
	wg.Add(len(channels))
	for _, c := range channels {
		go output(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (e *ETL) buildDefaultPipeline() *PipelineConfig {
	nodes := map[string]contracts.Node{
		"source": &SourceNode{
			sources:       e.sources,
			rawChanBuffer: e.rawChanBuffer,
			hooks:         e.hooks,
			validations:   e.validations,
			eventBus:      e.eventBus,
			metrics:       e.metrics,
		},
		"normalize": &NormalizeNode{
			schema:      e.normalizeSchema,
			workerCount: e.workerCount,
		},
		"map": &MapNode{
			mappers:     e.mappers,
			workerCount: e.workerCount,
			hooks:       e.hooks,
			eventBus:    e.eventBus,
			metrics:     e.metrics,
		},
		"transform": &TransformNode{
			transformers: e.transformers,
			workerCount:  e.workerCount,
			hooks:        e.hooks,
			eventBus:     e.eventBus,
			metrics:      e.metrics,
		},
		"load": &LoaderNode{
			loaders:         e.loaders,
			workerCount:     e.loaderWorkers,
			batchSize:       e.batchSize,
			retryCount:      e.retryCount,
			retryDelay:      e.retryDelay,
			circuitBreaker:  e.circuitBreaker,
			checkpointStore: e.checkpointStore,
			checkpointFunc:  e.checkpointFunc,
			cpMutex:         &e.cpMutex,
			lastCheckpoint:  e.lastCheckpoint,
			hooks:           e.hooks,
			validations:     e.validations,
			eventBus:        e.eventBus,
			dedupEnabled:    e.dedupEnabled,
			dedupField:      e.dedupField,
			metrics:         e.metrics,
		},
	}
	edges := []dagEdge{
		{Source: "source", Target: "normalize"},
		{Source: "normalize", Target: "map"},
		{Source: "map", Target: "transform"},
		{Source: "transform", Target: "load"},
	}
	return &PipelineConfig{
		Nodes: nodes,
		Edges: edges,
	}
}

func (e *ETL) lookupIn(args ...any) (any, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("lookupIn requires exactly 4 arguments")
	}
	datasetKey, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("lookupIn: first argument must be string (lookup dataset key)")
	}
	lookupField, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("lookupIn: second argument must be string (lookup field name)")
	}
	sourceValStr := fmt.Sprintf("%v", args[2])
	targetField, ok := args[3].(string)
	if !ok {
		return nil, fmt.Errorf("lookupIn: fourth argument must be string (target field name)")
	}
	cacheKey := datasetKey + ":" + lookupField + ":" + sourceValStr + ":" + targetField
	if cached, found := e.lookupInCache.Load(cacheKey); found {
		return cached, nil
	}
	dataset, exists := e.lookupStore[datasetKey]
	if !exists {
		return nil, fmt.Errorf("lookupIn: no lookup dataset found for key %s", datasetKey)
	}
	for _, row := range dataset {
		if fmt.Sprintf("%v", row[lookupField]) == sourceValStr {
			result := row[targetField]
			e.lookupInCache.Store(cacheKey, result)
			return result, nil
		}
	}
	return nil, fmt.Errorf("lookupIn: no matching value for %s in dataset %s", sourceValStr, datasetKey)
}

func (e *ETL) StreamingMode() bool {
	return e.streamingMode
}

func (e *ETL) DistributedMode() bool {
	return e.distributedMode
}

func (e *ETL) EventBus() *EventBus {
	return e.eventBus
}
