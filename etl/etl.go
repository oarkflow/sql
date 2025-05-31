package etl

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/oarkflow/expr"
	"github.com/oarkflow/transaction"
	"github.com/robfig/cron/v3"

	"github.com/oarkflow/sql/pkg/adapters/sqladapter"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/transactions"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/sql/pkg/utils/sqlutil"
)

func init() {
	expr.AddFunction("lookupIn", LookupInGlobal)
}

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
	ValidateSchema        func(rec utils.Record) error
	ValidateBusinessRules func(rec utils.Record) error
}

type Plugin interface {
	Name() string
	Init(e *ETL) error
}

type WorkerActivity struct {
	Node      string    `json:"node"`
	WorkerID  int       `json:"worker_id"`
	Processed int64     `json:"processed"`
	Failed    int64     `json:"failed"`
	Timestamp time.Time `json:"timestamp"`
	Activity  string    `json:"activity"`
}

type Metrics struct {
	Extracted        int64            `json:"extracted"`
	Mapped           int64            `json:"mapped"`
	Transformed      int64            `json:"transformed"`
	Loaded           int64            `json:"loaded"`
	Errors           int64            `json:"errors"`
	WorkerActivities []WorkerActivity `json:"worker_activities"`
	mu               sync.Mutex
}

func (m *Metrics) AddWorkerActivity(activity WorkerActivity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WorkerActivities = append(m.WorkerActivities, activity)
}

func Shutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		signal.Stop(sigChan)
		log.Printf("Received signal: %v. Initiating graceful shutdown...", sig)
		cancel()
	}()
}

type ETL struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	sources         []contracts.Source
	mappers         []contracts.Mapper
	transformers    []contracts.Transformer
	loaders         []contracts.Loader
	lookups         []contracts.LookupLoader
	checkpointStore contracts.CheckpointStore
	circuitBreaker  *transactions.CircuitBreaker
	tableCfg        config.TableMapping
	workerCount     int
	loaderWorkers   int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	rawChanBuffer   int
	checkpointFunc  func(rec utils.Record) string
	lastCheckpoint  *atomic.Value
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
	dashboardUser   string
	dashboardPass   string
	dedupEnabled    bool
	dedupField      string
	Logger          *log.Logger

	CreatedAt time.Time
	LastRunAt time.Time
	Status    string
}

func defaultConfig() *ETL {
	v := new(atomic.Value)
	v.Store("")
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
		Logger:         log.Default(),
		CreatedAt:      time.Now(),
		Status:         "INACTIVE",
		lastCheckpoint: v,
	}
}

func NewETL(id, name string, opts ...Option) *ETL {
	e := defaultConfig()
	e.ID = id
	e.Name = name
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

func (e *ETL) SetTableConfig(tableCfg config.TableMapping) {
	e.tableCfg = tableCfg
}

type Adjustable interface {
	AdjustWorker(newCount int)
}

func (e *ETL) AdjustWorker(newWorkerCount int) {
	e.workerCount = newWorkerCount
	e.loaderWorkers = newWorkerCount / 2
	if e.loaderWorkers == 0 {
		e.loaderWorkers = 1
	}
	log.Printf("[ETL %s] Adjusted worker count to %d and loader workers to %d", e.ID, e.workerCount, e.loaderWorkers)

	e.metrics.AddWorkerActivity(WorkerActivity{
		Node:      "ETL",
		WorkerID:  -1,
		Processed: 0,
		Failed:    0,
		Timestamp: time.Now(),
		Activity:  fmt.Sprintf("Adjusted ETL worker count to %d (loader: %d)", e.workerCount, e.loaderWorkers),
	})
	if e.pipelineConfig != nil {
		for nodeID, node := range e.pipelineConfig.Nodes {
			if adj, ok := node.(Adjustable); ok {
				if nodeID == "load" {
					adj.AdjustWorker(e.loaderWorkers)
				} else {
					adj.AdjustWorker(e.workerCount)
				}
			}
		}
	}
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

func (e *ETL) GetMetrics() Metrics {
	return Metrics{
		Extracted:        atomic.LoadInt64(&e.metrics.Extracted),
		Mapped:           atomic.LoadInt64(&e.metrics.Mapped),
		Transformed:      atomic.LoadInt64(&e.metrics.Transformed),
		Loaded:           atomic.LoadInt64(&e.metrics.Loaded),
		Errors:           atomic.LoadInt64(&e.metrics.Errors),
		WorkerActivities: e.metrics.WorkerActivities,
	}
}

type Summary struct {
	Metrics   Metrics `json:"metrics"`
	ID        string  `json:"ID"`
	Name      string  `json:"name"`
	StartedAt string  `json:"started_at"`
	LastRunAt string  `json:"last_run_at"`
	Status    string  `json:"status"`
	ErrorRate float64 `json:"error_rate"`
}

func (e *ETL) GetSummary() Summary {
	metrics := e.GetMetrics()
	var errorRate float64
	if metrics.Extracted > 0 {
		errorRate = float64(metrics.Errors) / float64(metrics.Extracted) * 100
	}
	return Summary{
		Metrics:   metrics,
		ID:        e.ID,
		Name:      e.Name,
		StartedAt: e.CreatedAt.Format(time.RFC3339),
		LastRunAt: e.LastRunAt.Format(time.RFC3339),
		Status:    e.Status,
		ErrorRate: errorRate,
	}
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

func applyTransformers(ctx context.Context, rec utils.Record, transformers []contracts.Transformer, workerID int, metrics *Metrics) ([]utils.Record, error) {
	records := []utils.Record{rec}
	for _, transformer := range transformers {
		var nextRecords []utils.Record
		if mt, ok := transformer.(contracts.MultiTransformer); ok {
			for _, r := range records {
				recs, err := mt.TransformMany(ctx, r)
				if err != nil {
					log.Printf("[Transformer Worker %d] MultiTransformer error: %v", workerID, err)
					atomic.AddInt64(&metrics.Errors, 1)
					nextRecords = append(nextRecords, r)
					continue
				}
				nextRecords = append(nextRecords, recs...)
			}
		} else {
			for _, r := range records {
				r2, err := transformer.Transform(ctx, r)
				if err != nil {
					log.Printf("[Transformer Worker %d] Transformer error: %v", workerID, err)
					atomic.AddInt64(&metrics.Errors, 1)
					nextRecords = append(nextRecords, r)
					continue
				}
				nextRecords = append(nextRecords, r2)
			}
		}
		records = nextRecords
	}
	return records, nil
}

func (e *ETL) Run(ctx context.Context) error {
	if e.Status == "RUNNING" {
		return fmt.Errorf("ETL job %s is already running", e.ID)
	}
	e.LastRunAt = time.Now()
	e.Status = "RUNNING"
	if e.streamingMode {
		if e.Logger != nil {
			e.Logger.Println("[ETL] Streaming mode is enabled.")
		} else {
			log.Println("[ETL] Streaming mode is enabled.")
		}
	}
	if e.distributedMode {
		if e.Logger != nil {
			e.Logger.Println("[ETL] Distributed mode is enabled.")
		} else {
			log.Println("[ETL] Distributed mode is enabled.")
		}
	}
	overallStart := time.Now()
	if e.pipelineConfig == nil {
		e.pipelineConfig = e.buildDefaultPipeline()
	}
	err := e.runPipeline(ctx, e.pipelineConfig)
	if err != nil {
		e.Status = "FAILED"
		if e.Logger != nil {
			e.Logger.Printf("[ETL] Pipeline error: %v", err)
		} else {
			log.Printf("[ETL] Pipeline error: %v", err)
		}
		return err
	}
	if atomic.LoadInt64(&e.metrics.Errors) >= int64(e.maxErrorCount) {
		e.Status = "FAILED"
		err = fmt.Errorf("maximum error threshold exceeded (%d errors)", e.metrics.Errors)
		if e.Logger != nil {
			e.Logger.Println("[ETL]", err)
		} else {
			log.Println("[ETL]", err)
		}
		return err
	}
	elapsed := time.Since(overallStart)
	if e.Logger != nil {
		e.Logger.Printf("[ETL] Total pipeline execution time: %v", elapsed)
		e.Logger.Printf("[ETL] Summary: %+v", e.GetSummary())
	} else {
		log.Printf("[ETL] Total pipeline execution time: %v", elapsed)
		log.Printf("[ETL] Summary: %+v", e.GetSummary())
	}
	if e.eventBus != nil {
		summary := e.GetSummary()
		e.eventBus.Publish("Summary", summary)
	}
	e.Status = "COMPLETED"
	return nil
}

// GetMetadata returns key ETL metadata in a structured format.
func (e *ETL) GetMetadata() map[string]any {
	metadata := make(map[string]any)
	metadata["ID"] = e.ID
	metadata["Name"] = e.Name
	metadata["CreatedAt"] = e.CreatedAt.Format(time.RFC3339)
	metadata["LastRunAt"] = e.LastRunAt.Format(time.RFC3339)
	metadata["Status"] = e.Status
	metrics := e.GetMetrics()
	metadata["Extracted"] = metrics.Extracted
	metadata["Mapped"] = metrics.Mapped
	metadata["Transformed"] = metrics.Transformed
	metadata["Loaded"] = metrics.Loaded
	metadata["ErrorRate"] = func() float64 {
		if metrics.Extracted > 0 {
			return float64(metrics.Errors) / float64(metrics.Extracted) * 100
		}
		return 0
	}()
	// You can also include additional metadata (e.g. lookup catalog, adapter details, etc.)
	return metadata
}

// ScheduleRun starts a cron scheduler that runs the ETL job per the schedule in the pipeline config.
// If the schedule field is empty, it immediately runs the job.
func (e *ETL) ScheduleRun(ctx context.Context) error {
	if e.pipelineConfig == nil || e.pipelineConfig.Schedule == "" {
		// No scheduling set; run immediately.
		return e.Run(ctx)
	}
	c := cron.New()
	_, err := c.AddFunc(e.pipelineConfig.Schedule, func() {
		log.Printf("[ETL %s] Scheduled run starting...", e.ID)
		if err := e.Run(context.Background()); err != nil {
			log.Printf("[ETL %s] Scheduled run error: %v", e.ID, err)
		} else {
			log.Printf("[ETL %s] Scheduled run completed successfully", e.ID)
		}
	})
	if err != nil {
		return fmt.Errorf("unable to add scheduled function: %w", err)
	}
	c.Start()
	// Listen for cancellation and stop the scheduler accordingly.
	go func() {
		<-ctx.Done()
		c.Stop()
		log.Printf("[ETL %s] Scheduler stopped", e.ID)
	}()
	return nil
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

func (e *ETL) buildDefaultPipeline() *PipelineConfig {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5 * 10, // ten times expected entries
		MaxCost:     1e5,      // ~100k entries max
		BufferItems: 64,
	})
	nodes := map[string]contracts.Node{
		"source": &SourceNode{
			sources:       e.sources,
			rawChanBuffer: e.rawChanBuffer,
			hooks:         e.hooks,
			validations:   e.validations,
			eventBus:      e.eventBus,
			metrics:       e.metrics,
			Logger:        e.Logger,
		},
		"normalize": &NormalizeNode{
			schema:      e.normalizeSchema,
			workerCount: e.workerCount,
			metrics:     e.metrics,
			NodeName:    "normalize",
			ctx:         context.Background(),
		},
		"map": &MapNode{
			mappers:     e.mappers,
			workerCount: e.workerCount,
			hooks:       e.hooks,
			eventBus:    e.eventBus,
			metrics:     e.metrics,
			NodeName:    "map",
			ctx:         context.Background(),
		},
		"transform": &TransformNode{
			transformers:    e.transformers,
			workerCount:     e.workerCount,
			hooks:           e.hooks,
			eventBus:        e.eventBus,
			metrics:         e.metrics,
			NodeName:        "transform",
			deadLetterQueue: make(chan utils.Record, e.workerCount*2),
			ctx:             context.Background(),
		},
		"load": &LoaderNode{
			loaders:            e.loaders,
			workerCount:        e.loaderWorkers,
			batchSize:          e.batchSize,
			retryCount:         e.retryCount,
			retryDelay:         e.retryDelay,
			circuitBreaker:     e.circuitBreaker,
			checkpointStore:    e.checkpointStore,
			checkpointFunc:     e.checkpointFunc,
			cpMutex:            &e.cpMutex,
			lastCheckpoint:     e.lastCheckpoint,
			hooks:              e.hooks,
			validations:        e.validations,
			eventBus:           e.eventBus,
			dedupEnabled:       e.dedupEnabled,
			dedupField:         e.dedupField,
			dedupCache:         cache,
			metrics:            e.metrics,
			NodeName:           "load",
			checkpointInterval: 5 * time.Second,
			lastCheckpointTime: time.Now(),
			deadLetterQueueCap: 1000,
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
	Nodes    map[string]contracts.Node
	Edges    []dagEdge
	Schedule string
}

func (e *ETL) runPipeline(ctx context.Context, pc *PipelineConfig) error {
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
		outCh, err := currentNode.pn.Process(ctx, input, e.tableCfg)
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

func (mn *MapNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
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

func (tn *TransformNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
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

func (ln *LoaderNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	done := make(chan utils.Record)
	ln.inChan = in
	ln.batchChan = make(chan []utils.Record, ln.workerCount)
	ln.workerProgress = make(map[int]int64)
	atomic.StoreInt32(&ln.desiredWorkerCount, int32(ln.workerCount))
	go ln.batchRecords(ctx)
	for i := 0; i < ln.workerCount; i++ {
		ln.wg.Add(1)
		go ln.loaderWorker(ctx, i)
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

func (ln *LoaderNode) loaderWorker(ctx context.Context, index int) {
	defer ln.wg.Done()
	var localLoaded int64 = 0
	var localFailed int64 = 0
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
