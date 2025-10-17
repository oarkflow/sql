package etl

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/oarkflow/expr"
	"github.com/robfig/cron/v3"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/transactions"
	"github.com/oarkflow/sql/pkg/utils"
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
	ID                 string `json:"id"`
	Name               string `json:"name"`
	sources            []contracts.Source
	mappers            []contracts.Mapper
	transformers       []contracts.Transformer
	loaders            []contracts.Loader
	lookups            []contracts.LookupLoader
	checkpointStore    contracts.CheckpointStore
	checkpointFile     string // NEW field to hold the checkpoint file path
	circuitBreaker     *transactions.CircuitBreaker
	tableCfg           config.TableMapping
	workerCount        int
	loaderWorkers      int
	batchSize          int
	retryCount         int
	retryDelay         time.Duration
	rawChanBuffer      int
	checkpointFunc     func(rec utils.Record) string
	lastCheckpoint     *atomic.Value
	cpMutex            sync.Mutex
	maxErrorCount      int
	errorCount         int
	cancelFunc         context.CancelFunc
	checkpointInterval time.Duration
	lookupStore        map[string][]utils.Record
	lookupInCache      sync.Map
	pipelineConfig     *PipelineConfig
	normalizeSchema    map[string]string
	hooks              *LifecycleHooks
	validations        *Validations
	eventBus           *EventBus
	plugins            []Plugin
	metrics            *Metrics
	dashboardUser      string
	dashboardPass      string
	dedupEnabled       bool
	dedupField         string
	Logger             *log.Logger
	CreatedAt          time.Time
	LastRunAt          time.Time
	Status             string

	// Enhanced incremental ETL components
	stateManager    *StateManager
	deadLetterQueue *DeadLetterQueue
	idempotencyMgr  *IdempotencyManager
	stateFile       string
	dlqFile         string
	idempotencyFile string
}

func defaultConfig() *ETL {
	v := new(atomic.Value)
	v.Store("")

	// Generate default file names based on timestamp
	timestamp := time.Now().Format("20060102_150405")
	etlID := fmt.Sprintf("etl_%s", timestamp)

	return &ETL{
		workerCount:        4,
		batchSize:          100,
		retryCount:         3,
		retryDelay:         100 * time.Millisecond,
		loaderWorkers:      2,
		rawChanBuffer:      100,
		maxErrorCount:      10,
		checkpointInterval: 5 * time.Second,
		lookupStore:        make(map[string][]utils.Record),
		circuitBreaker:     transactions.NewCircuitBreaker(5, 5*time.Second),
		metrics:            &Metrics{},
		Logger:             log.Default(),
		CreatedAt:          time.Now(),
		Status:             "INACTIVE",
		lastCheckpoint:     v,
		stateFile:          fmt.Sprintf("etl_state_%s.json", etlID),
		dlqFile:            fmt.Sprintf("etl_dlq_%s.json", etlID),
		idempotencyFile:    fmt.Sprintf("etl_idempotency_%s.json", etlID),
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

	// Initialize enhanced components
	e.initializeEnhancedComponents()

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

// initializeEnhancedComponents initializes the enhanced ETL components
func (e *ETL) initializeEnhancedComponents() {
	// Initialize state manager
	if e.stateManager == nil {
		e.stateManager = NewStateManager(e.ID, e.stateFile)
	}

	// Initialize dead letter queue
	if e.deadLetterQueue == nil {
		e.deadLetterQueue = NewDeadLetterQueue(
			e.dlqFile,
			10000,          // max size
			3,              // max retries
			time.Minute,    // base delay
			30*time.Minute, // max delay
		)
		e.deadLetterQueue.SetStateManager(e.stateManager)
	}

	// Initialize idempotency manager
	if e.idempotencyMgr == nil {
		e.idempotencyMgr = NewIdempotencyManager(
			e.idempotencyFile,
			24*time.Hour, // TTL
			100000,       // max keys
		)
	}

	// Start auto-save for state manager
	e.stateManager.StartAutoSave()

	// Start retry processing for dead letter queue
	if e.deadLetterQueue != nil {
		go e.deadLetterQueue.ProcessRetries(context.Background(), e.processDeadLetterRecord)
	}
}

// processDeadLetterRecord processes a record from the dead letter queue
func (e *ETL) processDeadLetterRecord(record DeadLetterRecord) error {
	// This would contain the logic to reprocess the failed record
	// For now, we'll just log it
	log.Printf("[ETL %s] Reprocessing dead letter record: %s", e.ID, record.ID)
	return nil
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

func (e *ETL) Run(ctx context.Context, args ...any) error {
	if e.Status == "RUNNING" {
		return fmt.Errorf("ETL job %s is already running", e.ID)
	}

	// Check if we can resume from a previous state
	resumeInfo := e.stateManager.GetResumeInfo()
	canResume := resumeInfo["can_resume"].(bool)

	if canResume {
		log.Printf("[ETL %s] Found resumable state: %+v", e.ID, resumeInfo)
		if !e.confirmResume() {
			log.Printf("[ETL %s] Resume declined by user, starting fresh", e.ID)
			e.stateManager.ResetState()
		} else {
			log.Printf("[ETL %s] Resuming from checkpoint: %s", e.ID, resumeInfo["last_checkpoint"])
			e.Status = "RESUMING"
		}
	} else {
		e.Status = "RUNNING"
	}

	e.LastRunAt = time.Now()
	e.stateManager.UpdateStatus(e.Status)
	overallStart := time.Now()

	// Save initial state
	if err := e.stateManager.SaveState(); err != nil {
		log.Printf("[ETL %s] Failed to save initial state: %v", e.ID, err)
	}

	if e.pipelineConfig == nil {
		e.pipelineConfig = e.buildDefaultPipeline()
	}

	err := e.runPipeline(ctx, e.pipelineConfig, args...)
	if err != nil {
		e.Status = "FAILED"
		e.stateManager.UpdateStatus("FAILED")
		if e.Logger != nil {
			e.Logger.Printf("[ETL] Pipeline error: %v", err)
		} else {
			log.Printf("[ETL] Pipeline error: %v", err)
		}

		// Save failed state
		if e.stateManager != nil {
			e.stateManager.AddError("ETL", 0, err.Error(), 0, "CRITICAL")
			e.stateManager.SaveState()
		}
		return err
	}

	if atomic.LoadInt64(&e.metrics.Errors) >= int64(e.maxErrorCount) {
		e.Status = "FAILED"
		e.stateManager.UpdateStatus("FAILED")
		err = fmt.Errorf("maximum error threshold exceeded (%d errors)", e.metrics.Errors)
		if e.Logger != nil {
			e.Logger.Println("[ETL]", err)
		} else {
			log.Println("[ETL]", err)
		}

		// Save failed state
		if e.stateManager != nil {
			e.stateManager.AddError("ETL", 0, err.Error(), 0, "CRITICAL")
			e.stateManager.SaveState()
		}
		return err
	}

	elapsed := time.Since(overallStart)
	e.Status = "COMPLETED"
	e.stateManager.UpdateStatus("COMPLETED")

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

	// Save final state
	if e.stateManager != nil {
		e.stateManager.SaveState()
	}

	if e.checkpointStore != nil {
		return e.checkpointStore.Remove()
	}
	return nil
}

// confirmResume asks the user if they want to resume
func (e *ETL) confirmResume() bool {
	fmt.Print("Found resumable state. Do you want to resume the ETL job? (y/N): ")
	var response string
	fmt.Scanln(&response)
	response = strings.ToLower(strings.TrimSpace(response))
	return response == "y" || response == "yes"
}

// GetEnhancedMetrics returns comprehensive metrics including state information
func (e *ETL) GetEnhancedMetrics() map[string]any {
	metrics := map[string]any{}

	// Basic metrics
	basicMetrics := e.GetMetrics()
	metrics["basic"] = map[string]any{
		"extracted":         basicMetrics.Extracted,
		"mapped":            basicMetrics.Mapped,
		"transformed":       basicMetrics.Transformed,
		"loaded":            basicMetrics.Loaded,
		"errors":            basicMetrics.Errors,
		"worker_activities": basicMetrics.WorkerActivities,
	}

	// State information
	if e.stateManager != nil {
		state := e.stateManager.GetState()
		metrics["state"] = map[string]any{
			"status":            state.Status,
			"processed_records": state.ProcessedRecords,
			"failed_records":    state.FailedRecords,
			"last_checkpoint":   state.LastCheckpoint,
			"state_version":     state.StateVersion,
			"can_resume":        e.stateManager.CanResume(),
		}

		// Resume info
		resumeInfo := e.stateManager.GetResumeInfo()
		metrics["resume_info"] = resumeInfo
	}

	// Dead letter queue stats
	if e.deadLetterQueue != nil {
		dlqStats := e.deadLetterQueue.GetQueueStats()
		metrics["dead_letter_queue"] = dlqStats
	}

	// Idempotency stats
	if e.idempotencyMgr != nil {
		idempotencyStats := e.idempotencyMgr.GetStats()
		metrics["idempotency"] = idempotencyStats
	}

	return metrics
}

// Resume resumes the ETL from the last checkpoint
func (e *ETL) Resume(ctx context.Context, args ...any) error {
	if !e.stateManager.CanResume() {
		return fmt.Errorf("ETL %s cannot be resumed (current status: %s)", e.ID, e.stateManager.GetState().Status)
	}

	log.Printf("[ETL %s] Resuming ETL process", e.ID)
	return e.Run(ctx, args...)
}

// Pause pauses the ETL process
func (e *ETL) Pause() error {
	if e.Status != "RUNNING" && e.Status != "RESUMING" {
		return fmt.Errorf("ETL %s is not running (status: %s)", e.ID, e.Status)
	}

	e.Status = "PAUSED"
	if e.stateManager != nil {
		e.stateManager.UpdateStatus("PAUSED")
		e.stateManager.SaveState()
	}

	log.Printf("[ETL %s] ETL process paused", e.ID)
	return nil
}

// Stop stops the ETL process
func (e *ETL) Stop() error {
	if e.Status != "RUNNING" && e.Status != "RESUMING" && e.Status != "PAUSED" {
		return fmt.Errorf("ETL %s is not running (status: %s)", e.ID, e.Status)
	}

	e.Status = "STOPPED"
	if e.stateManager != nil {
		e.stateManager.UpdateStatus("STOPPED")
		e.stateManager.SaveState()
	}

	// Stop enhanced components
	if e.stateManager != nil {
		e.stateManager.StopAutoSave()
	}
	if e.deadLetterQueue != nil {
		e.deadLetterQueue.Stop()
	}
	if e.idempotencyMgr != nil {
		e.idempotencyMgr.Stop()
	}

	log.Printf("[ETL %s] ETL process stopped", e.ID)
	return nil
}

// GetDeadLetterRecords returns failed records for manual inspection
func (e *ETL) GetDeadLetterRecords() []DeadLetterRecord {
	if e.deadLetterQueue == nil {
		return nil
	}
	return e.deadLetterQueue.GetFailedRecords()
}

// GetStateInfo returns current state information
func (e *ETL) GetStateInfo() *ETLState {
	if e.stateManager == nil {
		return nil
	}
	return e.stateManager.GetState()
}

// GetDeadLetterQueue returns the dead letter queue
func (e *ETL) GetDeadLetterQueue() *DeadLetterQueue {
	return e.deadLetterQueue
}

// GetIdempotencyManager returns the idempotency manager
func (e *ETL) GetIdempotencyManager() *IdempotencyManager {
	return e.idempotencyMgr
}

// GetStateManager returns the state manager
func (e *ETL) GetStateManager() *StateManager {
	return e.stateManager
}

// SetCheckpointInterval sets the checkpoint interval
func (e *ETL) SetCheckpointInterval(interval time.Duration) {
	e.checkpointInterval = interval
	log.Printf("[ETL %s] Checkpoint interval set to %v", e.ID, interval)
}

// SetMaxErrorThreshold sets the maximum error threshold (placeholder)
func (e *ETL) SetMaxErrorThreshold(threshold int) {
	e.maxErrorCount = threshold
	log.Printf("[ETL %s] Max error threshold set to %d", e.ID, threshold)
}

// Cleanup cleans up old state and temporary files
func (e *ETL) Cleanup(retentionPeriod time.Duration) {
	if e.stateManager != nil {
		e.stateManager.Cleanup(retentionPeriod)
	}

	if e.deadLetterQueue != nil {
		e.deadLetterQueue.Cleanup(retentionPeriod)
	}

	if e.idempotencyMgr != nil {
		e.idempotencyMgr.CleanupExpired()
	}

	log.Printf("[ETL %s] Cleanup completed", e.ID)
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
func (e *ETL) ScheduleRun(ctx context.Context, args ...any) error {
	if e.pipelineConfig == nil || e.pipelineConfig.Schedule == "" {
		// No scheduling set; run immediately.
		return e.Run(ctx, args...)
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
			checkpointInterval: e.checkpointInterval,
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
	nodes    map[string]*dagNode // For internal processing
}

func (pc *PipelineConfig) prepare() map[string]*dagNode {
	if pc.nodes == nil {
		pc.nodes = make(map[string]*dagNode)
		for id, node := range pc.Nodes {
			pc.nodes[id] = &dagNode{
				id: id,
				pn: node,
			}
		}
		for _, edge := range pc.Edges {
			if n, ok := pc.nodes[edge.Target]; ok {
				n.indegree++
			}
		}
	}
	return pc.nodes
}

func (e *ETL) runPipeline(ctx context.Context, pc *PipelineConfig, args ...any) error {
	nodes := pc.prepare()
	var mu sync.Mutex
	var wg sync.WaitGroup
	ready := make(chan string, len(pc.nodes)) // buffered channel for ready nodes

	// Initialize ready nodes
	mu.Lock()
	for id, node := range pc.nodes {
		if node.indegree == 0 {
			ready <- id
		}
	}
	mu.Unlock()

	// Process nodes concurrently
	for i := 0; i < len(pc.nodes); i++ {
		select {
		case currentID := <-ready:
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				currentNode := nodes[id]
				var inputCh <-chan utils.Record
				if len(currentNode.inChs) == 0 {
					inputCh = nil
				} else if len(currentNode.inChs) == 1 {
					inputCh = currentNode.inChs[0]
				} else {
					inputCh = mergeChannels(currentNode.inChs)
				}
				outCh, err := currentNode.pn.Process(ctx, inputCh, e.tableCfg, args...)
				if err != nil {
					log.Printf("error running node %s: %v", id, err)
					return // or handle error appropriately
				}
				currentNode.outCh = outCh

				// Update successors
				mu.Lock()
				for _, edge := range pc.Edges {
					if edge.Source == id {
						targetNode := nodes[edge.Target]
						targetNode.inChs = append(targetNode.inChs, currentNode.outCh)
						targetNode.indegree--
						if targetNode.indegree == 0 {
							ready <- targetNode.id
						}
					}
				}
				mu.Unlock()
			}(currentID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	wg.Wait()

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
