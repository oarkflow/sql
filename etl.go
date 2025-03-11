package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/oarkflow/expr"

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/checkpoint"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/contract"
	"github.com/oarkflow/etl/pkg/mappers"
	"github.com/oarkflow/etl/pkg/resilience"
	"github.com/oarkflow/etl/pkg/transactions"
	"github.com/oarkflow/etl/pkg/transformers"
	"github.com/oarkflow/etl/pkg/utils"
	"github.com/oarkflow/etl/pkg/utils/sqlutil"
)

func Run(cfg *config.Config) error {
	var destDB *sql.DB
	var err error
	if utils.IsSQLType(cfg.Destination.Type) {
		destDB, err = config.OpenDB(cfg.Destination)
		if err != nil {
			log.Printf("Error connecting to destination DB: %v\n", err)
			return err
		}
		defer destDB.Close()
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
	var sources []contract.Source
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
		log.Printf("Starting migration: %s -> %s", tableCfg.OldName, tableCfg.NewName)
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
			WithCheckpoint(checkpoint.NewFileCheckpointStore("checkpoint.txt"), func(rec utils.Record) string {
				name, _ := rec["name"].(string)
				return name
			}),
			WithWorkerCount(cfg.WorkerCount),
			WithBatchSize(tableCfg.BatchSize),
			WithRawChanBuffer(cfg.Buffer),
		}
		if tableCfg.NormalizeSchema != nil {
			opts = append(opts, WithNormalizeSchema(tableCfg.NormalizeSchema))
		}
		var mapperList []contract.Mapper
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
		ctx := context.Background()
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
				etlJob.lookupStore[lkup.Key] = data
			}
		}
		expr.AddFunction("lookupIn", etlJob.lookupIn)
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

type SourceNode struct {
	sources       []contract.Source
	rawChanBuffer int
}

func (sn *SourceNode) Process(ctx context.Context, _ <-chan utils.Record, tableCfg config.TableMapping) (<-chan utils.Record, error) {
	out := make(chan utils.Record, sn.rawChanBuffer)
	var wg sync.WaitGroup
	for _, src := range sn.sources {
		if err := src.Setup(ctx); err != nil {
			return nil, fmt.Errorf("source setup error: %v", err)
		}
		wg.Add(1)
		go func(source contract.Source) {
			defer wg.Done()
			startTime := time.Now()
			recordCount := 0
			var opts []contract.Option
			if tableCfg.OldName != "" {
				opts = append(opts, contract.WithTable(tableCfg.OldName))
			} else if tableCfg.Query != "" {
				opts = append(opts, contract.WithQuery(tableCfg.Query))
			}
			ch, err := source.Extract(ctx, opts...)
			if err != nil {
				log.Printf("Source extraction error: %v", err)
				return
			}
			for rec := range ch {
				out <- rec
				recordCount++
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
	mappers     []contract.Mapper
	workerCount int
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
				mapped, err := applyMappers(ctx, rec, mn.mappers, workerID)
				if err != nil {
					log.Printf("[Mapper Worker %d] Error: %v", workerID, err)
					continue
				}
				if mapped != nil {
					out <- mapped
					localCount++
				}
			}
			atomic.AddInt64(&totalMapped, int64(localCount))
			log.Printf("[Mapper Worker %d] mapped %d records", workerID, localCount)
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
		elapsed := time.Since(startTime)
		log.Printf("[Mapper] Total mapped records: %d in %v", totalMapped, elapsed)
	}()
	return out, nil
}

type TransformNode struct {
	transformers []contract.Transformer
	workerCount  int
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
				transformed, err := applyTransformers(ctx, rec, tn.transformers, workerID)
				if err != nil {
					log.Printf("[Transformer Worker %d] Error: %v", workerID, err)
					continue
				}
				for _, r := range transformed {
					if r != nil {
						out <- r
						localCount++
					}
				}
			}
			atomic.AddInt64(&totalTransformed, int64(localCount))
			log.Printf("[Transformer Worker %d] transformed %d records", workerID, localCount)
		}(i)
	}
	go func() {
		wg.Wait()
		for _, t := range tn.transformers {
			if flushable, ok := t.(contract.Flushable); ok {
				flushRecords, err := flushable.Flush(ctx)
				if err != nil {
					log.Printf("[Transformer] Flush error: %v", err)
					continue
				}
				for _, r := range flushRecords {
					out <- r
				}
			}
		}
		close(out)
		elapsed := time.Since(startTime)
		log.Printf("[Transformer] Total transformed records: %d in %v", totalTransformed, elapsed)
	}()
	return out, nil
}

type LoaderNode struct {
	loaders         []contract.Loader
	workerCount     int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	circuitBreaker  *resilience.CircuitBreaker
	checkpointStore contract.CheckpointStore
	checkpointFunc  func(rec utils.Record) string
	cpMutex         *sync.Mutex
	lastCheckpoint  string
}

func (ln *LoaderNode) Process(ctx context.Context, in <-chan utils.Record, _ config.TableMapping) (<-chan utils.Record, error) {
	done := make(chan utils.Record)
	batchChan := make(chan []utils.Record, ln.workerCount)
	startTime := time.Now()
	var totalLoaded int64
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
	var wg sync.WaitGroup
	for i := 0; i < ln.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLoaded := 0
			for batch := range batchChan {
				for _, loader := range ln.loaders {
					if err := loader.Setup(ctx); err != nil {
						log.Printf("[Loader Worker %d] Setup error: %v", workerID, err)
					}
					if txnLoader, ok := loader.(contract.Transactional); ok {
						if err := txnLoader.Begin(ctx); err != nil {
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
						err := resilience.RetryWithCircuit(ln.retryCount, ln.retryDelay, ln.circuitBreaker, func() error {
							return loader.StoreBatch(ctx, batch)
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Batch load error (transaction): %v", workerID, err)
							continue
						}
						if err := txnLoader.Commit(ctx); err != nil {
							log.Printf("[Loader Worker %d] Commit error: %v", workerID, err)
							continue
						}
					} else {
						err := transactions.RunInTransaction(ctx, func(tx *transactions.Transaction) error {
							return resilience.RetryWithCircuit(ln.retryCount, ln.retryDelay, ln.circuitBreaker, func() error {
								return loader.StoreBatch(ctx, batch)
							})
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Batch load error: %v", workerID, err)
							continue
						}
					}
					localLoaded += len(batch)
					if ln.checkpointStore != nil && ln.checkpointFunc != nil {
						cp := ln.checkpointFunc(batch[len(batch)-1])
						ln.cpMutex.Lock()
						if cp > ln.lastCheckpoint {
							if err := ln.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
								log.Printf("[Loader Worker %d] Checkpoint error: %v", workerID, err)
							} else {
								ln.lastCheckpoint = cp
							}
						}
						ln.cpMutex.Unlock()
					}
				}
			}
			atomic.AddInt64(&totalLoaded, int64(localLoaded))
			log.Printf("[Loader Worker %d] loaded %d records", workerID, localLoaded)
		}(i)
	}
	go func() {
		wg.Wait()
		close(done)
		elapsed := time.Since(startTime)
		log.Printf("[Loader] Total loaded records: %d in %v", totalLoaded, elapsed)
	}()
	return done, nil
}

func applyMappers(ctx context.Context, rec utils.Record, mappers []contract.Mapper, workerID int) (utils.Record, error) {
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

func applyTransformers(ctx context.Context, rec utils.Record, transformers []contract.Transformer, workerID int) ([]utils.Record, error) {
	records := []utils.Record{rec}
	for _, transformer := range transformers {
		var nextRecords []utils.Record
		if mt, ok := transformer.(contract.MultiTransformer); ok {
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

type dagNode struct {
	id       string
	pn       contract.Node
	inChs    []<-chan utils.Record
	outCh    <-chan utils.Record
	indegree int
}

type dagEdge struct {
	Source string
	Target string
}

type PipelineConfig struct {
	Nodes map[string]contract.Node
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

func (e *ETL) buildDefaultPipeline() *PipelineConfig {
	nodes := map[string]contract.Node{
		"source": &SourceNode{
			sources:       e.sources,
			rawChanBuffer: e.rawChanBuffer,
		},
		"normalize": &NormalizeNode{
			schema:      e.normalizeSchema,
			workerCount: e.workerCount,
		},
		"map": &MapNode{
			mappers:     e.mappers,
			workerCount: e.workerCount,
		},
		"transform": &TransformNode{
			transformers: e.transformers,
			workerCount:  e.workerCount,
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

type ETL struct {
	sources         []contract.Source
	mappers         []contract.Mapper
	transformers    []contract.Transformer
	loaders         []contract.Loader
	workerCount     int
	loaderWorkers   int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	rawChanBuffer   int
	checkpointStore contract.CheckpointStore
	checkpointFunc  func(rec utils.Record) string
	lastCheckpoint  string
	cpMutex         sync.Mutex
	maxErrorCount   int
	errorCount      int
	errorLock       sync.Mutex
	circuitBreaker  *resilience.CircuitBreaker
	cancelFunc      context.CancelFunc
	lookupStore     map[string][]utils.Record
	lookupInCache   sync.Map
	pipelineConfig  *PipelineConfig
	normalizeSchema map[string]string
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
		circuitBreaker: resilience.NewCircuitBreaker(5, 5*time.Second),
	}
}

func NewETL(opts ...Option) *ETL {
	e := defaultConfig()
	for _, opt := range opts {
		if err := opt(e); err != nil {
			log.Printf("Error applying option: %v", err)
		}
	}
	return e
}

func (e *ETL) Run(ctx context.Context, tableCfg config.TableMapping) error {
	overallStart := time.Now()
	if e.pipelineConfig == nil {
		e.pipelineConfig = e.buildDefaultPipeline()
	}
	err := e.runPipeline(ctx, e.pipelineConfig, tableCfg)
	elapsed := time.Since(overallStart)
	log.Printf("[ETL] Total pipeline execution time: %v", elapsed)
	return err
}

func (e *ETL) Close() error {
	for _, src := range e.sources {
		if err := src.Close(); err != nil {
			return fmt.Errorf("error closing source: %v", err)
		}
	}
	for _, loader := range e.loaders {
		if err := loader.Close(); err != nil {
			return fmt.Errorf("error closing loader: %v", err)
		}
	}
	return nil
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
