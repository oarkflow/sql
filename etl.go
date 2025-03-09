package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/oarkflow/expr"

	"github.com/oarkflow/etl/adapters"
	"github.com/oarkflow/etl/checkpoint"
	"github.com/oarkflow/etl/config"
	"github.com/oarkflow/etl/contract"
	"github.com/oarkflow/etl/mappers"
	"github.com/oarkflow/etl/resilience"
	"github.com/oarkflow/etl/transactions"
	"github.com/oarkflow/etl/utils"
	"github.com/oarkflow/etl/utils/sqlutil"
)

func Run(cfg *config.Config) {
	var destDB *sql.DB
	var err error
	if utils.IsSQLType(cfg.Destination.Type) {
		destDB, err = config.OpenDB(cfg.Destination)
		if err != nil {
			log.Fatalf("Error connecting to destination DB: %v", err)
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

	if len(cfg.Sources) == 0 && !utils.IsEmpty(cfg.Source) {
		cfg.Sources = append(cfg.Sources, cfg.Source)
	}
	for _, sourceCfg := range cfg.Sources {
		if sourceCfg.File != "" {
			sourceFile = sourceCfg.File
		}
		var sourceDB *sql.DB
		if utils.IsSQLType(sourceCfg.Type) {
			sourceDB, err = config.OpenDB(sourceCfg)
			if err != nil {
				log.Fatalf("Error connecting to source DB: %v", err)
			}
		}
		src, err := NewSource(sourceCfg.Type, sourceDB, sourceCfg.File, sourceCfg.Table, sourceCfg.Source)
		if err != nil {
			panic(err)
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
				log.Fatalf("Error creating key-value table %s: %v", tableCfg.NewName, err)
			}
		}
		opts := []Option{
			WithSources(sources...),
			WithDestination(cfg.Destination.Type, destDB, cfg.Destination.File, tableCfg),
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
		if len(cfg.Lookups) > 0 {
			for _, lkup := range cfg.Lookups {
				lookup, err := adapters.NewLookupLoader(lkup)
				if err != nil {
					log.Fatalf("Unsupported lookup type: %s", lkup.Type)
				}
				data, err := lookup.LoadData()
				if err != nil {
					log.Fatalf("Failed to load lookup data for %s: %v", lkup.Key, err)
				}
				etlJob.lookupStore[lkup.Key] = data
			}
		}
		expr.AddFunction("lookupIn", etlJob.lookupIn)
		ctx := context.Background()
		if err := etlJob.Run(ctx); err != nil {
			log.Printf("ETL DAG job failed: %v", err)
		}
		if err := etlJob.Close(); err != nil {
			log.Printf("Error closing ETL job: %v", err)
		}
		log.Printf("Migration for %s complete", tableCfg.OldName)
	}
	log.Println("All migrations complete.")
}

type SourceNode struct {
	sources       []contract.Source
	rawChanBuffer int
}

func (sn *SourceNode) Process(ctx context.Context, _ <-chan utils.Record) (<-chan utils.Record, error) {
	out := make(chan utils.Record, sn.rawChanBuffer)
	var wg sync.WaitGroup
	for _, src := range sn.sources {
		if err := src.Setup(ctx); err != nil {
			return nil, fmt.Errorf("source setup error: %v", err)
		}
		wg.Add(1)
		go func(source contract.Source) {
			defer wg.Done()
			ch, err := source.Extract(ctx)
			if err != nil {
				log.Printf("Source extraction error: %v", err)
				return
			}
			for rec := range ch {
				out <- rec
			}
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

func (nn *NormalizeNode) Process(ctx context.Context, in <-chan utils.Record) (<-chan utils.Record, error) {
	out := make(chan utils.Record, nn.workerCount*2)
	var wg sync.WaitGroup
	for i := 0; i < nn.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for rec := range in {
				if nn.schema == nil {
					out <- rec
					continue
				}
				nRec, err := normalizeRecord(rec, nn.schema)
				if err != nil {
					log.Printf("[Normalize Worker %d] Error: %v", workerID, err)
					continue
				}
				out <- nRec
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

type MapNode struct {
	mappers     []contract.Mapper
	workerCount int
}

func (mn *MapNode) Process(ctx context.Context, in <-chan utils.Record) (<-chan utils.Record, error) {
	out := make(chan utils.Record, mn.workerCount*2)
	var wg sync.WaitGroup
	for i := 0; i < mn.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for rec := range in {
				mapped, err := applyMappers(ctx, rec, mn.mappers, workerID)
				if err != nil {
					log.Printf("[Mapper Worker %d] Error: %v", workerID, err)
					continue
				}
				if mapped != nil {
					out <- mapped
				}
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

type TransformNode struct {
	transformers []contract.Transformer
	workerCount  int
}

func (tn *TransformNode) Process(ctx context.Context, in <-chan utils.Record) (<-chan utils.Record, error) {
	out := make(chan utils.Record, tn.workerCount*2)
	var wg sync.WaitGroup
	for i := 0; i < tn.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for rec := range in {
				transformed, err := applyTransformers(ctx, rec, tn.transformers, workerID)
				if err != nil {
					log.Printf("[Transformer Worker %d] Error: %v", workerID, err)
					continue
				}
				for _, r := range transformed {
					out <- r
				}
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
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

func (ln *LoaderNode) Process(ctx context.Context, in <-chan utils.Record) (<-chan utils.Record, error) {
	done := make(chan utils.Record)
	batchChan := make(chan []utils.Record, ln.workerCount)
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
			for batch := range batchChan {
				for _, loader := range ln.loaders {
					if err := loader.Setup(ctx); err != nil {
						log.Printf("[Loader Worker %d] Setup error: %v", workerID, err)
						continue
					}
					if txnLoader, ok := loader.(contract.Transactional); ok {
						if err := txnLoader.Begin(ctx); err != nil {
							log.Printf("[Loader Worker %d] Begin transaction error: %v", workerID, err)
							continue
						}
						if sqlLoader, ok := loader.(*adapters.SQLAdapter); ok && sqlLoader.AutoCreate && !sqlLoader.Created {
							if err := sqlutil.CreateTableFromRecord(sqlLoader.Db, sqlLoader.Table, batch[0]); err != nil {
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
		}(i)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return done, nil
}

func normalizeRecord(rec utils.Record, schema map[string]string) (utils.Record, error) {
	for field, targetType := range schema {
		if val, ok := rec[field]; ok {
			normalized, err := normalizeValue(val, targetType)
			if err != nil {
				return nil, fmt.Errorf("error normalizing field %s: %v", field, err)
			}
			rec[field] = normalized
		}
	}
	return rec, nil
}

func normalizeValue(val interface{}, targetType string) (interface{}, error) {
	switch targetType {
	case "int":
		switch v := val.(type) {
		case int:
			return v, nil
		case int64:
			return int(v), nil
		case float64:
			return int(v), nil
		case string:
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			return i, nil
		case bool:
			if v {
				return 1, nil
			}
			return 0, nil
		default:
			return nil, fmt.Errorf("unsupported type for int conversion: %T", val)
		}
	case "bool":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}
			return b, nil
		case int:
			return v != 0, nil
		case float64:
			return v != 0, nil
		default:
			return nil, fmt.Errorf("unsupported type for bool conversion: %T", val)
		}
	case "float":
		switch v := val.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
		default:
			return nil, fmt.Errorf("unsupported type for float conversion: %T", val)
		}
	case "string":
		return fmt.Sprintf("%v", val), nil
	default:
		return nil, fmt.Errorf("unknown target type: %s", targetType)
	}
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
		outCh, err := currentNode.pn.Process(ctx, input)
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

func (e *ETL) Run(ctx context.Context) error {
	if e.pipelineConfig == nil {
		e.pipelineConfig = e.buildDefaultPipeline()
	}
	return e.runPipeline(ctx, e.pipelineConfig)
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

func (e *ETL) lookupIn(args ...interface{}) (interface{}, error) {
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
