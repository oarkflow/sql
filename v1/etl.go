package v1

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/oarkflow/expr"

	"github.com/oarkflow/sql/adapters"
	"github.com/oarkflow/sql/checkpoint"
	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/etl/contract"
	"github.com/oarkflow/sql/mappers"
	"github.com/oarkflow/sql/resilience"
	"github.com/oarkflow/sql/transactions"
	"github.com/oarkflow/sql/utils"
	"github.com/oarkflow/sql/utils/sqlutil"
)

func NewLookupLoader(lkup config.LookupConfig) (contract.LookupLoader, error) {
	switch strings.ToLower(lkup.Type) {
	case "postgresql", "mysql", "sqlite":
		var dsn string
		switch strings.ToLower(lkup.Driver) {
		case "postgres", "postgresql":
			dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				lkup.Host, lkup.Port, lkup.Username, lkup.Password, lkup.Database)
		case "mysql":
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
				lkup.Username, lkup.Password, lkup.Host, lkup.Port, lkup.Database)
		case "sqlite":
			dsn = lkup.File
		default:
			log.Fatalf("unsupported SQL driver: %s", lkup.Driver)
		}
		db, err := sql.Open(lkup.Driver, dsn)
		if err != nil {
			log.Fatalf("Error connecting to lookup DB: %v", err)
		}
		return adapters.NewSQLAdapterAsSource(db, "", lkup.Source), nil
	case "csv", "json":
		return adapters.NewFileAdapter(lkup.File, "source", false), nil
	default:
		return nil, fmt.Errorf("Unsupported lookup type: %s", lkup.Type)
	}
}

func Run(cfg *config.Config) {
	var sourceDB *sql.DB
	var destDB *sql.DB
	var err error
	if utils.IsSQLType(cfg.Source.Type) {
		sourceDB, err = etl.OpenDB(cfg.Source)
		if err != nil {
			log.Fatalf("Error connecting to source DB: %v", err)
		}
		defer sourceDB.Close()
	}
	if utils.IsSQLType(cfg.Destination.Type) {
		destDB, err = etl.OpenDB(cfg.Destination)
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
	for _, tableCfg := range cfg.Tables {
		if utils.IsSQLType(cfg.Destination.Type) && !tableCfg.Migrate {
			continue
		}
		if tableCfg.OldName == "" && cfg.Source.File != "" {
			tableCfg.OldName = cfg.Source.File
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
			WithSource(cfg.Source.Type, sourceDB, cfg.Source.File, tableCfg.OldName, tableCfg.Query),
			WithDestination(cfg.Destination.Type, destDB, cfg.Destination.File, tableCfg),
			WithCheckpoint(checkpoint.NewFileCheckpointStore("checkpoint.txt"), func(rec utils.Record) string {
				name, _ := rec["name"].(string)
				return name
			}),
			WithWorkerCount(cfg.WorkerCount),
			WithBatchSize(tableCfg.BatchSize),
			WithRawChanBuffer(cfg.Buffer),
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
				lookup, err := NewLookupLoader(lkup)
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
			log.Printf("ETL job failed: %v", err)
		}
		if err := etlJob.Close(); err != nil {
			log.Printf("Error closing ETL job: %v", err)
		}
		log.Printf("Migration for %s complete", tableCfg.OldName)
	}
	log.Println("All migrations complete.")
}

type ETL struct {
	sources         []contract.Source
	mappers         []contract.Mapper
	validators      []contract.Validator
	transformers    []contract.Transformer
	loaders         []contract.Loader
	workerCount     int
	batchSize       int
	retryCount      int
	retryDelay      time.Duration
	loaderWorkers   int
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
	lookupStore     map[string][]map[string]string
	lookupInCache   sync.Map
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
		lookupStore:    make(map[string][]map[string]string),
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
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	defer cancel()

	if e.checkpointStore != nil {
		if cp, err := e.checkpointStore.GetCheckpoint(ctx); err != nil {
			log.Printf("Error retrieving checkpoint: %v", err)
		} else {
			e.lastCheckpoint = cp
			log.Printf("Resuming from checkpoint: %s", e.lastCheckpoint)
		}
	}
	rawChan := make(chan utils.Record, e.rawChanBuffer)
	var srcWG sync.WaitGroup
	for _, s := range e.sources {
		if err := s.Setup(ctx); err != nil {
			e.incrementError()
			return fmt.Errorf("source setup error: %v", err)
		}
		srcWG.Add(1)
		go func(src contract.Source) {
			defer srcWG.Done()
			ch, err := src.Extract(ctx)
			if err != nil {
				e.incrementError()
				log.Printf("Source extraction error: %v", err)
				return
			}
			for rec := range ch {
				rawChan <- rec
			}
		}(s)
	}
	go func() {
		srcWG.Wait()
		close(rawChan)
	}()
	processedChan := make(chan utils.Record, e.workerCount*2)
	var procWG sync.WaitGroup
	for i := 0; i < e.workerCount; i++ {
		procWG.Add(1)
		go func(workerID int) {
			defer procWG.Done()
			for raw := range rawChan {
				rec, err := e.applyMappers(ctx, raw, workerID)
				if err != nil {
					e.incrementError()
					continue
				}
				if rec == nil {
					continue
				}
				outRecords, err := e.applyTransformers(ctx, rec, workerID)
				if err != nil {
					e.incrementError()
					continue
				}
				for _, r := range outRecords {
					processedChan <- r
				}
			}
		}(i)
	}
	go func() {
		procWG.Wait()
		close(processedChan)
	}()
	batchChan := make(chan []utils.Record, e.loaderWorkers)
	var batchWG sync.WaitGroup
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		batch := make([]utils.Record, 0, e.batchSize)
		for rec := range processedChan {
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				batchChan <- batch
				batch = make([]utils.Record, 0, e.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()
	var loaderWG sync.WaitGroup
	for i := 0; i < e.loaderWorkers; i++ {
		loaderWG.Add(1)
		go func(workerID int) {
			defer loaderWG.Done()
			for batch := range batchChan {
				for _, loader := range e.loaders {
					if err := loader.Setup(ctx); err != nil {
						e.incrementError()
						log.Printf("[Loader Worker %d] Loader setup error: %v", workerID, err)
						continue
					}
					if txnLoader, ok := loader.(contract.Transactional); ok {
						if err := txnLoader.Begin(ctx); err != nil {
							log.Printf("[Loader Worker %d] Error beginning transaction: %v", workerID, err)
							continue
						}
						if sqlLoader, ok := loader.(*adapters.SQLAdapter); ok && sqlLoader.AutoCreate && !sqlLoader.Created {
							if err := sqlutil.CreateTableFromRecord(sqlLoader.Db, sqlLoader.Table, batch[0]); err != nil {
								e.incrementError()
								log.Printf("[Loader Worker %d] Error auto-creating table: %v", workerID, err)
								continue
							}
							sqlLoader.Created = true
						}
						if err := resilience.RetryWithCircuit(e.retryCount, e.retryDelay, e.circuitBreaker, func() error {
							return loader.StoreBatch(ctx, batch)
						}); err != nil {
							e.incrementError()
							log.Printf("[Loader Worker %d] Error loading batch with transaction: %v", workerID, err)
							continue
						}
						if err := txnLoader.Commit(ctx); err != nil {
							log.Printf("[Loader Worker %d] Commit failed: %v", workerID, err)
							continue
						}
					} else {
						err := transactions.RunInTransaction(ctx, func(tx *transactions.Transaction) error {
							return resilience.RetryWithCircuit(e.retryCount, e.retryDelay, e.circuitBreaker, func() error {
								return loader.StoreBatch(ctx, batch)
							})
						})
						if err != nil {
							log.Printf("[Loader Worker %d] Error loading batch: %v", workerID, err)
							continue
						}
					}
					if e.checkpointStore != nil && e.checkpointFunc != nil {
						cp := e.checkpointFunc(batch[len(batch)-1])
						e.cpMutex.Lock()
						if cp > e.lastCheckpoint {
							if err := e.checkpointStore.SaveCheckpoint(ctx, cp); err != nil {
								e.incrementError()
								log.Printf("[Loader Worker %d] Error saving checkpoint: %v", workerID, err)
							} else {
								e.lastCheckpoint = cp
							}
						}
						e.cpMutex.Unlock()
					}
				}
			}
		}(i)
	}
	batchWG.Wait()
	loaderWG.Wait()
	return nil
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

func (e *ETL) applyMappers(ctx context.Context, rec utils.Record, workerID int) (utils.Record, error) {
	for _, mapper := range e.mappers {
		var err error
		rec, err = mapper.Map(ctx, rec)
		if err != nil {
			log.Printf("[Worker %d] Mapper (%s) error: %v", workerID, mapper.Name(), err)
			return nil, err
		}
	}
	return rec, nil
}

func (e *ETL) applyTransformers(ctx context.Context, rec utils.Record, workerID int) ([]utils.Record, error) {
	records := []utils.Record{rec}
	for _, transformer := range e.transformers {
		var nextRecords []utils.Record
		for _, r := range records {
			if mt, ok := transformer.(contract.MultiTransformer); ok {
				recs, err := mt.TransformMany(ctx, r)
				if err != nil {
					log.Printf("[Worker %d] MultiTransformer error: %v", workerID, err)
					continue
				}
				nextRecords = append(nextRecords, recs...)
			} else {
				r2, err := transformer.Transform(ctx, r)
				if err != nil {
					log.Printf("[Worker %d] Transformer error: %v", workerID, err)
					continue
				}
				nextRecords = append(nextRecords, r2)
			}
		}
		records = nextRecords
	}
	return records, nil
}

func (e *ETL) incrementError() {
	e.errorLock.Lock()
	defer e.errorLock.Unlock()
	e.errorCount++
	if e.errorCount >= e.maxErrorCount {
		log.Println("Max error count reached, cancelling ETL process")
		if e.cancelFunc != nil {
			e.cancelFunc()
		}
	}
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
		if row[lookupField] == sourceValStr {
			result := row[targetField]
			e.lookupInCache.Store(cacheKey, result)
			return result, nil
		}
	}
	return nil, fmt.Errorf("lookupIn: no matching value for %s in dataset %s", sourceValStr, datasetKey)
}
