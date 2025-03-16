package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/oarkflow/convert"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/checkpoints"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/contracts"
	"github.com/oarkflow/etl/pkg/mappers"
	"github.com/oarkflow/etl/pkg/transformers"
	"github.com/oarkflow/etl/pkg/utils"
	"github.com/oarkflow/etl/pkg/utils/sqlutil"
)

// -------------------------
// Manager Implementation
// -------------------------

// Manager manages available ETL jobs.
type Manager struct {
	mu   sync.Mutex
	etls map[string]*ETL
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		etls: make(map[string]*ETL),
	}
}

// Prepare parses the configuration, prepares sources, mappers, loaders, etc., and creates one or more ETL jobs.
// It returns a slice of prepared ETL job IDs.
func (m *Manager) Prepare(cfg *config.Config, options ...Option) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var preparedIDs []string

	// Open destination DB if needed.
	var destDB *sql.DB
	var err error
	if utils.IsSQLType(cfg.Destination.Type) {
		destDB, err = config.OpenDB(cfg.Destination)
		if err != nil {
			log.Printf("Error connecting to destination DB: %v\n", err)
			return nil, err
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

	// Prepare sources.
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
		if sourceCfg.Key != "" {
			var keys []string
			for _, tableCfg := range cfg.Tables {
				if tableCfg.OldName != "" {
					keys = append(keys, sourceCfg.Key+"."+tableCfg.OldName)
				}
			}
			tmp = append(tmp, strings.Join(keys, ", "))
		}
		if len(tmp) > 0 {
			sourcesToMigrate = append(sourcesToMigrate, strings.Join(tmp, ", "))
		}
		var sourceDB *sql.DB
		if utils.IsSQLType(sourceCfg.Type) {
			sourceDB, err = config.OpenDB(sourceCfg)
			if err != nil {
				log.Printf("Error connecting to source DB: %v\n", err)
				return nil, err
			}
		}
		src, err := NewSource(sourceCfg.Type, sourceDB, sourceCfg.File, sourceCfg.Table, sourceCfg.Source, sourceCfg.Format)
		if err != nil {
			return nil, err
		}
		sources = append(sources, src)
	}

	// Prepare checkpoint settings.
	checkpointFile := cfg.Checkpoint.File
	checkpointField := cfg.Checkpoint.Field
	if checkpointFile == "" {
		checkpointFile = "checkpoints.txt"
	}
	if checkpointField == "" {
		checkpointField = "id"
	}

	// For each table configuration, prepare an ETL job.
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
				return nil, err
			}
		}
		// Build options for ETL.
		var opts []Option
		opts = append(opts, WithSources(sources...))
		opts = append(opts, WithDestination(cfg.Destination, destDB, tableCfg))
		opts = append(opts, WithCheckpoint(checkpoints.NewFileCheckpointStore(checkpointFile), func(rec utils.Record) string {
			val, ok := rec[checkpointField]
			if !ok {
				return ""
			}
			v, _ := convert.ToString(val)
			return v
		}))
		opts = append(opts, WithWorkerCount(cfg.WorkerCount))
		opts = append(opts, WithBatchSize(tableCfg.BatchSize))
		opts = append(opts, WithRawChanBuffer(cfg.Buffer))
		opts = append(opts, WithStreamingMode(cfg.StreamingMode))
		opts = append(opts, WithDistributedMode(cfg.DistributedMode))
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
		id := xid.New().String()
		etlJob := NewETL(id, id, opts...)
		var lookups []contracts.LookupLoader
		if len(cfg.Lookups) > 0 {
			for _, lkup := range cfg.Lookups {
				lookup, err := adapters.NewLookupLoader(lkup)
				if err != nil {
					log.Printf("Unsupported lookup type: %s", lkup.Type)
					return nil, err
				}
				err = lookup.Setup(context.Background())
				if err != nil {
					log.Printf("Unable to setup lookup: %s", lkup.Type)
					return nil, err
				}
				data, err := lookup.LoadData()
				if err != nil {
					log.Printf("Failed to load lookup data for %s: %v", lkup.Key, err)
					return nil, err
				}
				lookups = append(lookups, lookup)
				etlJob.lookupStore[lkup.Key] = data
			}
		}
		etlJob.lookups = append(etlJob.lookups, lookups...)
		for _, loader := range etlJob.loaders {
			err = loader.Setup(context.Background())
			if err != nil {
				log.Printf("Setting up loader failed: %v", err)
				return nil, err
			}
		}
		etlJob.SetTableConfig(tableCfg)
		log.Printf("Prepared migration: %s -> %s", tableCfg.OldName, tableCfg.NewName)
		// Store the prepared ETL job in the manager.
		m.etls[etlJob.ID] = etlJob
		preparedIDs = append(preparedIDs, etlJob.ID)
	}
	log.Println("All ETL jobs prepared.")
	return preparedIDs, nil
}

// Start retrieves a prepared ETL job by its ID and runs it.
func (m *Manager) Start(ctx context.Context, etlID string) error {
	m.mu.Lock()
	etlJob, ok := m.etls[etlID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no ETL job found with ID: %s", etlID)
	}

	// Setup graceful shutdown.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	Shutdown(cancel)

	// Run the ETL pipeline.
	if err := etlJob.Run(ctx); err != nil {
		log.Printf("ETL job %s failed: %v", etlID, err)
		return err
	}
	if err := etlJob.Close(); err != nil {
		log.Printf("Error closing ETL job %s: %v", etlID, err)
		return err
	}
	log.Printf("ETL job %s completed successfully.", etlID)
	return nil
}

func (m *Manager) GetETL(id string) (*ETL, bool) {
	m.mu.Lock()
	etl, ok := m.etls[id]
	m.mu.Unlock()
	return etl, ok
}

func (m *Manager) AdjustWorker(workerCount int, etlID string) {
	etl, ok := m.GetETL(etlID)
	if ok {
		etl.AdjustWorker(workerCount)
	}
}
