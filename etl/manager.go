package etl

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/convert"
	"github.com/oarkflow/json"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/xid/wuid"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/sql/pkg/adapters"
	"github.com/oarkflow/sql/pkg/checkpoints"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/mappers"
	"github.com/oarkflow/sql/pkg/transformers"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/sql/pkg/utils/sqlutil"
)

func DetectConfigFormat(input string) (config.Config, error) {
	trimmed := strings.TrimSpace(input)
	var cfg config.Config
	if json.Unmarshal([]byte(trimmed), &cfg) == nil {
		return cfg, nil
	}
	if yaml.Unmarshal([]byte(trimmed), &cfg) == nil {
		return cfg, nil
	}
	if _, err := bcl.Unmarshal([]byte(trimmed), &cfg); err == nil {
		return cfg, nil
	}
	return config.Config{}, fmt.Errorf("unable to detect config format, please provide valid JSON, YAML, or BCL")
}

type Manager struct {
	mu   sync.Mutex
	etls map[string]*ETL
}

func NewManager() *Manager {
	return &Manager{
		etls: make(map[string]*ETL),
	}
}

func (m *Manager) Prepare(cfg *config.Config, options ...Option) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var preparedIDs []string
	var err error
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
		var sourceDB *squealx.DB
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

	checkpointFile := cfg.Checkpoint.File
	checkpointField := cfg.Checkpoint.Field
	if checkpointFile == "" {
		checkpointFile = "checkpoints.txt"
	}
	if checkpointField == "" {
		checkpointField = "id"
	}

	for _, tableCfg := range cfg.Tables {
		// Skip table if migration not requested.
		if utils.IsSQLType(cfg.Destinations[0].Type) && !tableCfg.Migrate {
			continue
		}
		if tableCfg.OldName == "" && sourceFile != "" {
			tableCfg.OldName = sourceFile
		}
		// When NewName is not provided, use destination file from selected destination.
		// Select destination for this table using helper.
		destCfg, err := selectDestination(cfg.Destinations, tableCfg)
		if err != nil {
			return nil, err
		}
		if tableCfg.NewName == "" {
			tableCfg.NewName = destCfg.File
		}
		// For SQL type destination, auto-create table if required.
		var destDB *squealx.DB
		if utils.IsSQLType(destCfg.Type) && tableCfg.AutoCreateTable && tableCfg.KeyValueTable {
			destDB, err = config.OpenDB(destCfg)
			if err != nil {
				log.Printf("Error connecting to destination DB: %v\n", err)
				return nil, err
			}
			if err := sqlutil.CreateKeyValueTable(
				destDB, tableCfg.NewName,
				tableCfg.KeyField, tableCfg.ValueField, tableCfg.TruncateDestination, tableCfg.ExtraValues,
			); err != nil {
				log.Printf("Error creating key-value table %s: %v", tableCfg.NewName, err)
				return nil, err
			}
		} else if utils.IsSQLType(destCfg.Type) {
			destDB, err = config.OpenDB(destCfg)
			if err != nil {
				log.Printf("Error connecting to destination DB: %v\n", err)
				return nil, err
			}
		}
		var opts []Option
		opts = append(opts, WithSources(sources...))
		opts = append(opts, WithDestination(destCfg, destDB, tableCfg))
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
		id := wuid.New().String()
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
		m.etls[etlJob.ID] = etlJob
		preparedIDs = append(preparedIDs, etlJob.ID)
	}
	log.Println("All ETL jobs prepared.")
	return preparedIDs, nil
}

func (m *Manager) Start(ctx context.Context, etlID string) error {
	m.mu.Lock()
	etlJob, ok := m.etls[etlID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no ETL job found with ID: %s", etlID)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	Shutdown(cancel)
	if err := etlJob.Run(ctx); err != nil {
		log.Printf("ETL job %s failed: %v", etlID, err)
		return err
	}
	if err := etlJob.Close(); err != nil {
		log.Printf("Error closing ETL job %s: %v", etlID, err)
		return err
	}
	etlJob.Status = "COMPLETED"
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
