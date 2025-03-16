package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/oarkflow/convert"
	"github.com/oarkflow/json"
	"github.com/oarkflow/xid"

	"github.com/gofiber/fiber/v2"

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
				return nil, err
			}
		}
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

func (m *Manager) Serve(addr string) error {
	app := fiber.New()
	app.Use(logger.New())

	// Home page with links.
	app.Get("/", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		homeHTML := `
			<h1>ETL Manager</h1>
			<ul>
				<li><a href="/config">Enter ETL Config</a></li>
				<li><a href="/etls">List ETL Jobs</a></li>
			</ul>
		`
		return c.SendString(homeHTML)
	})

	// Page to enter JSON config.
	app.Get("/config", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		configHTML := `
			<h1>Enter ETL Config (JSON)</h1>
			<form action="/config" method="POST">
				<textarea name="config" rows="20" cols="80"></textarea><br>
				<input type="submit" value="Submit">
			</form>
		`
		return c.SendString(configHTML)
	})

	// POST endpoint to accept JSON config and prepare ETL.
	app.Post("/config", func(c *fiber.Ctx) error {
		configJSON := c.FormValue("config")
		var cfg config.Config
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return c.Status(400).SendString(fmt.Sprintf("Invalid config JSON: %v", err))
		}
		_, err := m.Prepare(&cfg)
		if err != nil {
			return c.Status(500).SendString(fmt.Sprintf("Error preparing ETL: %v", err))
		}
		return c.Redirect("/etls")
	})

	// Page to list existing ETL jobs with their status.
	app.Get("/etls", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		m.mu.Lock()
		defer m.mu.Unlock()
		html := "<h1>ETL Jobs</h1><ul>"
		for id, etl := range m.etls {
			html += fmt.Sprintf("<li>ID: %s - Status: %s - <a href=\"/etls/%s\">Details</a></li> - <a href=\"/etls/%s/start\">Start</a></li>", id, etl.Status, id, id)
		}
		html += "</ul>"
		return c.SendString(html)
	})

	// Page to show details and live metrics for an ETL job.
	app.Get("/etls/:id/start", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		go func(etl *ETL) {
			err := etl.Run(c.Context())
			if err != nil {
				panic(err)
			}
		}(etl)
		return c.Redirect("/etls/" + id)
	})

	app.Post("/etls/:id/adjust-worker", func(c *fiber.Ctx) error {
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		workerCount, _ := strconv.Atoi(c.FormValue("worker_count"))
		if workerCount == 0 {
			return c.Status(400).SendString("Invalid worker count")
		}
		etl.AdjustWorker(workerCount)
		return c.Redirect("/etls/" + id)
	})

	// Page to show details and live metrics for an ETL job.
	app.Get("/etls/:id", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		detailsHTML := fmt.Sprintf(`
			<h1>ETL %s Details</h1>
			<p>Status: %s, Createad At: %s</p>
			<h2>Metrics (updates every 2 seconds)</h2>
			<p>Current number of workers</p>
			<form action="/etls/%s/adjust-worker" method="post">
				<input type="number" name="worker_count" value="%d"/>
				<input type="submit"/>
			</form>
			<pre id="metrics"></pre>
			<script>
				async function fetchMetrics() {
					const res = await fetch("/etls/%s/metrics");
					const data = await res.json();
					document.getElementById("metrics").innerText = JSON.stringify(data, null, 2);
				}
				fetchMetrics();
				setInterval(fetchMetrics, 2000);
			</script>
		`, id, etl.Status, etl.CreatedAt.Format(time.DateTime), id, etl.workerCount, id)
		return c.SendString(detailsHTML)
	})

	// API endpoint to get metrics JSON for an ETL job.
	app.Get("/etls/:id/metrics", func(c *fiber.Ctx) error {
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		return c.JSON(etl.GetMetrics())
	})

	log.Printf("Starting API server on %s", addr)
	return app.Listen(addr)
}
