package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/sql/v1/config"
	"github.com/oarkflow/sql/v1/contracts"
	"github.com/oarkflow/sql/v1/loaders"
	"github.com/oarkflow/sql/v1/mappers"
	"github.com/oarkflow/sql/v1/sources"
	"github.com/oarkflow/sql/v1/transformers"
)

func openDB(cfg config.DataConfig) (*sql.DB, error) {
	var dsn string
	if cfg.Driver == "mysql" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	} else if cfg.Driver == "postgres" {
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)
	} else {
		return nil, fmt.Errorf("unsupported driver: %s", cfg.Driver)
	}
	db, err := sql.Open(cfg.Driver, dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createTableFromCSV(destDB *sql.DB, csvFileName string, tableName string) error {
	f, err := os.Open(csvFileName)
	if err != nil {
		return fmt.Errorf("opening CSV file: %w", err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header: %w", err)
	}
	columns := []string{}
	for _, col := range headers {
		col = strings.TrimSpace(col)
		columns = append(columns, fmt.Sprintf("%s TEXT", col))
	}
	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))
	_, err = destDB.Exec(createQuery)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	log.Printf("Table %s created (if not exists) with columns: %v", tableName, headers)
	return nil
}

type ETL struct {
	sources       []contracts.Source
	loaders       []contracts.Loader
	mappers       []contracts.Mapper
	transformers  []contracts.Transformer
	validators    []contracts.Validator
	workerCount   int
	batchSize     int
	rawChanBuffer int
}

func (e *ETL) Run(ctx context.Context) error {
	rawChan := make(chan contracts.Record, e.rawChanBuffer)
	var srcWG sync.WaitGroup
	for _, s := range e.sources {
		if err := s.Setup(ctx); err != nil {
			return err
		}
		srcWG.Add(1)
		go func(src contracts.Source) {
			defer srcWG.Done()
			ch, err := src.Extract(ctx)
			if err != nil {
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
	processedChan := make(chan contracts.Record, e.workerCount*2)
	var procWG sync.WaitGroup
	for i := 0; i < e.workerCount; i++ {
		procWG.Add(1)
		go func(workerID int) {
			defer procWG.Done()
			for raw := range rawChan {
				rec := raw
				for _, mapper := range e.mappers {
					var err error
					rec, err = mapper.Map(ctx, rec)
					if err != nil {
						log.Printf("[Worker %d] Mapper (%s) error: %v", workerID, mapper.Name(), err)
						rec = nil
						break
					}
				}
				if rec == nil {
					continue
				}
				for _, transformer := range e.transformers {
					var err error
					rec, err = transformer.Transform(ctx, rec)
					if err != nil {
						log.Printf("[Worker %d] Transformer error: %v", workerID, err)
						rec = nil
						break
					}
				}
				if rec != nil {
					processedChan <- rec
				}
			}
		}(i)
	}
	go func() {
		procWG.Wait()
		close(processedChan)
	}()
	batchChan := make(chan []contracts.Record, 10)
	var batchWG sync.WaitGroup
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		batch := make([]contracts.Record, 0, e.batchSize)
		for rec := range processedChan {
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				batchChan <- batch
				batch = make([]contracts.Record, 0, e.batchSize)
			}
		}
		if len(batch) > 0 {
			batchChan <- batch
		}
		close(batchChan)
	}()
	var loaderWG sync.WaitGroup
	for i, loader := range e.loaders {
		loaderWG.Add(1)
		go func(loader contracts.Loader, workerID int) {
			defer loaderWG.Done()
			for batch := range batchChan {
				if err := loader.Setup(ctx); err != nil {
					log.Printf("[Loader Worker %d] Loader setup error: %v", workerID, err)
					continue
				}
				if err := loader.LoadBatch(ctx, batch); err != nil {
					log.Printf("[Loader Worker %d] LoadBatch error: %v", workerID, err)
					continue
				}
			}
		}(loader, i)
	}
	batchWG.Wait()
	loaderWG.Wait()
	return nil
}

func (e *ETL) Close() error {
	for _, s := range e.sources {
		if err := s.Close(); err != nil {
			return err
		}
	}
	for _, l := range e.loaders {
		if err := l.Close(); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <config.yaml>", os.Args[0])
	}
	configPath := os.Args[1]
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	var sourceDB *sql.DB
	var destDB *sql.DB
	if cfg.Source.Type == "mysql" || cfg.Source.Type == "postgresql" {
		sourceDB, err = openDB(cfg.Source)
		if err != nil {
			log.Fatalf("Error connecting to source DB: %v", err)
		}
		defer sourceDB.Close()
	}
	if cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql" {
		destDB, err = openDB(cfg.Destination)
		if err != nil {
			log.Fatalf("Error connecting to destination DB: %v", err)
		}
		defer destDB.Close()
	}
	for _, tableCfg := range cfg.Tables {
		if !tableCfg.Migrate {
			continue
		}
		log.Printf("Starting migration: %s -> %s", tableCfg.OldName, tableCfg.NewName)

		if cfg.Destination.Type == "postgresql" && tableCfg.AutoCreateTable {
			csvFileName := tableCfg.OldName
			if csvFileName == "" {
				csvFileName = cfg.Source.File
			}
			if err := createTableFromCSV(destDB, csvFileName, tableCfg.NewName); err != nil {
				log.Fatalf("Error creating table %s: %v", tableCfg.NewName, err)
			}
		}
		var src contracts.Source
		switch cfg.Source.Type {
		case "mysql", "postgresql":
			src = sources.NewSQLSource(sourceDB, tableCfg.OldName, tableCfg.Query)
		case "csv":
			fileName := tableCfg.OldName
			if fileName == "" {
				fileName = cfg.Source.File
			}
			src = sources.NewCSVSource(fileName)
		case "json":
			fileName := tableCfg.OldName
			if fileName == "" {
				fileName = cfg.Source.File
			}
			src = sources.NewJSONSource(fileName)
		default:
			log.Fatalf("Unsupported source type: %s", cfg.Source.Type)
		}
		var loader contracts.Loader
		switch cfg.Destination.Type {
		case "mysql", "postgresql":

			if tableCfg.KeyValueTable {
				loader = loaders.NewKeyValueLoader(destDB, tableCfg.NewName, tableCfg.KeyField, tableCfg.ValueField,
					tableCfg.ExtraValues, tableCfg.IncludeFields, tableCfg.ExcludeFields, tableCfg.TruncateDestination)
			} else {
				loader = loaders.NewSQLLoader(destDB, tableCfg.NewName, tableCfg.TruncateDestination)
			}
		case "csv":
			fileName := tableCfg.NewName
			if fileName == "" {
				fileName = cfg.Destination.File
			}
			loader = loaders.NewCSVLoader(fileName)
		case "json":
			fileName := tableCfg.NewName
			if fileName == "" {
				fileName = cfg.Destination.File
			}
			loader = loaders.NewJSONLoader(fileName)
		default:
			log.Fatalf("Unsupported destination type: %s", cfg.Destination.Type)
		}
		var mpers []contracts.Mapper
		if len(tableCfg.Mapping) > 0 {
			mpers = append(mpers, mappers.NewFieldMapper(tableCfg.Mapping))
		}

		mpers = append(mpers, &mappers.LowercaseMapper{})

		trnsformers := []contracts.Transformer{
			&transformers.LookupTransformer{LookupData: map[string]string{"key1": "value1"}, Field: "some_field", TargetField: "lookup_result"},
		}
		etlJob := &ETL{
			sources:       []contracts.Source{src},
			loaders:       []contracts.Loader{loader},
			mappers:       mpers,
			transformers:  trnsformers,
			validators:    []contracts.Validator{},
			workerCount:   2,
			batchSize:     tableCfg.BatchSize,
			rawChanBuffer: 50,
		}
		ctx := context.Background()
		if err := etlJob.Run(ctx); err != nil {
			log.Printf("ETL job for %s failed: %v", tableCfg.OldName, err)
			if !tableCfg.SkipStoreError {
				continue
			}
		}
		if err := etlJob.Close(); err != nil {
			log.Printf("Error closing ETL job for %s: %v", tableCfg.OldName, err)
		}

		if (cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql") && tableCfg.UpdateSequence {
			seqName := fmt.Sprintf("%s_seq", tableCfg.NewName)
			q := fmt.Sprintf("SELECT setval('%s', (SELECT MAX(id) FROM %s))", seqName, tableCfg.NewName)
			if _, err := destDB.ExecContext(ctx, q); err != nil {
				log.Printf("Error updating sequence %s: %v", seqName, err)
			} else {
				log.Printf("Sequence %s updated", seqName)
			}
		}
		log.Printf("Migration for %s complete", tableCfg.OldName)
	}
	log.Println("All migrations complete.")
}
