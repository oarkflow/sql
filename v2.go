package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/utils"
	v1 "github.com/oarkflow/sql/v1"
)

func main() {
	paths := []string{
		"config.yaml",
	}
	for _, path := range paths {
		RunETL(path)
	}
}

func RunETL(configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	var sourceDB *sql.DB
	var destDB *sql.DB
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
	for _, tableCfg := range cfg.Tables {
		if utils.IsSQLType(cfg.Destination.Type) && !tableCfg.Migrate {
			continue
		}
		log.Printf("Starting migration: %s -> %s", tableCfg.OldName, tableCfg.NewName)
		if utils.IsSQLType(cfg.Destination.Type) && tableCfg.AutoCreateTable && tableCfg.KeyValueTable {
			if err := v1.CreateKeyValueTable(destDB, tableCfg.NewName, tableCfg); err != nil {
				log.Fatalf("Error creating key-value table %s: %v", tableCfg.NewName, err)
			}
		}
		opts := []v1.Option{
			v1.WithSource(cfg.Source.Type, sourceDB, cfg.Source.File, tableCfg.OldName, tableCfg.Query),
			v1.WithDestination(cfg.Destination.Type, destDB, cfg.Destination.File, tableCfg),
			v1.WithCheckpoint(v1.NewFileCheckpointStore("checkpoint.txt"), func(rec utils.Record) string {
				if name, ok := rec["name"].(string); ok {
					return name
				}
				return ""
			}),
			v1.WithMapping(tableCfg.Mapping),
			v1.WithTransformers(),
			v1.WithWorkerCount(2),
			v1.WithBatchSize(tableCfg.BatchSize),
			v1.WithRawChanBuffer(50),
		}
		if tableCfg.KeyValueTable {
			opts = append(opts, v1.WithKeyValueTransformer(
				tableCfg.ExtraValues,
				tableCfg.IncludeFields,
				tableCfg.ExcludeFields,
				tableCfg.KeyField,
				tableCfg.ValueField,
			))
		}
		etlJob := v1.NewETL(opts...)
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
