package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/checkpoints"
	"github.com/oarkflow/sql/etl/config"
	"github.com/oarkflow/sql/utils"
)

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
		sourceDB, err = etl.OpenDB(cfg.Source)
		if err != nil {
			log.Fatalf("Error connecting to source DB: %v", err)
		}
		defer sourceDB.Close()
	}
	if cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql" {
		destDB, err = etl.OpenDB(cfg.Destination)
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
			if err := etl.CreateTableFromCSV(destDB, csvFileName, tableCfg.NewName); err != nil {
				log.Fatalf("Error creating table %s: %v", tableCfg.NewName, err)
			}
		}
		etlJob := etl.NewETL(
			etl.WithSource(cfg.Source.Type, sourceDB, cfg.Source.File, tableCfg.OldName, tableCfg.Query),
			etl.WithDestination(cfg.Destination.Type, destDB, cfg.Destination.File, tableCfg),
			etl.WithCheckpoint(checkpoints.NewFileCheckpointStore("checkpoint.txt"), func(rec utils.Record) string {
				if name, ok := rec["name"].(string); ok {
					return name
				}
				return ""
			}),
			etl.WithMapping(tableCfg.Mapping),
			etl.WithTransformers(),
			etl.WithWorkerCount(2),
			etl.WithBatchSize(tableCfg.BatchSize),
			etl.WithRawChanBuffer(50),
		)

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
