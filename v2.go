package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	v1 "github.com/oarkflow/sql/v1"
	"github.com/oarkflow/sql/v1/config"
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
		sourceDB, err = v1.OpenDB(cfg.Source)
		if err != nil {
			log.Fatalf("Error connecting to source DB: %v", err)
		}
		defer sourceDB.Close()
	}
	if cfg.Destination.Type == "mysql" || cfg.Destination.Type == "postgresql" {
		destDB, err = v1.OpenDB(cfg.Destination)
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
			if err := v1.CreateTableFromCSV(destDB, csvFileName, tableCfg.NewName); err != nil {
				log.Fatalf("Error creating table %s: %v", tableCfg.NewName, err)
			}
		}
		etlJob := v1.NewETL(
			v1.WithSource(cfg.Source.Type, sourceDB, cfg.Source.File, tableCfg.OldName, tableCfg.Query),
			v1.WithDestination(cfg.Destination.Type, destDB, cfg.Destination.File, tableCfg),
			v1.WithMapping(tableCfg.Mapping),
			v1.WithTransformers(),
			v1.WithWorkerCount(2),
			v1.WithBatchSize(tableCfg.BatchSize),
			v1.WithRawChanBuffer(50),
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
