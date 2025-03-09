package main

import (
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/config"
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
	etl.Run(cfg)
}
