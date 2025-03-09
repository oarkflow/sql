package main

import (
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/sql/etl/config"
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
	v1.Run(cfg)
}
