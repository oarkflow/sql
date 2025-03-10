package main

import (
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/config"
)

func main() {
	paths := []string{
		"assets/aggregator.yaml",
		// "assets/multiple-source.yaml",
		// "assets/std.yaml",
	}
	for _, path := range paths {
		fmt.Println("Started executing", path)
		RunETL(path)
		fmt.Println("Execution Completed\n", path)
	}
}

func RunETL(configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	etl.Run(cfg)
}
