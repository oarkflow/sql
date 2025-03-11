package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/pkg/config"
)

func main() {
	paths := []string{
		"assets/prod.yaml",
		// "assets/multiple-source.yaml",
		// "assets/std.yaml",
	}
	for _, path := range paths {
		fmt.Println("Started executing", path)
		err := RunETL(path)
		if err != nil {
			panic(err)
		}
		fmt.Println("Execution Completed\n", path)
	}
}

func RunETL(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Printf("Error loading config: %v", err)
		return err
	}
	return etl.Run(cfg)
}
