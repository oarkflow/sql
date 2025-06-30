package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
)

func mai2n() {
	manager := etl.NewManager()
	manager.Serve(":8081")
}

func main() {
	paths := []string{
		"assets/user.bcl",
	}
	for _, path := range paths {
		fmt.Println("Started executing", path)
		err := runETL(path)
		if err != nil {
			panic(err)
		}
		fmt.Println("Execution Completed\n", path)
	}
}

func runETL(configPath string) error {
	cfg, err := config.LoadBCL(configPath)
	if err != nil {
		log.Printf("Error loading config: %v", err)
		return err
	}
	opts := []etl.Option{
		etl.WithDashboardAuth("admin", "password"),
	}
	manager := etl.NewManager()
	ids, err := manager.Prepare(cfg, opts...)
	if err != nil {
		return err
	}
	for _, id := range ids {
		go func(manager *etl.Manager) {
			time.Sleep(5 * time.Second)
			manager.AdjustWorker(1, id)
		}(manager)
		if err := manager.Start(context.Background(), id); err != nil {
			return err
		}
	}
	return nil
}
