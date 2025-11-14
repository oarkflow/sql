package main

import (
	"context"
	"log"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/bridglink"
	"github.com/oarkflow/sql/pkg/config"
)

func main() {
	cfg, err := config.LoadBridgLinkConfig("examples/bridglink_hl7_to_json.yaml")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	manager, err := bridglink.NewManager(cfg, etl.NewManager())
	if err != nil {
		log.Fatalf("new manager: %v", err)
	}
	if _, err := manager.RunPipeline(context.Background(), "hl7_to_json"); err != nil {
		log.Fatalf("run pipeline: %v", err)
	}
}
