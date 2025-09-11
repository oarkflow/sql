package main

import (
	"context"
	"log"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

func main() {
	etlCfg, err := config.Load("config.bcl")
	if err != nil {
		log.Fatalf("Config load error: %v", err)
	}
	manager := etl.NewManager()
	ids, err := manager.Prepare(etlCfg)
	if err != nil {
		log.Fatalf("Prepare error: %v", err)
	}
	for _, id := range ids {
		if err := manager.Start(context.Background(), id); err != nil {
			log.Fatalf("ETL run error: %v", err)
		}
	}
}

func mai3n() {
	_, fields, err := utils.DetectSchema("facilities", "facilities.csv", "postgres", true)
	if err != nil {
		panic(err)
	}
	schema := make(map[string]string)
	for _, v := range fields {
		schema[v.Name] = v.DataType
	}
	opts := []etl.Option{
		etl.WithSource("csv", nil, "facilities.csv", "", "", "csv"),
		etl.WithDestination(config.DataConfig{
			Type:     "postgresql",
			Driver:   "postgres",
			Host:     "localhost",
			Username: "postgres",
			Password: "postgres",
			Port:     5432,
			Database: "bi",
		}, nil, config.TableMapping{
			OldName:         "sample",
			NewName:         "sample",
			CloneSource:     true,
			Migrate:         true,
			AutoCreateTable: true,
			NormalizeSchema: schema,
			Mapping: map[string]string{
				"id":            "id",
				"facility_name": "facility_name",
				"logo":          "logo",
				"contact_email": "contact_email",
			},
		}),
	}
	instance := etl.NewETL("my_etl", "My ETL Process", opts...)
	err = instance.Run(context.Background())
	if err != nil {
		panic(err)
	}
}
