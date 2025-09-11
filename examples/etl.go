package main

import (
	"context"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/connection"
)

func main() {
	cfg := squealx.Config{
		Driver:   "postgres",
		Host:     "localhost",
		Username: "postgres",
		Password: "postgres",
		Port:     5432,
		Database: "bi",
	}
	db, _, err := connection.FromConfig(cfg)
	if err != nil {
		panic(err)
	}
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
		}, db, config.TableMapping{
			OldName:         "sample",
			NewName:         "sample",
			CloneSource:     true,
			BatchSize:       100,
			EnableBatch:     true,
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
