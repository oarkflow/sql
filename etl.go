package main

import (
	"log"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/etl/loader"
	"github.com/oarkflow/sql/etl/mapper"
	"github.com/oarkflow/sql/etl/source"
	"github.com/oarkflow/sql/etl/transfomer"
)

func main() {
	fileSource := source.NewFileSource("data.json")
	httpSource := source.NewHTTPSource("https://jsonplaceholder.typicode.com/users")
	etlHandler := etl.NewETL(
		etl.WithSources(fileSource, httpSource),
		etl.WithMappers(&mapper.ExampleMapper{}, &mapper.AddTimestampMapper{}),
		etl.WithTransformers(&transfomer.BasicValidator{}, &transfomer.UppercaseTransformer{}),
		etl.WithLoaders(&loader.ConsoleLoader{}, &loader.VerboseConsoleLoader{}),
	)
	if err := etlHandler.Run(); err != nil {
		log.Fatalf("ETL process failed: %v", err)
	}
	etlHandler.Close()
}
