package main

import (
	"log"

	"github.com/chand1012/sq/etl"
	"github.com/chand1012/sq/etl/loader"
	"github.com/chand1012/sq/etl/mapper"
	"github.com/chand1012/sq/etl/source"
	"github.com/chand1012/sq/etl/transfomer"
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
