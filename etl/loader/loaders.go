package loader

import (
	"encoding/json"
	"log"

	"github.com/oarkflow/sql/utils"
)

type ConsoleLoader struct{}

func (l *ConsoleLoader) Close() error {
	return nil
}

func (l *ConsoleLoader) Setup() error {
	return nil
}

func (l *ConsoleLoader) LoadBatch(records []utils.Record) error {
	log.Printf("ConsoleLoader: Loading batch of %d records", len(records))
	for _, rec := range records {
		log.Printf("utils.Record: %v", rec)
	}
	return nil
}

type VerboseConsoleLoader struct{}

func (l *VerboseConsoleLoader) LoadBatch(records []utils.Record) error {
	log.Printf("VerboseConsoleLoader: Processing batch with %d records", len(records))
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	log.Println(string(data))
	return nil
}

func (l *VerboseConsoleLoader) Close() error {
	return nil
}

func (l *VerboseConsoleLoader) Setup() error {
	return nil
}
