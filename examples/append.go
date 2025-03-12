package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/etl/pkg/utils/fileutil"
)

func main() {
	// Create or open the JSON appender
	appender, err := fileutil.NewJSONAppender("data.json")
	if err != nil {
		log.Fatalf("Error initializing JSONAppender: %v", err)
	}
	defer func() {
		if err := appender.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

	// Define a batch of elements to append
	elements := []any{
		map[string]any{"id": 1, "name": "Alice"},
		map[string]any{"id": 2, "name": "Bob"},
		map[string]any{"id": 3, "name": "Charlie"},
	}

	// Append the batch to the JSON file
	if err := appender.AppendBatch(elements); err != nil {
		log.Fatalf("Error appending batch: %v", err)
	}

	fmt.Println("Batch appended successfully!")
}
