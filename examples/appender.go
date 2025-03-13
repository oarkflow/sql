package main

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/etl/pkg/utils/fileutil"
)

func main() {
	// Create or open the JSON appender
	appender, err := fileutil.NewAppender[any]("data.csv", "csv", true)
	if err != nil {
		log.Fatalf("Error initializing JSONAppender: %v", err)
	}
	defer func() {
		if err := appender.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()
	start := time.Now()
	for i := 0; i < 1000000; i++ {
		if err := appender.Append(map[string]any{
			"id":  i + 1,
			"msg": "Alice",
		}); err != nil {
			log.Fatalf("Error appending batch: %v", err)
		}
	}
	fmt.Println(fmt.Sprintf("%s", time.Since(start)))

	fmt.Println("Batch appended successfully!")
}
