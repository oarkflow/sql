package main

import (
	"fmt"

	"github.com/oarkflow/etl/pkg/utils/fileutil"
)

func main() {
	appender, err := fileutil.NewJSONAppender("data.json")
	if err != nil {
		fmt.Println("Error creating appender:", err)
		return
	}
	defer func() {
		_ = appender.Close()
	}()
	element := map[string]any{
		"id":   1,
		"name": "Test Element",
	}
	if err := appender.Append(element); err != nil {
		fmt.Println("Error appending element:", err)
	} else {
		fmt.Println("Element appended successfully")
	}
}
