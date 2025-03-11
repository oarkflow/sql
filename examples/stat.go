package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/etl/report"
)

func someFunction() {
	time.Sleep(2 * time.Second)
}

func main() {
	start := report.Start()
	someFunction()
	since := report.Since(start)
	fmt.Println("\nUsage since the starting snapshot:")
	fmt.Println(since.ToString())
}
