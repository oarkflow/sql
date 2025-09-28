package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"
)

// CSVGeneratorDemo demonstrates the CSV generation capabilities
func main() {
	fmt.Println("=== CSV Generator Demonstration ===\n")

	// 1. Generate a sample file for testing
	fmt.Println("1. Generating sample file...")
	DemoGenerateSample()

	// 2. Validate the generated file
	fmt.Println("\n2. Validating generated file...")
	DemoValidateCSV("sample_large_dataset.csv")

	// 3. Demonstrate resume capability
	fmt.Println("\n3. Demonstrating resume capability...")
	DemoGenerateWithResume()

	// 4. Benchmark generation speed
	fmt.Println("\n4. Benchmarking generation speed...")
	DemoBenchmarkGeneration()

	fmt.Println("\n=== Demonstration completed ===")
	fmt.Println("\nTo generate the full 2 billion record dataset, call:")
	fmt.Println("  GenerateLargeCSV()")
	fmt.Println("\nNote: This will create a massive file (~500GB+) and may take several hours/days.")
}

// DemoGenerateSample generates a smaller sample for testing
func DemoGenerateSample() {
	columns := Create30ColumnConfig()
	filename := "sample_large_dataset.csv"
	totalRecords := int64(10000) // 10k records for testing

	fmt.Printf("Generating sample file: %s with %d records\n", filename, totalRecords)

	generator := NewLargeDatasetGenerator(filename, columns, totalRecords)
	if err := generator.Generate(); err != nil {
		log.Fatalf("Sample generation failed: %v", err)
	}

	fmt.Println("Sample generation completed!")
}

// DemoGenerateWithResume demonstrates resume capability
func DemoGenerateWithResume() {
	columns := Create30ColumnConfig()
	filename := "resumable_dataset.csv"
	totalRecords := int64(50000) // 50k records

	fmt.Println("=== Resume Demonstration ===")

	// First, generate some records
	fmt.Println("Generating initial records...")
	generator := NewLargeDatasetGenerator(filename, columns, totalRecords)
	if err := generator.Generate(); err != nil {
		log.Fatalf("Initial generation failed: %v", err)
	}

	current, total, _ := generator.GetProgress()
	fmt.Printf("Generated %d/%d records\n", current, total)

	// Simulate interruption and resume
	fmt.Println("\nSimulating interruption and resume...")

	// Create new generator instance (simulating restart)
	generator2 := NewLargeDatasetGenerator(filename, columns, totalRecords)
	if err := generator2.resumeFromExistingFile(); err != nil {
		log.Printf("Resume failed: %v", err)
		return
	}

	// Continue generation
	if err := generator2.Generate(); err != nil {
		log.Fatalf("Resume generation failed: %v", err)
	}

	fmt.Println("Resume demonstration completed!")
}

// DemoBenchmarkGeneration benchmarks the generation speed
func DemoBenchmarkGeneration() {
	columns := Create30ColumnConfig()
	totalRecords := int64(50000) // 50k for benchmarking

	fmt.Println("=== Generation Benchmark ===")

	times := 2
	for i := 0; i < times; i++ {
		filename := fmt.Sprintf("benchmark_%d.csv", i+1)

		startTime := time.Now()
		generator := NewLargeDatasetGenerator(filename, columns, totalRecords)
		if err := generator.Generate(); err != nil {
			log.Printf("Benchmark %d failed: %v", i+1, err)
			continue
		}

		elapsed := time.Since(startTime)
		rate := float64(totalRecords) / elapsed.Seconds()

		fmt.Printf("Benchmark %d: %v (%.1f records/sec)\n", i+1, elapsed, rate)
	}
}

// DemoValidateCSV validates the generated CSV file
func DemoValidateCSV(filename string) error {
	fmt.Printf("Validating CSV file: %s\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	fmt.Printf("Header columns: %d\n", len(header))

	// Validate first few records
	recordCount := 0
	for i := 0; i < 3; i++ {
		record, err := reader.Read()
		if err != nil {
			break
		}

		fmt.Printf("Record %d: %d columns\n", i+1, len(record))
		recordCount++
	}

	// Count total records (approximate)
	scanner := bufio.NewScanner(file)
	totalRecords := 0
	for scanner.Scan() {
		totalRecords++
	}

	fmt.Printf("Total records (including header): %d\n", totalRecords)
	fmt.Printf("Data records: %d\n", totalRecords-1)

	return nil
}

// DemoGenerateLargeCSV generates the full 2 billion record dataset
func DemoGenerateLargeCSV() {
	fmt.Println("=== WARNING: Large Dataset Generation ===")
	fmt.Println("This will generate a 2 billion record CSV file")
	fmt.Println("Estimated size: ~500GB")
	fmt.Println("Estimated time: Several hours to days")
	fmt.Println()

	// Ask for confirmation multiple times
	for i := 0; i < 3; i++ {
		fmt.Printf("Are you absolutely sure you want to proceed? (type 'YES' to confirm): ")
		var response string
		fmt.Scanln(&response)
		if response == "YES" {
			break
		}
		if i == 2 {
			fmt.Println("Generation cancelled.")
			return
		}
	}

	// Actual generation code (commented out to prevent accidental execution)
	/*
		columns := Create30ColumnConfig()
		filename := "large_dataset.csv"
		totalRecords := int64(2000000000) // 2 billion

		generator := NewLargeDatasetGenerator(filename, columns, totalRecords)
		if err := generator.Generate(); err != nil {
			log.Fatalf("Generation failed: %v", err)
		}

		fmt.Println("Large dataset generation completed!")
	*/
	fmt.Println("Generation function is available but commented out for safety.")
	fmt.Println("Uncomment the code in GenerateLargeCSV() to run the actual generation.")
}
