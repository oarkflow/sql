package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// ColumnConfig defines the configuration for a CSV column
type ColumnConfig struct {
	Name     string
	Type     string  // string, int, float, date, bool, name, email, phone, address, uuid
	Min      float64 // for numeric types
	Max      float64 // for numeric types
	Format   string  // for date formatting
	Required bool
}

// LargeDatasetGenerator generates massive CSV files efficiently
type LargeDatasetGenerator struct {
	filename       string
	columns        []ColumnConfig
	totalRecords   int64
	batchSize      int
	writer         *csv.Writer
	file           *os.File
	bufferedWriter *bufio.Writer
	currentRecord  int64
	startRecord    int64
}

// NewLargeDatasetGenerator creates a new CSV generator
func NewLargeDatasetGenerator(filename string, columns []ColumnConfig, totalRecords int64) *LargeDatasetGenerator {
	return &LargeDatasetGenerator{
		filename:      filename,
		columns:       columns,
		totalRecords:  totalRecords,
		batchSize:     10000, // Write in batches for efficiency
		currentRecord: 0,
		startRecord:   0,
	}
}

// Initialize sets up the generator for writing
func (g *LargeDatasetGenerator) Initialize() error {
	// Check if file exists to support resuming
	if _, err := os.Stat(g.filename); err == nil {
		// File exists, check if we should resume
		fmt.Printf("File %s already exists. Resume generation? (y/n): ", g.filename)
		var response string
		fmt.Scanln(&response)
		if response == "y" || response == "Y" {
			return g.resumeFromExistingFile()
		} else {
			return fmt.Errorf("file already exists, choose different filename or remove existing file")
		}
	}

	// Create new file
	file, err := os.Create(g.filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	g.file = file
	g.bufferedWriter = bufio.NewWriterSize(file, 64*1024*1024) // 64MB buffer
	g.writer = csv.NewWriter(g.bufferedWriter)

	// Write header
	header := make([]string, len(g.columns))
	for i, col := range g.columns {
		header[i] = col.Name
	}

	if err := g.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	g.writer.Flush()
	return g.bufferedWriter.Flush()
}

// resumeFromExistingFile attempts to resume from an existing file
func (g *LargeDatasetGenerator) resumeFromExistingFile() error {
	file, err := os.OpenFile(g.filename, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open existing file: %w", err)
	}

	g.file = file
	g.bufferedWriter = bufio.NewWriterSize(file, 64*1024*1024)
	g.writer = csv.NewWriter(g.bufferedWriter)

	// Count existing records (approximate)
	g.currentRecord = g.countExistingRecords()
	g.startRecord = g.currentRecord

	log.Printf("Resuming from record %d", g.currentRecord)
	return nil
}

// countExistingRecords counts records in existing file
func (g *LargeDatasetGenerator) countExistingRecords() int64 {
	scanner := bufio.NewScanner(g.file)

	// Skip header (first line)
	if scanner.Scan() {
		count := int64(0)
		for scanner.Scan() {
			count++
			if count%1000000 == 0 {
				log.Printf("Counted %d records...", count)
			}
		}
		return count
	}
	return 0
}

// GenerateRecord generates a single record
func (g *LargeDatasetGenerator) GenerateRecord(recordNum int64) []string {
	record := make([]string, len(g.columns))

	for i, col := range g.columns {
		record[i] = g.generateColumnValue(col, recordNum, i)
	}

	return record
}

// generateColumnValue generates a value for a specific column
func (g *LargeDatasetGenerator) generateColumnValue(col ColumnConfig, recordNum int64, colIndex int) string {
	switch col.Type {
	case "id", "int":
		if col.Min == 0 && col.Max == 0 {
			return strconv.FormatInt(recordNum+g.startRecord+1, 10)
		}
		return strconv.FormatInt(int64(col.Min)+recordNum%int64(col.Max-col.Min), 10)

	case "float":
		rangeSize := col.Max - col.Min
		value := col.Min + float64(recordNum%int64(rangeSize))
		return strconv.FormatFloat(value, 'f', 2, 64)

	case "date":
		baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		days := recordNum % 3650 // 10 years of data
		date := baseDate.AddDate(0, 0, int(days))
		if col.Format != "" {
			return date.Format(col.Format)
		}
		return date.Format("2006-01-02 15:04:05")

	case "bool":
		return strconv.FormatBool(recordNum%2 == 0)

	case "name":
		firstNames := []string{"John", "Jane", "Michael", "Sarah", "David", "Emma", "James", "Lisa", "Robert", "Mary"}
		lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
		first := firstNames[recordNum%int64(len(firstNames))]
		last := lastNames[recordNum%int64(len(lastNames))]
		return fmt.Sprintf("%s %s", first, last)

	case "email":
		domains := []string{"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com"}
		username := fmt.Sprintf("user%d", recordNum)
		domain := domains[recordNum%int64(len(domains))]
		return fmt.Sprintf("%s@%s", username, domain)

	case "phone":
		countryCode := 1
		areaCode := 100 + (recordNum % 900)
		exchange := 200 + (recordNum % 800)
		number := 1000 + (recordNum % 9000)
		return fmt.Sprintf("+%d-%03d-%03d-%04d", countryCode, areaCode, exchange, number)

	case "address":
		streetNumbers := []string{"123", "456", "789", "321", "654", "987"}
		streetNames := []string{"Main St", "Oak Ave", "Elm Dr", "Pine Rd", "Cedar Ln", "Maple Way"}
		cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"}
		states := []string{"NY", "CA", "IL", "TX", "AZ", "PA"}

		number := streetNumbers[recordNum%int64(len(streetNumbers))]
		street := streetNames[recordNum%int64(len(streetNames))]
		city := cities[recordNum%int64(len(cities))]
		state := states[recordNum%int64(len(states))]

		return fmt.Sprintf("%s %s, %s, %s", number, street, city, state)

	case "uuid":
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			uint32(recordNum), uint16(recordNum>>32), uint16(recordNum>>48),
			uint16(recordNum>>64), recordNum>>80)

	case "category":
		categories := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
		return categories[recordNum%int64(len(categories))]

	case "status":
		statuses := []string{"active", "inactive", "pending", "completed", "cancelled"}
		return statuses[recordNum%int64(len(statuses))]

	case "priority":
		priorities := []string{"low", "medium", "high", "critical"}
		return priorities[recordNum%int64(len(priorities))]

	case "percentage":
		return strconv.FormatFloat(float64(recordNum%100), 'f', 2, 64)

	case "currency":
		amount := float64(rand.Intn(1000000)) + float64(recordNum%10000)/100.0
		return strconv.FormatFloat(amount, 'f', 2, 64)

	case "text":
		length := int(col.Min) + (int(recordNum) % int(col.Max-col.Min))
		if length <= 0 {
			length = 50
		}
		return g.generateRandomText(length)

	case "url":
		domains := []string{"example.com", "test.org", "demo.net", "sample.io", "mock.com"}
		paths := []string{"/page", "/api/data", "/user/profile", "/product", "/category"}
		domain := domains[recordNum%int64(len(domains))]
		path := paths[recordNum%int64(len(paths))]
		return fmt.Sprintf("https://%s%s/%d", domain, path, recordNum%1000)

	default: // string
		return fmt.Sprintf("value_%d_%d", recordNum, colIndex)
	}
}

// generateRandomText generates random text of specified length
func (g *LargeDatasetGenerator) generateRandomText(length int) string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

// Generate generates the CSV file
func (g *LargeDatasetGenerator) Generate() error {
	if g.file == nil {
		if err := g.Initialize(); err != nil {
			return err
		}
	}

	defer g.Close()

	log.Printf("Starting generation of %d records (starting from %d)", g.totalRecords, g.currentRecord)

	startTime := time.Now()

	for g.currentRecord < g.totalRecords {
		// Generate batch of records
		batch := make([][]string, 0, g.batchSize)

		for i := 0; i < g.batchSize && g.currentRecord < g.totalRecords; i++ {
			record := g.GenerateRecord(g.currentRecord)
			batch = append(batch, record)
			g.currentRecord++
		}

		// Write batch
		if err := g.writer.WriteAll(batch); err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}

		// Periodic flush and progress report
		if g.currentRecord%100000 == 0 { // Every 100k records
			g.writer.Flush()
			g.bufferedWriter.Flush()

			now := time.Now()
			elapsed := now.Sub(startTime)
			rate := float64(g.currentRecord) / elapsed.Seconds()

			log.Printf("Progress: %d/%d records (%.1f%%) - Rate: %.1f records/sec - Elapsed: %v",
				g.currentRecord, g.totalRecords,
				float64(g.currentRecord)/float64(g.totalRecords)*100,
				rate, elapsed)

			// Estimate time remaining
			if rate > 0 {
				remaining := float64(g.totalRecords-g.currentRecord) / rate
				log.Printf("Estimated time remaining: %v", time.Duration(remaining)*time.Second)
			}
		}
	}

	// Final flush
	g.writer.Flush()
	return g.bufferedWriter.Flush()
}

// Close closes the generator and file handles
func (g *LargeDatasetGenerator) Close() error {
	if g.writer != nil {
		g.writer.Flush()
	}
	if g.bufferedWriter != nil {
		g.bufferedWriter.Flush()
	}
	if g.file != nil {
		return g.file.Close()
	}
	return nil
}

// GetProgress returns current progress
func (g *LargeDatasetGenerator) GetProgress() (current, total int64, percentage float64) {
	return g.currentRecord, g.totalRecords, float64(g.currentRecord) / float64(g.totalRecords) * 100
}

// EstimateFileSize estimates the size of the final CSV file
func (g *LargeDatasetGenerator) EstimateFileSize() int64 {
	// Rough estimation based on column types
	var avgRecordSize int64 = 0
	for _, col := range g.columns {
		switch col.Type {
		case "id", "int":
			avgRecordSize += 20 // Average ID length
		case "name":
			avgRecordSize += 20
		case "email":
			avgRecordSize += 25
		case "phone":
			avgRecordSize += 15
		case "address":
			avgRecordSize += 50
		case "uuid":
			avgRecordSize += 36
		case "text":
			avgRecordSize += int64(col.Min + (col.Max-col.Min)/2)
		case "date":
			avgRecordSize += 19
		case "float":
			avgRecordSize += 10
		case "bool":
			avgRecordSize += 5
		default:
			avgRecordSize += 20
		}
	}

	// Add CSV overhead (commas, quotes, newlines)
	avgRecordSize = int64(float64(avgRecordSize) * 1.2)

	// Add header size
	headerSize := int64(len(g.columns) * 20)

	return headerSize + (avgRecordSize * g.totalRecords)
}

// Create30ColumnConfig creates a configuration for 30 diverse columns
func Create30ColumnConfig() []ColumnConfig {
	return []ColumnConfig{
		{Name: "id", Type: "id", Required: true},
		{Name: "transaction_id", Type: "uuid", Required: true},
		{Name: "customer_name", Type: "name", Required: true},
		{Name: "email", Type: "email", Required: true},
		{Name: "phone", Type: "phone", Required: false},
		{Name: "address", Type: "address", Required: false},
		{Name: "age", Type: "int", Min: 18, Max: 80},
		{Name: "salary", Type: "float", Min: 30000, Max: 200000},
		{Name: "account_balance", Type: "currency", Required: false},
		{Name: "registration_date", Type: "date", Format: "2006-01-02"},
		{Name: "last_login", Type: "date", Format: "2006-01-02 15:04:05"},
		{Name: "is_active", Type: "bool"},
		{Name: "membership_level", Type: "category"},
		{Name: "status", Type: "status"},
		{Name: "priority", Type: "priority"},
		{Name: "completion_percentage", Type: "percentage"},
		{Name: "risk_score", Type: "float", Min: 0, Max: 100},
		{Name: "credit_score", Type: "int", Min: 300, Max: 850},
		{Name: "product_count", Type: "int", Min: 0, Max: 100},
		{Name: "total_orders", Type: "int", Min: 0, Max: 1000},
		{Name: "average_order_value", Type: "currency"},
		{Name: "website_url", Type: "url"},
		{Name: "company_size", Type: "category"},
		{Name: "industry", Type: "category"},
		{Name: "department", Type: "category"},
		{Name: "manager_name", Type: "name"},
		{Name: "start_date", Type: "date", Format: "2006-01-02"},
		{Name: "end_date", Type: "date", Format: "2006-01-02"},
		{Name: "notes", Type: "text", Min: 10, Max: 200},
		{Name: "tags", Type: "category"},
	}
}

func GenerateLargeCSV() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Create 30-column configuration
	columns := Create30ColumnConfig()

	// Configuration
	filename := "large_dataset.csv"
	totalRecords := int64(2000000000) // 2 billion records

	fmt.Printf("=== Large CSV Dataset Generator ===\n")
	fmt.Printf("Columns: %d\n", len(columns))
	fmt.Printf("Records: %d\n", totalRecords)
	fmt.Printf("Output file: %s\n", filename)

	// Estimate file size
	generator := NewLargeDatasetGenerator(filename, columns, totalRecords)
	estimatedSize := generator.EstimateFileSize()
	fmt.Printf("Estimated file size: %.2f GB\n", float64(estimatedSize)/(1024*1024*1024))

	// Confirm generation
	fmt.Print("\nThis will generate a very large file. Continue? (y/n): ")
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" {
		fmt.Println("Generation cancelled.")
		return
	}

	// Start generation
	startTime := time.Now()
	fmt.Println("\nStarting generation...")

	if err := generator.Generate(); err != nil {
		log.Fatalf("Generation failed: %v", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\nGeneration completed in %v\n", elapsed)

	// Final statistics
	current, total, percentage := generator.GetProgress()
	fmt.Printf("Final stats: %d/%d records (%.2f%%)\n", current, total, percentage)

	// File info
	if fileInfo, err := os.Stat(filename); err == nil {
		actualSize := fileInfo.Size()
		fmt.Printf("Actual file size: %.2f GB\n", float64(actualSize)/(1024*1024*1024))
		rate := float64(current) / elapsed.Seconds()
		fmt.Printf("Average generation rate: %.1f records/second\n", rate)
	}
}

// GenerateSample generates a smaller sample for testing
func GenerateSample() {
	columns := Create30ColumnConfig()
	filename := "sample_large_dataset.csv"
	totalRecords := int64(100000) // 100k records for testing

	fmt.Printf("Generating sample file: %s with %d records\n", filename, totalRecords)

	generator := NewLargeDatasetGenerator(filename, columns, totalRecords)
	if err := generator.Generate(); err != nil {
		log.Fatalf("Sample generation failed: %v", err)
	}

	fmt.Println("Sample generation completed!")
}

// GenerateWithResume demonstrates resume capability
func GenerateWithResume() {
	columns := Create30ColumnConfig()
	filename := "resumable_dataset.csv"
	totalRecords := int64(1000000) // 1M records

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

// BenchmarkGeneration benchmarks the generation speed
func BenchmarkGeneration() {
	columns := Create30ColumnConfig()
	totalRecords := int64(100000) // 100k for benchmarking

	fmt.Println("=== Generation Benchmark ===")

	times := 3
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

// ValidateCSV validates the generated CSV file
func ValidateCSV(filename string) error {
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
	for i := 0; i < 5; i++ {
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
