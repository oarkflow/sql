package main

import (
	"context"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
)

type NotificationConfig struct {
	OnSuccess []ChannelConfig `yaml:"on_success"`
	OnFailure []ChannelConfig `yaml:"on_failure"`
}

type ChannelConfig struct {
	Type       string   `yaml:"type"`
	Recipients []string `yaml:"recipients"`
	Subject    string   `yaml:"subject"`
	URL        string   `yaml:"url"`
	Method     string   `yaml:"method"`
}

type ExtendedConfig struct {
	config.Config `yaml:",inline"`
	Notifications NotificationConfig `yaml:"notifications"`
}

func main() {
	// 1. Read config file
	configFile := "medical_coding_config.yaml"
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfg ExtendedConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// 2. Create sample CSV if it doesn't exist
	if _, err := os.Stat(cfg.Source.File); os.IsNotExist(err) {
		createSampleCSV(cfg.Source.File)
	}

	// 3. Create ETL Manager
	manager := etl.NewManager()

	// 4. Prepare ETL jobs from config
	ids, err := manager.Prepare(&cfg.Config)
	if err != nil {
		log.Fatalf("Failed to prepare ETL: %v", err)
		sendNotifications(cfg.Notifications.OnFailure, "FAILURE", err.Error())
		return
	}

	log.Printf("Prepared %d ETL jobs: %v", len(ids), ids)

	// 5. Execute all jobs
	ctx := context.Background()
	var lastError error
	successCount := 0
	totalRecords := int64(0)

	for _, id := range ids {
		log.Printf("Starting ETL job: %s", id)
		if err := manager.Start(ctx, id); err != nil {
			log.Printf("ETL job %s failed: %v", id, err)
			lastError = err
			// Continue with other jobs even if one fails
		} else {
			successCount++
			if etl, ok := manager.GetETL(id); ok {
				totalRecords += etl.GetMetrics().Loaded
			}
		}
	}

	log.Printf("\n=== ETL Pipeline Summary ===")
	log.Printf("Total Jobs: %d", len(ids))
	log.Printf("Successful: %d", successCount)
	log.Printf("Failed: %d", len(ids)-successCount)
	log.Printf("Total Records Loaded: %d", totalRecords)
	log.Printf("===========================\n")

	// 6. Send notifications
	if lastError != nil {
		log.Println("ETL Pipeline completed with errors")
		sendNotifications(cfg.Notifications.OnFailure, "FAILURE", lastError.Error())
	} else {
		log.Println("ETL Pipeline completed successfully")
		sendNotifications(cfg.Notifications.OnSuccess, "SUCCESS", "All jobs completed successfully")
	}
}

func sendNotifications(channels []ChannelConfig, eventType string, details string) {
	if len(channels) == 0 {
		return
	}
	log.Printf("--- Processing %s Notifications ---", eventType)
	for _, ch := range channels {
		switch strings.ToLower(ch.Type) {
		case "email":
			// In a real implementation, use github.com/oarkflow/mail
			log.Printf("[Email] To: %v | Subject: %s | Body: %s", ch.Recipients, ch.Subject, details)
		case "webhook":
			// In a real implementation, make HTTP request
			log.Printf("[Webhook] %s %s | Payload: %s", ch.Method, ch.URL, details)
		default:
			log.Printf("[Unknown Channel] %s", ch.Type)
		}
	}
	log.Printf("-------------------------------------")
}

func createSampleCSV(filename string) {
	// CSV header: 22 columns
	header := []string{
		"encounter_id", "patient_id", "service_date", "cdi_code", "cdi_type", "pro_info",
		"cpt_code", "cpt_pro_code", "units", "amount", "em_fac_code", "em_fac_level",
		"em_pro_code", "em_pro_level", "hcpcs_fac_code", "hcpcs_pro_code", "dx_fac_code",
		"dx_pro_code", "pqri_code", "pqrs_code", "special_fac_code", "special_pro_code",
	}

	// Sample data rows - each slice must have exactly 22 elements
	rows := [][]string{
		{"ENC001", "PAT001", "2023-01-01", "CDI123", "TypeA", "Some Info", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
		{"ENC001", "PAT001", "2023-01-01", "", "", "", "", "CPT99213", "1", "100.00", "", "", "", "", "", "", "", "", "", "", "", ""},
		{"ENC001", "PAT001", "2023-01-01", "", "", "", "", "CPT99214", "2", "150.00", "", "", "", "", "", "", "", "", "", "", "", ""},
		{"ENC001", "PAT001", "2023-01-01", "", "", "", "", "", "", "", "EM001", "3", "", "", "", "", "", "", "", "", "", ""},
		{"ENC002", "PAT002", "2023-01-02", "", "", "", "", "", "", "", "", "", "", "", "", "R10.9", "", "", "", "", "", ""},
		{"ENC002", "PAT002", "2023-01-02", "CDI456", "TypeB", "CDI Details", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""},
		{"ENC003", "PAT003", "2023-01-03", "", "", "", "", "", "", "", "", "", "EM99285", "5", "", "", "", "", "", "", "", ""},
		{"ENC003", "PAT003", "2023-01-03", "", "", "", "", "", "", "", "", "", "", "", "G0101", "", "", "", "PQR01", "", "", ""},
		{"ENC003", "PAT003", "2023-01-03", "", "", "", "", "", "", "", "", "", "", "", "", "J1234", "", "", "", "", "", "SPEC001"},
		{"ENC004", "PAT004", "2023-01-04", "", "", "", "", "CPT99284", "1", "200.00", "", "", "", "", "", "", "", "", "", "", "", ""},
		{"ENC004", "PAT004", "2023-01-04", "", "", "", "", "", "", "", "", "", "", "", "", "A4649", "", "", "", "", "SF001", ""},
	}

	var content strings.Builder
	content.WriteString(strings.Join(header, ","))
	content.WriteString("\n")
	for _, row := range rows {
		content.WriteString(strings.Join(row, ","))
		content.WriteString("\n")
	}

	if err := os.WriteFile(filename, []byte(content.String()), 0644); err != nil {
		log.Fatalf("Failed to create sample CSV: %v", err)
	}
	log.Printf("Created sample CSV: %s", filename)
}
