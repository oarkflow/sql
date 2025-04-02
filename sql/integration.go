package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery" // HTML crawling

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/utils"
)

// FieldMapping defines how to extract a specific field from a row.
type FieldMapping struct {
	Field    string `json:"field"`    // Name of the field in the output record
	Selector string `json:"selector"` // CSS selector relative to the row container
	Target   string `json:"target"`   // Extraction method: "text", "html", or "attr:xxx"
}

// Integration represents various integration types. For web integrations,
// it now supports both a single extraction rule (Rules, Target, OutputFormat)
// and a more structured approach using FieldMappings (e.g. for tables).
type Integration struct {
	Type          string
	DataConfig    *config.DataConfig // used for SQL (mysql, postgres, etc.)
	Endpoint      string             // used for REST and web integrations
	Method        string
	Rules         string         // CSS selector rule for either a full-page extraction or for row selection in a table
	Target        string         // Target extraction for single value extraction ("text", "html", or "attr:xxx")
	OutputFormat  string         // Desired output format: "string", "json", or empty for raw slice
	FieldMappings []FieldMapping // New: mapping for multi-target extraction (e.g., table columns)
}

var (
	irMu                sync.RWMutex
	integrationRegistry = make(map[string]Integration)
)

// AddIntegration adds a new integration or updates an existing one.
func AddIntegration(key string, integration Integration) {
	irMu.Lock()
	defer irMu.Unlock()
	integrationRegistry[key] = integration
}

// RemoveIntegration removes an integration by its key.
func RemoveIntegration(key string) error {
	irMu.Lock()
	defer irMu.Unlock()
	if _, exists := integrationRegistry[key]; !exists {
		return fmt.Errorf("integration not found: %s", key)
	}
	delete(integrationRegistry, key)
	return nil
}

func ReadService(identifier string) ([]utils.Record, error) {
	parts := strings.SplitN(identifier, ".", 2)
	integrationKey := parts[0]
	var source string
	if len(parts) > 1 {
		source = parts[1]
	}
	// Read integration safely
	irMu.RLock()
	integration, exists := integrationRegistry[integrationKey]
	irMu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("integration not found: %s", integrationKey)
	}
	switch strings.ToLower(integration.Type) {
	case "mysql", "postgres", "sqlite", "sqlite3":
		return readSQLIntegration(integration, source)
	case "rest":
		return readRESTIntegration(integration)
	case "web":
		return readWebIntegration(integration)
	default:
		return nil, fmt.Errorf("unsupported integration type: %s", integration.Type)
	}
}

func readSQLIntegration(integration Integration, source string) ([]utils.Record, error) {
	if integration.DataConfig == nil {
		return nil, fmt.Errorf("no data config provided for SQL integration")
	}
	if source == "" {
		return nil, fmt.Errorf("no table name provided for SQL integration")
	}
	db, err := config.OpenDB(*integration.DataConfig)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	src := adapters.NewSQLAdapterAsSource(db, source, "")
	ctx := context.Background()
	err = src.Setup(ctx)
	if err != nil {
		return nil, err
	}
	return src.LoadData()
}

func readRESTIntegration(integration Integration) ([]utils.Record, error) {
	if integration.Method == "" {
		integration.Method = "GET"
	}
	data, err := utils.Request[[]utils.Record](integration.Endpoint, integration.Method, nil)
	if err != nil {
		// Attempt to parse a single record response
		singleData, err := utils.Request[utils.Record](integration.Endpoint, integration.Method, nil)
		if err != nil {
			return nil, err
		}
		return []utils.Record{singleData}, nil
	}
	return data, nil
}

func readWebIntegration(integration Integration) ([]utils.Record, error) {
	resp, err := http.Get(integration.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to GET %s: %w", integration.Endpoint, err)
	}
	defer resp.Body.Close()

	// Parse the HTML document
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML from %s: %w", integration.Endpoint, err)
	}

	// If field mappings are provided, treat the result as a table or list with multiple columns.
	if len(integration.FieldMappings) > 0 {
		var records []utils.Record
		doc.Find(integration.Rules).Each(func(i int, row *goquery.Selection) {
			record := make(utils.Record)
			var fieldValues []string
			for _, mapping := range integration.FieldMappings {
				elem := row.Find(mapping.Selector)
				var value string
				switch {
				case mapping.Target == "" || mapping.Target == "text":
					value = elem.Text()
				case mapping.Target == "html":
					if html, err := elem.Html(); err == nil {
						value = html
					}
				case strings.HasPrefix(mapping.Target, "attr:"):
					attrName := strings.TrimPrefix(mapping.Target, "attr:")
					if attr, exists := elem.Attr(attrName); exists {
						value = attr
					}
				default:
					value = elem.Text()
				}
				record[mapping.Field] = value
				fieldValues = append(fieldValues, value)
			}
			// If OutputFormat is "string", also merge all fields into a "content" field.
			if integration.OutputFormat == "string" {
				record["content"] = strings.Join(fieldValues, " | ")
			}
			records = append(records, record)
		})
		return records, nil
	}

	// Fallback: single target extraction using integration.Target.
	if integration.Rules != "" {
		// Use default target "text" if not specified.
		target := integration.Target
		if target == "" {
			target = "text"
		}
		var results []interface{}
		doc.Find(integration.Rules).Each(func(i int, s *goquery.Selection) {
			var extracted interface{}
			switch {
			case target == "text":
				extracted = s.Text()
			case target == "html":
				if html, err := s.Html(); err == nil {
					extracted = html
				} else {
					extracted = ""
				}
			case strings.HasPrefix(target, "attr:"):
				attrName := strings.TrimPrefix(target, "attr:")
				if attr, exists := s.Attr(attrName); exists {
					extracted = attr
				} else {
					extracted = ""
				}
			default:
				extracted = s.Text()
			}
			results = append(results, extracted)
		})

		switch integration.OutputFormat {
		case "string":
			// Concatenate all string results.
			var stringResults []string
			for _, r := range results {
				if str, ok := r.(string); ok {
					stringResults = append(stringResults, str)
				}
			}
			joined := strings.Join(stringResults, "\n")
			record := utils.Record{"content": joined}
			return []utils.Record{record}, nil

		case "json":
			// Attempt to join and unmarshal the result as JSON.
			var stringResults []string
			for _, r := range results {
				if str, ok := r.(string); ok {
					stringResults = append(stringResults, str)
				}
			}
			joined := strings.Join(stringResults, "\n")
			var parsed interface{}
			if err := json.Unmarshal([]byte(joined), &parsed); err != nil {
				record := utils.Record{"content": joined}
				return []utils.Record{record}, nil
			}
			record := utils.Record{"content": parsed}
			return []utils.Record{record}, nil

		default:
			// Return raw slice of extracted results.
			record := utils.Record{"content": results}
			return []utils.Record{record}, nil
		}
	}

	// Final fallback: return the full page content.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response from %s: %w", integration.Endpoint, err)
	}
	record := utils.Record{"content": string(body)}
	return []utils.Record{record}, nil
}
