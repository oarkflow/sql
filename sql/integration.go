package sql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/oarkflow/json"

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/utils"
)

type Integration interface {
	Type() string
	Name() string
	ReadData(source string) ([]utils.Record, error)
}

var (
	irMu                sync.RWMutex
	integrationRegistry = make(map[string]Integration)
)

func RegisterIntegration(key string, integration Integration) {
	irMu.Lock()
	defer irMu.Unlock()
	integrationRegistry[key] = integration
}

func UnregisterIntegration(key string) error {
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
	irMu.RLock()
	integration, exists := integrationRegistry[integrationKey]
	irMu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("integration not found: %s", integrationKey)
	}
	return integration.ReadData(source)
}

type SQLIntegration struct {
	DataConfig *config.DataConfig
}

func (s *SQLIntegration) Type() string {
	return "sql"
}

func (s *SQLIntegration) Name() string {
	return "SQLIntegration"
}

func (s *SQLIntegration) ReadData(source string) ([]utils.Record, error) {
	if s.DataConfig == nil {
		return nil, fmt.Errorf("no data config provided for SQL integration")
	}
	if source == "" {
		return nil, fmt.Errorf("no table name provided for SQL integration")
	}
	db, err := config.OpenDB(*s.DataConfig)
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

type RESTIntegration struct {
	Endpoint     string
	Method       string
	Headers      map[string]string
	Body         interface{}
	QueryParams  map[string]string
	OutputFormat string
}

func (r *RESTIntegration) Type() string {
	return "rest"
}

func (r *RESTIntegration) Name() string {
	return "RESTIntegration"
}

func (r *RESTIntegration) ReadData(source string) ([]utils.Record, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	reqBody, _ := json.Marshal(r.Body)
	req, err := http.NewRequest(strings.ToUpper(r.Method), r.buildURL(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	for key, value := range r.Headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()
	var records []utils.Record
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	err = json.Unmarshal(data, &records)
	if err != nil {
		var singleRecord utils.Record
		err = json.Unmarshal(data, &singleRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}
		records = append(records, singleRecord)
	}
	return records, nil
}

func (r *RESTIntegration) buildURL() string {
	if len(r.QueryParams) == 0 {
		return r.Endpoint
	}
	var params []string
	for key, value := range r.QueryParams {
		params = append(params, fmt.Sprintf("%s=%s", key, value))
	}
	return fmt.Sprintf("%s?%s", r.Endpoint, strings.Join(params, "&"))
}

type FieldMapping struct {
	Field    string `json:"field"`
	Selector string `json:"selector"`
	Target   string `json:"target"`
}

type WebIntegration struct {
	Endpoint      string
	Rules         string
	Target        string
	OutputFormat  string
	FieldMappings []FieldMapping
}

func (w *WebIntegration) Type() string {
	return "web"
}

func (w *WebIntegration) Name() string {
	return "WebIntegration"
}

func (w *WebIntegration) ReadData(source string) ([]utils.Record, error) {
	resp, err := http.Get(w.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to GET %s: %w", w.Endpoint, err)
	}
	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML from %s: %w", w.Endpoint, err)
	}

	if len(w.FieldMappings) > 0 {
		var records []utils.Record
		doc.Find(w.Rules).Each(func(i int, row *goquery.Selection) {
			record := make(utils.Record)
			var fieldValues []string
			for _, mapping := range w.FieldMappings {
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
			records = append(records, record)
		})
		return records, nil
	}
	if w.Rules != "" {
		target := w.Target
		if target == "" {
			target = "text"
		}
		var results []interface{}
		doc.Find(w.Rules).Each(func(i int, s *goquery.Selection) {
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
		switch w.OutputFormat {
		case "string":
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
			record := utils.Record{"content": results}
			return []utils.Record{record}, nil
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response from %s: %w", w.Endpoint, err)
	}
	record := utils.Record{"content": string(body)}
	return []utils.Record{record}, nil
}
