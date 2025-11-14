package restadapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/oarkflow/dipper"
	"github.com/oarkflow/json"
	"github.com/oarkflow/log"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type Adapter struct {
	config       config.DataConfig
	httpClient   *http.Client
	mode         string
	method       string
	contentType  string
	payloadField string
	headers      map[string]string
	timeout      time.Duration
}

func New(cfg config.DataConfig) contracts.LookupLoader {
	return newAdapter(cfg, "lookup")
}

func NewLoader(cfg config.DataConfig) contracts.Loader {
	return newAdapter(cfg, "loader")
}

func newAdapter(cfg config.DataConfig, mode string) *Adapter {
	headers := map[string]string{}
	for k, v := range cfg.Settings {
		if strings.EqualFold(k, "headers") {
			if hv, ok := v.(map[string]any); ok {
				for hk, hvv := range hv {
					if str, ok := hvv.(string); ok {
						headers[hk] = str
					}
				}
			}
		}
	}

	method := strings.ToUpper(getSetting(cfg.Settings, "method"))
	if method == "" {
		if mode == "loader" {
			method = http.MethodPost
		} else {
			method = http.MethodGet
		}
	}

	contentType := getSetting(cfg.Settings, "content_type")
	if contentType == "" && mode == "loader" {
		contentType = "application/json"
	}

	payloadField := getSetting(cfg.Settings, "payload_field")
	if payloadField == "" && mode == "loader" {
		payloadField = "hl7_json"
	}

	timeout := 10 * time.Second
	if v := getSetting(cfg.Settings, "timeout_ms"); v != "" {
		if ms, err := parseDurationMillis(v); err == nil {
			timeout = ms
		}
	}

	adapter := &Adapter{
		config:       cfg,
		httpClient:   &http.Client{Timeout: timeout},
		mode:         mode,
		method:       method,
		contentType:  contentType,
		payloadField: payloadField,
		headers:      headers,
		timeout:      timeout,
	}
	return adapter
}

func (a *Adapter) Setup(ctx context.Context) error {
	if a.httpClient == nil {
		a.httpClient = &http.Client{Timeout: a.timeout}
	}
	if a.endpoint() == "" {
		return fmt.Errorf("rest adapter: missing endpoint")
	}
	if a.mode == "lookup" && strings.EqualFold(a.method, http.MethodGet) {
		resp, err := a.httpClient.Get(a.endpoint())
		if err != nil {
			return err
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	return nil
}

func (a *Adapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	for _, rec := range records {
		if err := a.StoreSingle(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

// New method: StoreSingle wraps StoreBatch to store one record.
func (a *Adapter) StoreSingle(ctx context.Context, rec utils.Record) error {
	if a.mode != "loader" {
		return fmt.Errorf("rest adapter: not configured as loader")
	}
	body, contentType, err := a.buildPayload(rec)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, a.method, a.endpoint(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range a.headers {
		req.Header.Set(k, v)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("REST request returned status %s", resp.Status)
	}
	return nil
}

func (a *Adapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := a.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (a *Adapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record, 100)
	go func() {
		defer close(out)
		resp, err := a.httpClient.Get(a.endpoint())
		if err != nil {
			log.Printf("REST GET error: %v", err)
			return
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Error reading REST response: %v", err)
			return
		}
		// Expecting a JSON array of objects or an object with data field
		var rawData any
		if err := json.Unmarshal(data, &rawData); err != nil {
			log.Printf("JSON unmarshal error: %v", err)
			return
		}
		var recs []utils.Record
		if a.config.DataPath != "" {
			// Get data from the specified path
			dataVal, err := dipper.Get(rawData, a.config.DataPath)
			if err != nil {
				log.Printf("Error getting data from path %s: %v", a.config.DataPath, err)
				return
			}
			// Assume it's an array
			if dataSlice, ok := dataVal.([]any); ok {
				for _, item := range dataSlice {
					if rec, ok := item.(map[string]any); ok {
						recs = append(recs, utils.Record(rec))
					}
				}
			} else {
				log.Printf("Data at path %s is not an array", a.config.DataPath)
				return
			}
		} else {
			// Assume root is the array
			if dataSlice, ok := rawData.([]any); ok {
				for _, item := range dataSlice {
					if rec, ok := item.(map[string]any); ok {
						recs = append(recs, utils.Record(rec))
					}
				}
			} else {
				log.Printf("Root data is not an array")
				return
			}
		}
		for _, rec := range recs {
			out <- rec
		}
	}()
	return out, nil
}

func (a *Adapter) Close() error {
	// nothing to close for HTTP client
	return nil
}

func (a *Adapter) endpoint() string {
	if a.config.Source != "" {
		return a.config.Source
	}
	if a.config.File != "" {
		return a.config.File
	}
	return ""
}

func (a *Adapter) buildPayload(rec utils.Record) ([]byte, string, error) {
	var payload any = rec
	if a.payloadField != "" {
		if val, ok := rec[a.payloadField]; ok {
			payload = val
		} else {
			return nil, "", fmt.Errorf("rest adapter: missing payload field %s", a.payloadField)
		}
	}
	switch v := payload.(type) {
	case []byte:
		return v, a.contentTypeOrDefault("application/octet-stream"), nil
	case string:
		return []byte(v), a.contentTypeOrDefault("text/plain"), nil
	case fmt.Stringer:
		return []byte(v.String()), a.contentTypeOrDefault("text/plain"), nil
	case map[string]any:
		data, err := json.Marshal(v)
		return data, a.contentTypeOrDefault("application/json"), err
	case []utils.Record:
		data, err := json.Marshal(v)
		return data, a.contentTypeOrDefault("application/json"), err
	case []any:
		data, err := json.Marshal(v)
		return data, a.contentTypeOrDefault("application/json"), err
	default:
		data, err := json.Marshal(v)
		return data, a.contentTypeOrDefault("application/json"), err
	}
}

func (a *Adapter) contentTypeOrDefault(fallback string) string {
	if a.contentType != "" {
		return a.contentType
	}
	return fallback
}

func getSetting(settings map[string]any, key string) string {
	for k, v := range settings {
		if strings.EqualFold(k, key) {
			if str, ok := v.(string); ok {
				return str
			}
			if f, ok := v.(fmt.Stringer); ok {
				return f.String()
			}
		}
	}
	return ""
}

func parseDurationMillis(value string) (time.Duration, error) {
	if value == "" {
		return 0, fmt.Errorf("empty duration")
	}
	if strings.ContainsAny(value, "hms") {
		return time.ParseDuration(value)
	}
	var ms int64
	if _, err := fmt.Sscanf(value, "%d", &ms); err != nil {
		return 0, err
	}
	return time.Duration(ms) * time.Millisecond, nil
}
