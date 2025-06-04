package restadapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/log"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type Adapter struct {
	config     config.DataConfig
	httpClient *http.Client
}

func New(cfg config.DataConfig) contracts.LookupLoader {
	return &Adapter{
		config:     cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (a *Adapter) Setup(ctx context.Context) error {
	// Optionally, test connectivity with a GET request.
	resp, err := a.httpClient.Get(a.config.Source)
	if err != nil {
		return err
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	return nil
}

func (a *Adapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	// POST the records as a JSON array to the REST endpoint.
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	resp, err := a.httpClient.Post(a.config.Source, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("REST POST returned status %s", resp.Status)
	}
	return nil
}

// New method: StoreSingle wraps StoreBatch to store one record.
func (a *Adapter) StoreSingle(ctx context.Context, rec utils.Record) error {
	return a.StoreBatch(ctx, []utils.Record{rec})
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
		resp, err := a.httpClient.Get(a.config.Source)
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
		// Expecting a JSON array of objects
		var recs []utils.Record
		if err := json.Unmarshal(data, &recs); err != nil {
			log.Printf("JSON unmarshal error: %v", err)
			return
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
