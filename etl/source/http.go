package source

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/oarkflow/sql/utils"
)

type HTTPSource struct {
	URL string
}

func (hs *HTTPSource) Close() error {
	return nil
}

func (hs *HTTPSource) Setup() error {
	return nil
}

func NewHTTPSource(url string) *HTTPSource {
	return &HTTPSource{URL: url}
}

func (hs *HTTPSource) Extract() (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		resp, err := http.Get(hs.URL)
		if err != nil {
			log.Printf("HTTP extraction error: %v", err)
			return
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read HTTP response: %v", err)
			return
		}
		var records []utils.Record
		if err := json.Unmarshal(data, &records); err != nil {
			log.Printf("JSON decoding error: %v", err)
			return
		}
		for _, rec := range records {
			out <- rec
		}
	}()
	return out, nil
}
