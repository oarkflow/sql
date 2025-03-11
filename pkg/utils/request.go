package utils

import (
	"crypto/tls"
	"io"
	"net/http"

	"github.com/oarkflow/json"
)

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Disable TLS certificate verification
		},
	},
}

func Request[T any](url, method string, body io.Reader, headers ...map[string]string) (T, error) {
	req, _ := http.NewRequest(method, url, body)
	if len(headers) > 0 {
		for k, v := range headers[0] {
			req.Header.Add(k, v)
		}
	}
	var resp T
	res, err := insecureHTTPClient.Do(req)
	if err != nil {
		return resp, err
	}
	defer res.Body.Close()
	bodyByte, err := io.ReadAll(res.Body)
	if err != nil {
		return resp, err
	}
	err = json.Unmarshal(bodyByte, &resp)
	return resp, err
}
