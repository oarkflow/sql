package sql

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/oarkflow/etl/pkg/adapters"
	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/utils"
)

type Integration struct {
	Type       string
	DataConfig *config.DataConfig // used for SQL (mysql, postgres, etc.)
	Endpoint   string             // used for REST integration
	Method     string
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

func readService(identifier string) ([]utils.Record, error) {
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
