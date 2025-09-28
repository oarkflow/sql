package serviceadapter

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

// ServiceAdapter implements the ETL Source interface using integrations as the data source
type Adapter struct {
	manager     *integrations.Manager
	serviceName string
	serviceType string
	query       string
	table       string
	key         string
	credentials map[string]interface{}
	mu          sync.RWMutex
	initialized bool
}

// Config represents the configuration for the service adapter
type Config struct {
	ServiceName string                 `json:"service_name"`
	ServiceType string                 `json:"service_type"`
	Query       string                 `json:"query"`
	Table       string                 `json:"table"`
	Key         string                 `json:"key"`
	Credentials map[string]interface{} `json:"credentials"`
}

// New creates a new service adapter
func New(manager *integrations.Manager, config Config) *Adapter {
	return &Adapter{
		manager:     manager,
		serviceName: config.ServiceName,
		serviceType: config.ServiceType,
		query:       config.Query,
		table:       config.Table,
		key:         config.Key,
		credentials: config.Credentials,
	}
}

// Setup initializes the service adapter
func (a *Adapter) Setup(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.initialized {
		return nil
	}

	// Validate service exists
	if _, err := a.manager.GetService(a.serviceName); err != nil {
		return fmt.Errorf("service %s not found: %w", a.serviceName, err)
	}

	// Validate service type is supported for data extraction
	service, _ := a.manager.GetService(a.serviceName)
	if service.Type != integrations.ServiceTypeDB {
		return fmt.Errorf("service type %s is not supported for data extraction (only database services are supported)", service.Type)
	}

	a.initialized = true
	log.Printf("[ServiceAdapter] Initialized for service: %s", a.serviceName)
	return nil
}

// Extract executes the query against the service and returns a channel of records
func (a *Adapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	opt := &contracts.SourceOption{Table: a.table, Query: a.query}
	for _, op := range opts {
		op(opt)
	}
	table, query := a.table, a.query
	if opt.Table != "" {
		table = opt.Table
	}
	if opt.Query != "" {
		query = opt.Query
	}
	var q string
	if query != "" {
		q = query
	} else {
		q = fmt.Sprintf("SELECT * FROM %s", table)
	}
	if !a.initialized {
		if err := a.Setup(ctx); err != nil {
			return nil, err
		}
	}

	out := make(chan utils.Record, 100)

	go func() {
		defer close(out)

		log.Printf("[ServiceAdapter] Executing query: %s", q)

		// Execute the query through the integrations manager
		result, err := a.manager.ExecuteDatabaseQuery(ctx, a.serviceName, q, opt.Args...)
		if err != nil {
			log.Printf("[ServiceAdapter] Query execution failed: %v", err)
			return
		}
		fmt.Println(result)
		// Convert result to records
		records := a.convertToRecords(result)
		log.Printf("[ServiceAdapter] Retrieved %d records", len(records))
		// Send records through channel
		for _, record := range records {
			select {
			case <-ctx.Done():
				log.Printf("[ServiceAdapter] Context cancelled, stopping extraction")
				return
			case out <- record:
			}
		}
	}()

	return out, nil
}

// convertToRecords converts the integration result to ETL records
func (a *Adapter) convertToRecords(result interface{}) []utils.Record {
	rows, ok := result.([]map[string]interface{})
	if !ok {
		log.Printf("[ServiceAdapter] Unexpected result type: %T", result)
		return nil
	}

	records := make([]utils.Record, 0, len(rows))
	for _, row := range rows {
		record := make(utils.Record)
		for key, value := range row {
			record[key] = value
		}
		records = append(records, record)
	}

	return records
}

// Close closes the service adapter
func (a *Adapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.initialized = false
	log.Printf("[ServiceAdapter] Closed for service: %s", a.serviceName)
	return nil
}

// GetServiceName returns the service name
func (a *Adapter) GetServiceName() string {
	return a.serviceName
}

// GetServiceType returns the service type
func (a *Adapter) GetServiceType() string {
	return a.serviceType
}

// IsInitialized returns whether the adapter is initialized
func (a *Adapter) IsInitialized() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.initialized
}

// UpdateCredentials updates the credentials for the service
func (a *Adapter) UpdateCredentials(credentials map[string]interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.credentials = credentials
}

// GetConfig returns the current configuration
func (a *Adapter) GetConfig() Config {
	return Config{
		ServiceName: a.serviceName,
		ServiceType: a.serviceType,
		Query:       a.query,
		Table:       a.table,
		Key:         a.key,
		Credentials: a.credentials,
	}
}
