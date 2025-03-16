package etl

import (
	"sync"
)

// Manager maintains a registry of multiple ETL services.
type Manager struct {
	services map[string]*ETL
	mu       sync.RWMutex
}

// NewETLManager creates an ETLManager.
func NewETLManager() *Manager {
	return &Manager{
		services: make(map[string]*ETL),
	}
}

// AddService registers a new ETL service.
func (m *Manager) AddService(s *ETL) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services[s.ID] = s
}

// GetService retrieves an ETL service by its ID.
func (m *Manager) GetService(id string) (*ETL, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.services[id]
	return s, ok
}

// ListServices returns all registered ETL services.
func (m *Manager) ListServices() []*ETL {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]*ETL, 0, len(m.services))
	for _, s := range m.services {
		list = append(list, s)
	}
	return list
}

/*
func (m *Manager) Start(addr string) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})
	app.Use(logger.New())

	api := app.Group("/api")
	// Upload a YAML configuration.
	// POST /api/config
	api.Post("/etl", func(c *fiber.Ctx) error {
		var cfg config.Config
		err := c.BodyParser(&cfg)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "Invalid configuration")
		}

		// For each table mapping in the config, create an ETL instance.
		for i, table := range cfg.Tables {
			id := fmt.Sprintf("etl-%d", i+1)
			etlInstance := CreateETLFromConfig(&cfg, table)
			service := &ETLService{
				ID:    id,
				Name:  fmt.Sprintf("ETL Service %d", i+1),
				ETL:   etlInstance,
				Table: table,
			}
			manager.AddService(service)
			log.Printf("Registered ETL service: %s", id)
		}
		return c.JSON(fiber.Map{
			"message": "Configuration loaded and ETL services created",
		})
	})

	// List all ETL services.
	// GET /api/etls
	api.Get("/etl/list", func(c *fiber.Ctx) error {
		services := manager.ListServices()
		list := make([]fiber.Map, 0, len(services))
		for _, s := range services {
			list = append(list, fiber.Map{
				"id":   s.ID,
				"name": s.Name,
			})
		}
		return c.JSON(list)
	})

	// Get details for a specific ETL service.
	// GET /api/etl/:id
	api.Get("/etl/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		service, ok := manager.GetService(id)
		if !ok {
			return fiber.NewError(fiber.StatusNotFound, "ETL service not found")
		}
		return c.JSON(fiber.Map{
			"global": GlobalConfig,
			"table":  service.Table,
		})
	})

	// Trigger an ETL run.
	// POST /api/etl/:id/run
	api.Post("/etl/:id/run", func(c *fiber.Ctx) error {
		id := c.Params("id")
		service, ok := manager.GetService(id)
		if !ok {
			return fiber.NewError(fiber.StatusNotFound, "ETL service not found")
		}
		go func() {
			if err := service.ETL.Run(context.Background(), service.Table); err != nil {
				log.Printf("ETL run error for service %s: %v", id, err)
			}
		}()
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
			"message": "ETL run started",
		})
	})

	// Adjust the worker count.
	// POST /api/etl/:id/adjust-worker
	api.Post("/etl/:id/adjust-worker", func(c *fiber.Ctx) error {
		id := c.Params("id")
		service, ok := manager.GetService(id)
		if !ok {
			return fiber.NewError(fiber.StatusNotFound, "ETL service not found")
		}
		var payload struct {
			WorkerCount int `json:"workerCount"`
		}
		if err := c.BodyParser(&payload); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "Invalid payload")
		}
		if payload.WorkerCount < 1 {
			return fiber.NewError(fiber.StatusBadRequest, "workerCount must be at least 1")
		}
		service.ETL.SetWorkerCount(payload.WorkerCount)
		return c.JSON(fiber.Map{
			"message": "Worker count updated",
		})
	})

	// Get ETL metrics.
	// GET /api/etl/:id/metrics
	api.Get("/etl/:id/metrics", func(c *fiber.Ctx) error {
		id := c.Params("id")
		service, ok := manager.GetService(id)
		if !ok {
			return fiber.NewError(fiber.StatusNotFound, "ETL service not found")
		}
		metrics := service.ETL.GetMetrics()
		return c.JSON(metrics)
	})

	// Serve frontend static files from "./public".
	app.Static("/", "./public")

	log.Println("Centralized ETL Server running on :3000")
	log.Fatal(app.Listen(":3000"))

}
*/
