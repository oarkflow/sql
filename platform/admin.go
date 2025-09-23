package platform

import (
	"github.com/gofiber/fiber/v2"
)

// AdminAPI provides administrative endpoints
type AdminAPI struct {
	app           *fiber.App
	port          string
	registry      SchemaRegistry
	messageBus    MessageBus
	metrics       MetricsProvider
	healthChecker *HealthChecker
	pipelines     map[string]*ProcessingPipelineExecutor
	gateways      []Adapter
}

// MetricsProvider interface for compatibility
type MetricsProvider interface {
	GetMetrics() map[string]interface{}
}

func NewAdminAPI(port string, registry SchemaRegistry, messageBus MessageBus, metrics MetricsProvider, healthChecker *HealthChecker) *AdminAPI {
	app := fiber.New()

	api := &AdminAPI{
		app:           app,
		port:          port,
		registry:      registry,
		messageBus:    messageBus,
		metrics:       metrics,
		healthChecker: healthChecker,
		pipelines:     make(map[string]*ProcessingPipelineExecutor),
		gateways:      make([]Adapter, 0),
	}

	api.setupRoutes()
	return api
}

func (a *AdminAPI) setupRoutes() {
	// Schema management
	a.app.Get("/api/schemas", a.listSchemas)
	a.app.Post("/api/schemas", a.createSchema)
	a.app.Get("/api/schemas/:name", a.getSchema)
	a.app.Put("/api/schemas/:name", a.updateSchema)
	a.app.Delete("/api/schemas/:name", a.deleteSchema)

	// Pipeline management
	a.app.Get("/api/pipelines", a.listPipelines)
	a.app.Post("/api/pipelines", a.createPipeline)
	a.app.Get("/api/pipelines/:name", a.getPipeline)
	a.app.Put("/api/pipelines/:name", a.updatePipeline)
	a.app.Delete("/api/pipelines/:name", a.deletePipeline)

	// Gateway management
	a.app.Get("/api/gateways", a.listGateways)
	a.app.Post("/api/gateways", a.createGateway)

	// Monitoring
	a.app.Get("/api/metrics", a.getMetrics)
	a.app.Get("/api/health", a.getHealth)

	// Configuration
	a.app.Get("/api/config", a.getConfig)
	a.app.Post("/api/config", a.updateConfig)

	// Web UI
	a.app.Get("/", a.serveUI)
	a.app.Get("/dashboard", a.serveDashboard)
}

func (a *AdminAPI) Start() error {
	return a.app.Listen(":" + a.port)
}

func (a *AdminAPI) RegisterPipeline(name string, pipeline *ProcessingPipelineExecutor) {
	a.pipelines[name] = pipeline
}

func (a *AdminAPI) RegisterGateway(gateway Adapter) {
	a.gateways = append(a.gateways, gateway)
}

func (a *AdminAPI) listSchemas(c *fiber.Ctx) error {
	schemas := a.registry.List()
	return c.JSON(fiber.Map{
		"schemas": schemas,
	})
}

func (a *AdminAPI) createSchema(c *fiber.Ctx) error {
	var schema CanonicalModel
	if err := c.BodyParser(&schema); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid schema format"})
	}

	if err := a.registry.Register(schema); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(201).JSON(fiber.Map{"message": "Schema created"})
}

func (a *AdminAPI) getSchema(c *fiber.Ctx) error {
	name := c.Params("name")

	schema, err := a.registry.Get(name)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Schema not found"})
	}

	return c.JSON(schema)
}

func (a *AdminAPI) updateSchema(c *fiber.Ctx) error {
	name := c.Params("name")

	var schema CanonicalModel
	if err := c.BodyParser(&schema); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid schema format"})
	}

	schema.SchemaName = name
	if err := a.registry.Register(schema); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"message": "Schema updated"})
}

func (a *AdminAPI) deleteSchema(c *fiber.Ctx) error {
	// In a real implementation, this would delete from registry
	return c.JSON(fiber.Map{"message": "Schema deletion not implemented"})
}

func (a *AdminAPI) listPipelines(c *fiber.Ctx) error {
	pipelineNames := make([]string, 0, len(a.pipelines))
	for name := range a.pipelines {
		pipelineNames = append(pipelineNames, name)
	}

	return c.JSON(fiber.Map{
		"pipelines": pipelineNames,
	})
}

func (a *AdminAPI) createPipeline(c *fiber.Ctx) error {
	var pipeline ProcessingPipeline
	if err := c.BodyParser(&pipeline); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid pipeline format"})
	}

	// In a real implementation, this would create and register the pipeline
	return c.Status(201).JSON(fiber.Map{"message": "Pipeline creation not fully implemented"})
}

func (a *AdminAPI) getPipeline(c *fiber.Ctx) error {
	name := c.Params("name")

	pipeline, exists := a.pipelines[name]
	if !exists {
		return c.Status(404).JSON(fiber.Map{"error": "Pipeline not found"})
	}

	return c.JSON(fiber.Map{
		"name":   pipeline.pipeline.Name,
		"stages": pipeline.pipeline.Stages,
	})
}

func (a *AdminAPI) updatePipeline(c *fiber.Ctx) error {
	// Implementation would update pipeline configuration
	return c.JSON(fiber.Map{"message": "Pipeline update not implemented"})
}

func (a *AdminAPI) deletePipeline(c *fiber.Ctx) error {
	// Implementation would remove pipeline
	return c.JSON(fiber.Map{"message": "Pipeline deletion not implemented"})
}

func (a *AdminAPI) listGateways(c *fiber.Ctx) error {
	gatewayInfo := make([]fiber.Map, len(a.gateways))
	for i, gw := range a.gateways {
		gatewayInfo[i] = fiber.Map{
			"name": gw.Name(),
		}
	}

	return c.JSON(fiber.Map{
		"gateways": gatewayInfo,
	})
}

func (a *AdminAPI) createGateway(c *fiber.Ctx) error {
	// Implementation would create and start gateway
	return c.JSON(fiber.Map{"message": "Gateway creation not implemented"})
}

func (a *AdminAPI) getMetrics(c *fiber.Ctx) error {
	if a.metrics != nil {
		return c.JSON(a.metrics.GetMetrics())
	}
	return c.JSON(fiber.Map{"message": "Metrics not available"})
}

func (a *AdminAPI) getHealth(c *fiber.Ctx) error {
	if a.healthChecker != nil {
		results := a.healthChecker.Check()
		return c.JSON(results)
	}
	return c.JSON(fiber.Map{"status": "unknown"})
}

func (a *AdminAPI) getConfig(c *fiber.Ctx) error {
	// Return current configuration
	return c.JSON(fiber.Map{
		"message": "Configuration retrieval not implemented",
	})
}

func (a *AdminAPI) updateConfig(c *fiber.Ctx) error {
	// Update configuration
	return c.JSON(fiber.Map{
		"message": "Configuration update not implemented",
	})
}

func (a *AdminAPI) serveUI(c *fiber.Ctx) error {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Data Ingestion Platform Admin</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .section { margin: 20px 0; padding: 20px; border: 1px solid #ddd; }
        .metric { display: inline-block; margin: 10px; padding: 10px; background: #f0f0f0; }
    </style>
</head>
<body>
    <h1>Data Ingestion Platform Admin</h1>

    <div class="section">
        <h2>Quick Actions</h2>
        <a href="/dashboard">Dashboard</a> |
        <a href="/api/schemas">Schemas</a> |
        <a href="/api/pipelines">Pipelines</a> |
        <a href="/api/metrics">Metrics</a>
    </div>

    <div class="section">
        <h2>API Endpoints</h2>
        <ul>
            <li>GET /api/schemas - List schemas</li>
            <li>POST /api/schemas - Create schema</li>
            <li>GET /api/pipelines - List pipelines</li>
            <li>GET /api/metrics - Get metrics</li>
            <li>GET /api/health - Health check</li>
        </ul>
    </div>
</body>
</html>
`
	c.Set("Content-Type", "text/html")
	return c.SendString(html)
}

func (a *AdminAPI) serveDashboard(c *fiber.Ctx) error {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Platform Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .metric { display: inline-block; margin: 10px; padding: 20px; background: #f8f9fa; border-radius: 5px; }
        .status-good { border-left: 5px solid green; }
        .status-bad { border-left: 5px solid red; }
    </style>
    <script>
        async function loadMetrics() {
            try {
                const response = await fetch('/api/metrics');
                const data = await response.json();
                displayMetrics(data);
            } catch (error) {
                console.error('Error loading metrics:', error);
            }
        }

        function displayMetrics(data) {
            const container = document.getElementById('metrics');
            container.innerHTML = '';

            for (const [key, value] of Object.entries(data)) {
                const div = document.createElement('div');
                div.className = 'metric';
                div.innerHTML = '<strong>' + key + ':</strong> ' + value;
                container.appendChild(div);
            }
        }

        setInterval(loadMetrics, 5000);
        loadMetrics();
    </script>
</head>
<body>
    <h1>Platform Dashboard</h1>

    <div id="metrics">
        <div class="metric">Loading metrics...</div>
    </div>

    <div style="margin-top: 30px;">
        <a href="/">Back to Admin</a>
    </div>
</body>
</html>
`
	c.Set("Content-Type", "text/html")
	return c.SendString(html)
}
