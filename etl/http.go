package etl

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func (m *Manager) Serve(addr string) error {
	app := fiber.New()
	app.Use(logger.New())
	
	// If dashboard credentials are set via env variables, secure endpoints.
	user := os.Getenv("DASHBOARD_USER")
	pass := os.Getenv("DASHBOARD_PASS")
	if user != "" && pass != "" {
		app.Use(basicauth.New(basicauth.Config{
			Users: map[string]string{
				user: pass,
			},
		}))
	}
	
	// Home page with links.
	app.Get("/", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		homeHTML := `
			<h1>ETL Manager</h1>
			<ul>
				<li><a href="/config">Enter ETL Config</a></li>
				<li><a href="/etls">List ETL Jobs</a></li>
			</ul>
		`
		return c.SendString(homeHTML)
	})
	
	// Page to enter JSON config.
	app.Get("/config", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		configHTML := `
			<h1>Enter ETL Config (JSON, YAML or BCL)</h1>
			<form action="/config" method="POST">
				<textarea name="config" rows="20" cols="80"></textarea><br>
				<input type="submit" value="Submit">
			</form>
		`
		return c.SendString(configHTML)
	})
	
	// POST endpoint to accept JSON config and prepare ETL.
	app.Post("/config", func(c *fiber.Ctx) error {
		configJSON := c.FormValue("config")
		cfg, err := DetectConfigFormat(configJSON)
		if err != nil {
			return c.Status(400).SendString(fmt.Sprintf("Invalid config format: %v", err))
		}
		_, err = m.Prepare(&cfg)
		if err != nil {
			return c.Status(500).SendString(fmt.Sprintf("Error preparing ETL: %v", err))
		}
		return c.Redirect("/etls")
	})
	
	// Page to list existing ETL jobs with their status.
	app.Get("/etls", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		m.mu.Lock()
		defer m.mu.Unlock()
		html := "<h1>ETL Jobs</h1><ul>"
		for id, etl := range m.etls {
			// Fixed HTML: one list item per job containing both links.
			html += fmt.Sprintf("<li>ID: %s - Status: %s - <a href=\"/etls/%s\">Details</a> - <a href=\"/etls/%s/start\">Start</a></li>", id, etl.Status, id, id)
		}
		html += "</ul>"
		return c.SendString(html)
	})
	
	// Page to show details and live metrics for an ETL job.
	app.Get("/etls/:id/start", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		// Use a background context to run the ETL job to avoid cancellation of the request context.
		go func(etl *ETL) {
			if err := etl.Run(context.Background()); err != nil {
				log.Printf("ETL job %s failed asynchronously: %v", id, err)
			}
		}(etl)
		return c.Redirect("/etls/" + id)
	})
	
	app.Post("/etls/:id/stop", func(c *fiber.Ctx) error {
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		if etl.cancelFunc != nil {
			etl.cancelFunc()
			etl.Status = "STOPPED"
			return c.SendString(fmt.Sprintf("ETL job %s has been stopped", id))
		}
		return c.Status(400).SendString("ETL job cannot be stopped (it may not be running)")
	})
	
	app.Post("/etls/:id/adjust-worker", func(c *fiber.Ctx) error {
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		workerCount, _ := strconv.Atoi(c.FormValue("worker_count"))
		if workerCount == 0 {
			return c.Status(400).SendString("Invalid worker count")
		}
		etl.AdjustWorker(workerCount)
		return c.Redirect("/etls/" + id)
	})
	
	// Page to show details and live metrics for an ETL job.
	app.Get("/etls/:id", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		detailsHTML := fmt.Sprintf(`
			<h1>ETL %s Details</h1>
			<p>Status: %s, Createad At: %s</p>
			<h2>Metrics (updates every 2 seconds)</h2>
			<p>Current number of workers</p>
			<form action="/etls/%s/adjust-worker" method="post">
				<input type="number" name="worker_count" value="%d"/>
				<input type="submit"/>
			</form>
			<pre id="metrics"></pre>
			<script>
				async function fetchMetrics() {
					const res = await fetch("/etls/%s/metrics");
					const data = await res.json();
					document.getElementById("metrics").innerText = JSON.stringify(data, null, 2);
				}
				fetchMetrics();
				setInterval(fetchMetrics, 2000);
			</script>
		`, id, etl.Status, etl.CreatedAt.Format(time.DateTime), id, etl.workerCount, id)
		return c.SendString(detailsHTML)
	})
	
	// API endpoint to get metrics JSON for an ETL job.
	app.Get("/etls/:id/metrics", func(c *fiber.Ctx) error {
		id := c.Params("id")
		m.mu.Lock()
		etl, ok := m.etls[id]
		m.mu.Unlock()
		if !ok {
			return c.Status(404).SendString("ETL not found")
		}
		return c.JSON(etl.GetSummary())
	})
	
	app.Post("/shutdown", func(c *fiber.Ctx) error {
		go func() {
			time.Sleep(100 * time.Millisecond)
			if err := app.Shutdown(); err != nil {
				log.Printf("Error during shutdown: %v", err)
			}
		}()
		return c.SendString("Server is shutting down...")
	})
	
	log.Printf("Starting API server on %s", addr)
	return app.Listen(addr)
}
