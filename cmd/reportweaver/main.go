package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/oarkflow/sql"
	"github.com/oarkflow/sql/integrations"
)

type appState struct {
	Reports      []map[string]any    `json:"reports"`
	ReportGroups []map[string]any    `json:"report_groups"`
	Queries      []map[string]any    `json:"queries"`
	Integrations integrations.Config `json:"integrations"`
}

type stateStore struct {
	path  string
	mu    sync.RWMutex
	state appState
}

type queryRequest struct {
	SQL    string              `json:"sql"`
	Config integrations.Config `json:"config"`
	UserID string              `json:"user_id"`
}

type queryResponse struct {
	Data    []map[string]any `json:"data"`
	Columns []string         `json:"columns"`
}

func newStateStore(path string) *stateStore {
	store := &stateStore{path: path, state: appState{}}
	_ = store.load()
	return store
}

func (s *stateStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.path) == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var next appState
	if err := json.Unmarshal(data, &next); err != nil {
		return err
	}
	s.state = next
	return nil
}

func (s *stateStore) snapshot() appState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *stateStore) update(mutator func(state *appState) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := mutator(&s.state); err != nil {
		return err
	}
	return s.saveLocked()
}

func (s *stateStore) saveLocked() error {
	if strings.TrimSpace(s.path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, body, 0o644)
}

func getID(item map[string]any) string {
	value, _ := item["id"].(string)
	return strings.TrimSpace(value)
}

func getKey(item map[string]any, key string) string {
	value, _ := item[key].(string)
	return strings.TrimSpace(value)
}

func upsertByID(items []map[string]any, candidate map[string]any) []map[string]any {
	id := getID(candidate)
	if id == "" {
		candidate["id"] = fmt.Sprintf("%d", time.Now().UnixNano())
		return append(items, candidate)
	}
	for idx := range items {
		if getID(items[idx]) == id {
			items[idx] = candidate
			return items
		}
	}
	return append(items, candidate)
}

func removeByID(items []map[string]any, id string) []map[string]any {
	next := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if getID(item) == id {
			continue
		}
		next = append(next, item)
	}
	return next
}

func upsertService(services []integrations.Service, candidate integrations.Service) []integrations.Service {
	for idx := range services {
		if strings.EqualFold(services[idx].Name, candidate.Name) {
			services[idx] = candidate
			return services
		}
	}
	return append(services, candidate)
}

func removeService(services []integrations.Service, name string) []integrations.Service {
	next := make([]integrations.Service, 0, len(services))
	for _, service := range services {
		if strings.EqualFold(service.Name, name) {
			continue
		}
		next = append(next, service)
	}
	return next
}

func upsertCredential(credentials []integrations.Credential, candidate integrations.Credential) []integrations.Credential {
	for idx := range credentials {
		if strings.EqualFold(credentials[idx].Key, candidate.Key) {
			credentials[idx] = candidate
			return credentials
		}
	}
	return append(credentials, candidate)
}

func removeCredential(credentials []integrations.Credential, key string) []integrations.Credential {
	next := make([]integrations.Credential, 0, len(credentials))
	for _, credential := range credentials {
		if strings.EqualFold(credential.Key, key) {
			continue
		}
		next = append(next, credential)
	}
	return next
}

func extractColumns(records []map[string]any) []string {
	if len(records) == 0 {
		return []string{}
	}
	columns := make([]string, 0, len(records[0]))
	for key := range records[0] {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	return columns
}

func getPages(report map[string]any) []any {
	raw, ok := report["pages"].([]any)
	if ok {
		return raw
	}
	return []any{}
}

func getWidgets(page map[string]any) []any {
	raw, ok := page["widgets"].([]any)
	if ok {
		return raw
	}
	return []any{}
}

func createApp(store *stateStore) *fiber.App {
	app := fiber.New(fiber.Config{AppName: "report-weaver-sql-api"})

	app.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin,Content-Type,Accept,Authorization",
		AllowCredentials: false,
	}))

	app.Get("/api/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	app.Get("/api/state", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot())
	})

	app.Put("/api/state", func(c *fiber.Ctx) error {
		var next appState
		if err := c.BodyParser(&next); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid state payload"})
		}
		if err := store.update(func(state *appState) error {
			*state = next
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "saved"})
	})

	app.Get("/api/reports", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot().Reports)
	})

	app.Post("/api/reports", func(c *fiber.Ctx) error {
		var report map[string]any
		if err := c.BodyParser(&report); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid report payload"})
		}
		if err := store.update(func(state *appState) error {
			state.Reports = upsertByID(state.Reports, report)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(report)
	})

	app.Put("/api/reports/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var report map[string]any
		if err := c.BodyParser(&report); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid report payload"})
		}
		report["id"] = id
		if err := store.update(func(state *appState) error {
			state.Reports = upsertByID(state.Reports, report)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(report)
	})

	app.Delete("/api/reports/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		if err := store.update(func(state *appState) error {
			state.Reports = removeByID(state.Reports, id)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	app.Post("/api/reports/:reportId/pages/:pageId/widgets", func(c *fiber.Ctx) error {
		reportID := c.Params("reportId")
		pageID := c.Params("pageId")
		var widget map[string]any
		if err := c.BodyParser(&widget); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid widget payload"})
		}
		if err := store.update(func(state *appState) error {
			for ridx := range state.Reports {
				report := state.Reports[ridx]
				if getID(report) != reportID {
					continue
				}
				pages := getPages(report)
				for pidx := range pages {
					page, ok := pages[pidx].(map[string]any)
					if !ok || getID(page) != pageID {
						continue
					}
					widgets := getWidgets(page)
					widgetID := getKey(widget, "id")
					if widgetID == "" {
						widget["id"] = fmt.Sprintf("%d", time.Now().UnixNano())
					}
					widgets = append(widgets, widget)
					page["widgets"] = widgets
					pages[pidx] = page
					report["pages"] = pages
					state.Reports[ridx] = report
					return nil
				}
			}
			return fmt.Errorf("report/page not found")
		}); err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(widget)
	})

	app.Put("/api/reports/:reportId/pages/:pageId/widgets/:widgetId", func(c *fiber.Ctx) error {
		reportID := c.Params("reportId")
		pageID := c.Params("pageId")
		widgetID := c.Params("widgetId")
		var widget map[string]any
		if err := c.BodyParser(&widget); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid widget payload"})
		}
		widget["id"] = widgetID
		if err := store.update(func(state *appState) error {
			for ridx := range state.Reports {
				report := state.Reports[ridx]
				if getID(report) != reportID {
					continue
				}
				pages := getPages(report)
				for pidx := range pages {
					page, ok := pages[pidx].(map[string]any)
					if !ok || getID(page) != pageID {
						continue
					}
					widgets := getWidgets(page)
					for widx := range widgets {
						current, ok := widgets[widx].(map[string]any)
						if !ok || getID(current) != widgetID {
							continue
						}
						widgets[widx] = widget
						page["widgets"] = widgets
						pages[pidx] = page
						report["pages"] = pages
						state.Reports[ridx] = report
						return nil
					}
				}
			}
			return fmt.Errorf("report/page/widget not found")
		}); err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(widget)
	})

	app.Delete("/api/reports/:reportId/pages/:pageId/widgets/:widgetId", func(c *fiber.Ctx) error {
		reportID := c.Params("reportId")
		pageID := c.Params("pageId")
		widgetID := c.Params("widgetId")
		if err := store.update(func(state *appState) error {
			for ridx := range state.Reports {
				report := state.Reports[ridx]
				if getID(report) != reportID {
					continue
				}
				pages := getPages(report)
				for pidx := range pages {
					page, ok := pages[pidx].(map[string]any)
					if !ok || getID(page) != pageID {
						continue
					}
					widgets := getWidgets(page)
					next := make([]any, 0, len(widgets))
					for _, item := range widgets {
						mapped, ok := item.(map[string]any)
						if !ok || getID(mapped) != widgetID {
							next = append(next, item)
						}
					}
					page["widgets"] = next
					pages[pidx] = page
					report["pages"] = pages
					state.Reports[ridx] = report
					return nil
				}
			}
			return fmt.Errorf("report/page/widget not found")
		}); err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	app.Get("/api/report-groups", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot().ReportGroups)
	})

	app.Post("/api/report-groups", func(c *fiber.Ctx) error {
		var group map[string]any
		if err := c.BodyParser(&group); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid report group payload"})
		}
		if err := store.update(func(state *appState) error {
			state.ReportGroups = upsertByID(state.ReportGroups, group)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(group)
	})

	app.Put("/api/report-groups/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var group map[string]any
		if err := c.BodyParser(&group); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid report group payload"})
		}
		group["id"] = id
		if err := store.update(func(state *appState) error {
			state.ReportGroups = upsertByID(state.ReportGroups, group)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(group)
	})

	app.Delete("/api/report-groups/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		if err := store.update(func(state *appState) error {
			state.ReportGroups = removeByID(state.ReportGroups, id)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	app.Get("/api/queries", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot().Queries)
	})

	app.Post("/api/queries", func(c *fiber.Ctx) error {
		var query map[string]any
		if err := c.BodyParser(&query); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid query payload"})
		}
		if err := store.update(func(state *appState) error {
			state.Queries = upsertByID(state.Queries, query)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(query)
	})

	app.Put("/api/queries/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		var query map[string]any
		if err := c.BodyParser(&query); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid query payload"})
		}
		query["id"] = id
		if err := store.update(func(state *appState) error {
			state.Queries = upsertByID(state.Queries, query)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(query)
	})

	app.Delete("/api/queries/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		if err := store.update(func(state *appState) error {
			state.Queries = removeByID(state.Queries, id)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	app.Get("/api/integrations/config", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot().Integrations)
	})

	app.Put("/api/integrations/config", func(c *fiber.Ctx) error {
		var cfg integrations.Config
		if err := c.BodyParser(&cfg); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid integration config payload"})
		}
		if err := store.update(func(state *appState) error {
			state.Integrations = cfg
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(cfg)
	})

	app.Get("/api/integrations", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot().Integrations.Services)
	})

	app.Post("/api/integrations", func(c *fiber.Ctx) error {
		var service integrations.Service
		if err := c.BodyParser(&service); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid integration payload"})
		}
		if strings.TrimSpace(service.Name) == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "integration name is required"})
		}
		if err := store.update(func(state *appState) error {
			state.Integrations.Services = upsertService(state.Integrations.Services, service)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(service)
	})

	app.Put("/api/integrations/:name", func(c *fiber.Ctx) error {
		name := c.Params("name")
		var service integrations.Service
		if err := c.BodyParser(&service); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid integration payload"})
		}
		service.Name = name
		if err := store.update(func(state *appState) error {
			state.Integrations.Services = upsertService(state.Integrations.Services, service)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(service)
	})

	app.Delete("/api/integrations/:name", func(c *fiber.Ctx) error {
		name := c.Params("name")
		if err := store.update(func(state *appState) error {
			state.Integrations.Services = removeService(state.Integrations.Services, name)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	app.Get("/api/credentials", func(c *fiber.Ctx) error {
		return c.JSON(store.snapshot().Integrations.Credentials)
	})

	app.Post("/api/credentials", func(c *fiber.Ctx) error {
		var credential integrations.Credential
		if err := c.BodyParser(&credential); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid credential payload"})
		}
		if strings.TrimSpace(credential.Key) == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "credential key is required"})
		}
		if err := store.update(func(state *appState) error {
			state.Integrations.Credentials = upsertCredential(state.Integrations.Credentials, credential)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(credential)
	})

	app.Put("/api/credentials/:key", func(c *fiber.Ctx) error {
		key := c.Params("key")
		var credential integrations.Credential
		if err := c.BodyParser(&credential); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid credential payload"})
		}
		credential.Key = key
		if err := store.update(func(state *appState) error {
			state.Integrations.Credentials = upsertCredential(state.Integrations.Credentials, credential)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(credential)
	})

	app.Delete("/api/credentials/:key", func(c *fiber.Ctx) error {
		key := c.Params("key")
		if err := store.update(func(state *appState) error {
			state.Integrations.Credentials = removeCredential(state.Integrations.Credentials, key)
			return nil
		}); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted"})
	})

	app.Post("/api/query", func(c *fiber.Ctx) error {
		var req queryRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request payload"})
		}

		query := strings.TrimSpace(req.SQL)
		if query == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "sql is required"})
		}

		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = "report-weaver"
		}

		config := req.Config
		if len(config.Services) == 0 && len(config.Credentials) == 0 {
			config = store.snapshot().Integrations
		}

		ctx := context.WithValue(context.Background(), "user_id", userID)
		manager := integrations.New()
		if err := sql.RegisterUserManager(ctx, manager); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		for _, cred := range config.Credentials {
			if err := manager.UpdateCredential(cred); err != nil {
				if err := manager.AddCredential(cred); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
				}
			}
		}

		for _, service := range config.Services {
			if err := manager.UpdateService(service); err != nil {
				if err := manager.AddService(service); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
				}
			}
		}

		records, err := sql.Query(ctx, query)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(queryResponse{Data: records, Columns: extractColumns(records)})
	})

	return app
}

func main() {
	storePath := os.Getenv("REPORT_WEAVER_STORE_PATH")
	if strings.TrimSpace(storePath) == "" {
		storePath = filepath.Join(".", "data", "report_weaver_store.json")
	}
	store := newStateStore(storePath)
	app := createApp(store)

	port := os.Getenv("PORT")
	if strings.TrimSpace(port) == "" {
		port = "8090"
	}
	if err := app.Listen(":" + port); err != nil {
		log.Fatal(err)
	}
}
