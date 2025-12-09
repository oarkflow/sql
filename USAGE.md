# Usage Guide

This guide walks through the key workflows covered by the latest changes, including running the server, exploring the query workbench, and managing saved queries with the new rename/delete capabilities.

## Prerequisites

- Go 1.21+
- SQLite (the default Go SQLite driver is bundled; no external service required)
- Modern browser for the web console (Chrome/Firefox/Safari/Edge)

## Build & Run

```bash
# from repository root
GO111MODULE=on go build -o etl ./cmd/cli

# start the HTTP server (serves the web console + APIs)
./etl serve --enable-mocks
# optionally customize: ./etl serve --port 3000 --static-path ./views
```

Open `http://localhost:8080` (or your chosen port) to reach the dashboard.

## Query Workbench Walkthrough

The **Query Editor & Runner** view (`/query`) now has full support for saved query CRUD operations.

1. **Select Integration** – choose a datasource from the dropdown to load schema metadata in the sidebar.
2. **Compose SQL** – Monaco editor powers linting, formatting, keyboard shortcuts (`Ctrl/Cmd+Enter` to run, `Ctrl/Cmd+S` to save).
3. **Execute** – click `Execute` or use the shortcut to POST to `/api/query`. The UI displays row counts, execution time, and keeps a rolling performance chart.
4. **History Drawer** – switch to the History tab to replay prior executions. Each entry tracks success status, integration, duration, and links back to the editor.

## Saved Query Lifecycle

| Action | UI Path | Backend Endpoint |
|--------|---------|------------------|
| Create | `Save Query` button / `Ctrl/Cmd+S` | `POST /api/query/save` |
| List   | `Saved Queries` dropdown auto-refresh | `GET /api/query/save` |
| Update name/query/integration | `Rename` button in toolbar | `PUT /api/query/save/:id` |
| Delete | `Delete` button in toolbar | `DELETE /api/query/save/:id` |

### Creating a Saved Query

1. Write your SQL and pick an integration.
2. Click **Save Query** (or `Ctrl/Cmd+S`).
3. Provide a descriptive name when prompted.
4. The dropdown refreshes with the new entry selected; future executions will carry the `savedQueryId` so history ties back to the saved definition.

### Renaming a Saved Query

1. Select an item in the **Saved Queries** dropdown.
2. Click **Rename**. Enter the new label and confirm.
3. The UI issues a `PUT /api/query/save/:id` with the updated name and refreshes the dropdown while keeping your selection.

### Updating Query Text Via API

While the UI currently renames only, the API also allows updating the SQL text or integration:

```bash
curl -X PUT http://localhost:8080/api/query/save/<id> \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM read_file(\"users.csv\") LIMIT 50"}'
```

Refreshing the page (or reloading saved queries) will pull the updated content into the editor.

### Deleting a Saved Query

1. Pick the target query in the dropdown.
2. Click **Delete** and confirm the prompt.
3. The UI calls `DELETE /api/query/save/:id`, removes the entry, clears the dropdown selection, and disables Rename/Delete until another saved query is chosen.

### Keyboard Shortcut Recap

- `Ctrl/Cmd+Enter` – Execute current query
- `Ctrl/Cmd+S` – Save query (opens name prompt, then persists and refreshes dropdown)

## Query History Hygiene

- Use **Clear History** (History tab) to call `DELETE /api/query/history` and reset the list.
- When executing while a saved query is selected, the history item contains `savedQueryId`, making it easy to trace which saved queries are actively used.

## Troubleshooting

- **Buttons disabled?** Ensure a saved query is selected; Rename/Delete stay disabled otherwise.
- **Schema not loading?** Verify the integration dropdown is set and the backend exposes `/api/schema/:integration` for that source.
- **API errors?** Inspect browser dev tools – the UI surfaces messages via the blue/green/red status banner at the bottom of the page.

For additional ETL runner/CLI details, see `README.md`. This document focuses on the query workbench updates delivered in the latest changeset.
