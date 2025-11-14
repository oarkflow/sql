# ETL Tool

A powerful ETL (Extract, Transform, Load) tool for data processing with both CLI and server modes.

## Features

- **Multiple Configuration Formats**: Support for BCL, YAML, and JSON configurations
- **Asynchronous Job Processing**: Built-in runner for managing ETL jobs asynchronously
- **Web Interface**: Server mode with a web dashboard for job monitoring
- **Real-time Metrics**: Detailed metrics including WorkerActivities tracking
- **Graceful Shutdown**: Proper signal handling for clean shutdowns
- **BridgLink Pipelines**: Describe complex source → parser → transform → destination workflows using YAML/JSON/BCL, including out-of-the-box HL7 → XML/JSON conversions.

## Installation

```bash
go build -o etl ./cmd/cli
```

## Usage

### BridgLink HL7 Pipelines

The BridgLink manager compiles high-level pipeline definitions into ETL jobs. A sample configuration is provided at `examples/bridglink_hl7_to_json.yaml`, which ingests HL7 messages from `examples/data/hl7_sample.hl7`, converts them to JSON/XML, and writes the payload to `examples/data/hl7_output.json`.

Programmatic execution:

```go
cfg, err := config.LoadBridgLinkConfig("examples/bridglink_hl7_to_json.yaml")
if err != nil {
  log.Fatal(err)
}

manager, err := bridglink.NewManager(cfg, etl.NewManager())
if err != nil {
  log.Fatal(err)
}

if _, err := manager.RunPipeline(context.Background(), "hl7_to_json"); err != nil {
  log.Fatal(err)
}
```

The configuration supports multiple connectors, routing rules, and transformation chains. HL7 transformers emit JSON, XML, typed structures, and metadata fields that downstream destinations can leverage. HTTP/webhook destinations can be declared using the `protocol: http` connector.

### Synchronous ETL Execution

Run ETL jobs synchronously from configuration files (requires server to be running):

```bash
# Start the server first in one terminal
./etl runner start

# Then run ETL from a configuration file in another terminal
./etl cli --file config.bcl

# Run ETL from a YAML configuration file
./etl cli --file config.yaml

# Run ETL from a JSON configuration file
./etl cli --file config.json
```

### Asynchronous ETL Execution with Runner

For more advanced usage with job queuing and management via server:

#### 1. Start the Runner Server

```bash
# Start runner server on default port 8080
./etl runner start

# Start runner server on custom port
./etl runner start --port 3000

# Start runner server on custom host and port
./etl runner start --host 0.0.0.0 --port 8080
```

The server will start and accept job submissions. Use Ctrl+C to stop the server gracefully.

#### 2. Submit Jobs to the Runner Server

```bash
# Submit a job to the running server
./etl runner submit --file config.bcl

# Submit a job with custom name
./etl runner submit --file config.bcl --name "Daily Data Sync"

# Submit to a different server
./etl runner submit --file config.bcl --server http://localhost:3000
```

#### 3. Monitor Jobs

```bash
# List all jobs
./etl runner list

# List jobs by status
./etl runner list --status running
./etl runner list --status completed
./etl runner list --status failed

# Get detailed status of a specific job
./etl runner status --job-id exec-1234567890

# Use different server
./etl runner list --server http://localhost:3000
```

#### 4. Cancel Jobs

```bash
# Cancel a job (not yet implemented)
./etl runner cancel --job-id exec-1234567890
```

### Server Mode

Start the web server for a graphical interface:

```bash
# Start server on default port 8080
./etl serve

# Start server on custom port
./etl serve --port 3000

# Start server with custom static files path
./etl serve --static-path ./custom-views

# Enable mock data for development
./etl serve --enable-mocks
```

Once the server is running, open your browser to `http://localhost:8080` to access the web interface.

## Configuration Formats

### BCL (Business Configuration Language)

```bcl
source: {
  type: "csv"
  file: "input.csv"
}

destinations: [{
  type: "postgres"
  host: "localhost"
  database: "mydb"
  table: "output_table"
}]

tables: [{
  old_name: "input.csv"
  new_name: "output_table"
  mapping: {
    "input_column": "output_column"
  }
}]
```

### YAML

```yaml
source:
  type: "csv"
  file: "input.csv"

destinations:
  - type: "postgres"
    host: "localhost"
    database: "mydb"
    table: "output_table"

tables:
  - old_name: "input.csv"
    new_name: "output_table"
    mapping:
      input_column: output_column
```

### JSON

```json
{
  "source": {
    "type": "csv",
    "file": "input.csv"
  },
  "destinations": [{
    "type": "postgres",
    "host": "localhost",
    "database": "mydb",
    "table": "output_table"
  }],
  "tables": [{
    "old_name": "input.csv",
    "new_name": "output_table",
    "mapping": {
      "input_column": "output_column"
    }
  }]
}
```

## Field Formatting Transformers

Table mappings can declare formatting pipelines inside the existing `transformers` block using the new `field_formatter` type. Each entry exposes a per-field mini DSL where you can chain operations with `|`—for example, convert a date into a canonical format and then upper-case it.

```bcl
tables "users" {
  transformers = [{
    name: "format-dates"
    type: "field_formatter"
    options = {
      fields = {
        created_on_iso = "transform(created_on, \"YYYY-MM-DD\", \"DDMMYYYY\")|default(\"1970-01-01\")"
        user_code      = "copy(user_id)|prefix(\"USR-\")|uppercase()"
      }
    }
  }]
}
```

Available operations today:

- `transform(srcField, outputPattern, inputPattern...)` – parse a value (supporting multiple input formats) and emit it using a Moment-style pattern.
- `copy(field)` / `set(value)` – seed the pipeline from another field or literal.
- `default(value)` – fallback when the current value is empty.
- `uppercase()`, `lowercase()`, `trim()` – string utilities.
- `prefix(value)`, `suffix(value)` and `replace(old, new)` – adjust text after formatting.

You can combine as many steps as needed; the pipeline runs sequentially for every record before the destination loaders execute.

## Job Statuses

- **pending**: Job is queued and waiting to be processed
- **running**: Job is currently being executed
- **completed**: Job finished successfully
- **failed**: Job encountered an error during execution
- **cancelled**: Job was cancelled before completion

## Metrics

Each completed job provides detailed metrics:

- **Records Processed**: Total number of records handled
- **Duration**: Time taken to complete the job
- **Extracted**: Number of records extracted from source
- **Mapped**: Number of records successfully mapped
- **Transformed**: Number of records transformed
- **Loaded**: Number of records loaded to destination
- **Errors**: Number of errors encountered
- **WorkerActivities**: Detailed per-worker activity logs

## Examples

See the `examples/` directory for sample configurations and usage examples.

- `config_csv_to_json.bcl` – reads CSV input and writes JSON output while exercising the field formatter transformer.
- `config_hl7_to_json.bcl` – streams HL7 messages from `data/hl7_sample.hl7`, runs the built-in HL7 transformer, and writes enriched JSON records to `data/hl7_messages.json`.

## Development

### Building

```bash
go build ./cmd/cli
```

### Testing

```bash
go test ./...
```

### Web Interface Development

The web interface is located in the `web/` directory. To work on the frontend:

```bash
cd web
npm install
npm run dev
```

## License

[Add your license information here]
