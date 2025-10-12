# ETL Tool

A powerful ETL (Extract, Transform, Load) tool for data processing with both CLI and server modes.

## Features

- **Multiple Configuration Formats**: Support for BCL, YAML, and JSON configurations
- **Asynchronous Job Processing**: Built-in runner for managing ETL jobs asynchronously
- **Web Interface**: Server mode with a web dashboard for job monitoring
- **Real-time Metrics**: Detailed metrics including WorkerActivities tracking
- **Graceful Shutdown**: Proper signal handling for clean shutdowns

## Installation

```bash
go build -o etl ./cmd/cli
```

## Usage

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
