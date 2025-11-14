# BridgeLink - Modern ETL Engine in Go

A high-performance, extensible ETL (Extract, Transform, Load) engine written in Go, designed as a modern alternative to Mirth Connect. BridgeLink uses YAML/JSON configuration files and provides a plugin-based architecture for connectors, transformers, and helpers.

## Features

- **High Performance**: Built with Go's concurrency primitives for maximum throughput
- **Extensible Architecture**: Plugin-based system for easy extension
- **Configuration-Driven**: YAML/JSON configuration files for defining channels
- **Multiple Connectors**: HTTP, File, HL7 MLLP, Database support
- **Flexible Transformations**: Chain multiple transformers together
- **Error Handling**: Configurable error handling strategies (continue, stop, retry)
- **Worker Pools**: Parallel message processing with configurable workers
- **Type-Safe**: Strongly-typed interfaces for all components

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Source    │────▶│ Transformers │────▶│  Destinations   │
│  Connector  │     │   (Chain)    │     │   (Multiple)    │
└─────────────┘     └──────────────┘     └─────────────────┘
```

### Core Components

1. **Connectors**:
   - `SourceConnector`: Reads data from sources (HL7 MLLP, HTTP, Files, etc.)
   - `DestinationConnector`: Writes data to destinations (HTTP, Database, Files, etc.)

2. **Transformers**: Transform messages between formats (HL7→JSON, CSV→JSON, etc.)

3. **Helpers**: Utility functions for data manipulation

4. **Channel**: An ETL pipeline combining source, transformers, and destinations

5. **Engine**: Manages multiple channels and their lifecycle

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/bridgelink.git
cd bridgelink

# Initialize Go module
go mod init github.com/yourusername/bridgelink
go mod tidy

# Build
go build -o bridgelink

# Create output directory
mkdir -p output
```

## Quick Start

### 1. Configure a Channel

Create `channel_hl7.yaml`:

```yaml
id: hl7-to-json-channel
name: HL7 MLLP to JSON Converter
enabled: true

source:
  type: hl7_mllp
  config:
    host: 0.0.0.0
    port: 2575

transformers:
  - type: hl7_to_json
    config:
      pretty_print: true

destinations:
  - type: console
    config:
      pretty_print: true

performance:
  workers: 4
  buffer_size: 100
```

### 2. Run BridgeLink

```bash
./bridgelink
```

### 3. Send HL7 Messages

Using a simple Python sender:

```python
import socket

# Sample HL7 message
hl7_message = """MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20231114120000||ADT^A01|MSG00001|P|2.5
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701
PV1|1|I|W^389^1^Hospital^^ICU^2|||2234^Doctor^Attending|||SUR||||ADM|A0|"""

# Connect and send
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 2575))

# MLLP format: <VT>message<FS><CR>
message = b'\x0B' + hl7_message.encode() + b'\x1C\x0D'
sock.send(message)

# Receive ACK
ack = sock.recv(1024)
print(f"ACK: {ack}")

sock.close()
```

## Configuration Reference

### Channel Configuration

```yaml
id: unique-channel-id
name: Human Readable Name
description: Channel description
enabled: true|false

source:
  type: connector_type
  config:
    # Source-specific configuration

transformers:
  - type: transformer_type
    config:
      # Transformer-specific configuration

destinations:
  - type: connector_type
    config:
      # Destination-specific configuration

error_handling:
  on_error: continue|stop|retry
  retry_attempts: 3
  retry_delay: 5s

performance:
  workers: 4          # Number of parallel workers
  batch_size: 10      # Batch size for processing
  buffer_size: 100    # Message buffer size
```

## Built-in Connectors

### Sources

#### 1. HL7 MLLP Source
```yaml
source:
  type: hl7_mllp
  config:
    host: 0.0.0.0
    port: 2575
```

#### 2. HTTP Source
```yaml
source:
  type: http
  config:
    url: https://api.example.com/data
    method: GET
    poll_interval_seconds: 60
    headers:
      Authorization: Bearer token
```

#### 3. File Source
```yaml
source:
  type: file
  config:
    directory: ./input
    pattern: "*.txt"
    delete_after_read: true
    poll_interval_seconds: 5
```

### Destinations

#### 1. Console Destination
```yaml
destinations:
  - type: console
    config:
      pretty_print: true
```

#### 2. JSON File Destination
```yaml
destinations:
  - type: json_file
    config:
      directory: ./output
      pretty_print: true
```

#### 3. HTTP Destination
```yaml
destinations:
  - type: http
    config:
      url: https://api.example.com/webhook
      method: POST
      timeout_seconds: 30
      headers:
        Content-Type: application/json
```

#### 4. Database Destination
```yaml
destinations:
  - type: database
    config:
      driver: postgres
      dsn: "host=localhost user=postgres password=secret dbname=bridgelink"
      table: messages
```

## Built-in Transformers

### 1. HL7 to JSON
```yaml
transformers:
  - type: hl7_to_json
    config:
      pretty_print: true
```

**Output Format:**
```json
{
  "MSH": {
    "segment_type": "MSH",
    "sending_application": "SendingApp",
    "sending_facility": "SendingFac",
    "message_type": "ADT^A01",
    "message_control_id": "MSG00001"
  },
  "PID": {
    "segment_type": "PID",
    "patient_id": "12345",
    "patient_name": ["Doe", "John", "A"],
    "date_of_birth": "19800101"
  }
}
```

### 2. CSV to JSON
```yaml
transformers:
  - type: csv_to_json
    config:
      delimiter: ","
      has_header: true
```

## Creating Custom Connectors

### Custom Source Connector

```go
package main

import (
    "context"
    "time"
)

type CustomSource struct {
    msgChan chan Message
    config  map[string]interface{}
}

func NewCustomSource() *CustomSource {
    return &CustomSource{
        msgChan: make(chan Message, 100),
    }
}

func (s *CustomSource) Initialize(config map[string]interface{}) error {
    s.config = config
    // Initialize your source
    return nil
}

func (s *CustomSource) Start(ctx context.Context) error {
    // Start reading from your source
    go s.readData(ctx)
    return nil
}

func (s *CustomSource) readData(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
            // Read your data
            msg := Message{
                ID:        uuid.New().String(),
                Timestamp: time.Now(),
                Data:      "your data here",
                Metadata:  make(map[string]interface{}),
            }
            s.msgChan <- msg
        }
    }
}

func (s *CustomSource) Read(ctx context.Context) (<-chan Message, error) {
    return s.msgChan, nil
}

func (s *CustomSource) Stop() error {
    close(s.msgChan)
    return nil
}

func (s *CustomSource) GetType() string {
    return "custom_source"
}

// Register in main.go
func main() {
    registry := NewConnectorRegistry()
    registry.RegisterSource("custom_source", func() SourceConnector {
        return NewCustomSource()
    })
    // ... rest of setup
}
```

### Custom Transformer

```go
type CustomTransformer struct {
    config map[string]interface{}
}

func NewCustomTransformer() *CustomTransformer {
    return &CustomTransformer{}
}

func (t *CustomTransformer) Initialize(config map[string]interface{}) error {
    t.config = config
    return nil
}

func (t *CustomTransformer) Transform(ctx context.Context, msg Message) (Message, error) {
    // Transform your data
    transformedData := someTransformation(msg.Data)

    msg.Data = transformedData
    msg.Metadata["transformed_by"] = "custom_transformer"

    return msg, nil
}

func (t *CustomTransformer) GetName() string {
    return "custom_transformer"
}
```

## Performance Tuning

### Worker Configuration
```yaml
performance:
  workers: 8           # Increase for CPU-bound transformations
  buffer_size: 1000    # Larger buffer for high-throughput scenarios
  batch_size: 50       # Batch processing where applicable
```

### Best Practices

1. **Workers**: Set to number of CPU cores for CPU-bound tasks
2. **Buffer Size**: Increase for high message volumes to prevent backpressure
3. **Batch Size**: Use batching for database writes to improve throughput
4. **Error Handling**: Use `continue` for non-critical errors in high-volume scenarios

## Testing HL7 Integration

### Sample HL7 Messages

**ADT (Admission/Discharge/Transfer):**
```
MSH|^~\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20231114120000||ADT^A01|MSG00001|P|2.5
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M|||123 Main St^^Springfield^IL^62701
PV1|1|I|W^389^1^Hospital^^ICU^2|||2234^Doctor^Attending|||SUR||||ADM|A0|
```

**ORU (Observation Result):**
```
MSH|^~\&|LAB|Hospital|CLINIC|Hospital|20231114120000||ORU^R01|MSG00002|P|2.5
PID|1||12345^^^Hospital^MR||Doe^John^A||19800101|M
OBR|1||LAB12345|CBC^Complete Blood Count|||20231114120000
OBX|1|NM|WBC^White Blood Count||7.5|10^9/L|4.0-11.0|N|||F
OBX|2|NM|RBC^Red Blood Count||4.5|10^12/L|4.2-5.4|N|||F
```

### Testing Tools

1. **HL7 Sender Script** (Python)
2. **Mirth Connect** - Can send test messages to BridgeLink
3. **7Edit** - HL7 message editor and sender
4. **Telnet** - Quick testing: `telnet localhost 2575`

## Monitoring & Logging

BridgeLink logs all important events:

- Channel start/stop
- Message processing
- Transformation errors
- Connection issues
- Performance metrics

Example output:
```
2024/11/14 12:00:00 BridgeLink engine started
2024/11/14 12:00:00 Starting channel: HL7 MLLP to JSON Converter
2024/11/14 12:00:00 HL7 MLLP listener started on 0.0.0.0:2575
2024/11/14 12:00:15 New connection from 127.0.0.1:54321
2024/11/14 12:00:15 Written message to ./output/abc-123.json
```

## Production Deployment

### Systemd Service

Create `/etc/systemd/system/bridgelink.service`:

```ini
[Unit]
Description=BridgeLink ETL Engine
After=network.target

[Service]
Type=simple
User=bridgelink
WorkingDirectory=/opt/bridgelink
ExecStart=/opt/bridgelink/bridgelink
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable bridgelink
sudo systemctl start bridgelink
sudo systemctl status bridgelink
```

### Docker Deployment

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o bridgelink

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/bridgelink .
COPY --from=builder /app/*.yaml .
EXPOSE 2575
CMD ["./bridgelink"]
```

Build and run:
```bash
docker build -t bridgelink .
docker run -d -p 2575:2575 -v $(pwd)/output:/root/output bridgelink
```

## Roadmap

- [ ] Web-based admin UI
- [ ] Metrics and monitoring (Prometheus integration)
- [ ] More built-in connectors (Kafka, RabbitMQ, SFTP)
- [ ] Advanced transformers (FHIR, X12, XML)
- [ ] Message replay and audit trail
- [ ] Distributed deployment support
- [ ] GraphQL API for management

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - See LICENSE file for details

## Support

- GitHub Issues: Report bugs and request features
- Documentation: See `docs/` directory
- Examples: See `examples/` directory

## Comparison with Mirth Connect

| Feature | BridgeLink | Mirth Connect |
|---------|------------|---------------|
| Language | Go | Java |
| Configuration | YAML/JSON | XML/GUI |
| Performance | High (native concurrency) | Good |
| Memory Usage | Low | High |
| Deployment | Single binary | JVM required |
| Extensibility | Code-based plugins | JavaScript |
| Learning Curve | Moderate | Steep |
| Admin UI | CLI (UI planned) | Full GUI |

## Acknowledgments

- Inspired by Mirth Connect
- Built with Go's excellent standard library
- Community contributions welcome!
