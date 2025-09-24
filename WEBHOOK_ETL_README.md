# Robust Webhook ETL Integration System

This system provides a comprehensive solution for receiving webhook data, parsing it with multiple format support (HL7, JSON, XML), and processing it through configurable ETL pipelines.

## Features

- **Multi-Format Support**: Automatically detects and parses HL7, JSON, XML, and other data formats
- **ETL Integration**: Configurable ETL pipelines for data transformation and storage
- **Security**: HMAC signature verification for webhook authenticity
- **Scalability**: Worker pool architecture for concurrent processing
- **Health Monitoring**: Built-in health check endpoint
- **Robust Error Handling**: Comprehensive logging and error recovery

## Architecture

```
Webhook Request → Parser Detection → Data Extraction → ETL Pipeline → Database/Storage
```

## Configuration

### Basic Webhook Configuration

```go
webhookConfig := &webhook.Config{
    Port:       "8080",
    MaxWorkers: 10,
    Secret:     "your-webhook-secret", // For HMAC verification
    DataTypes:  []string{"hl7", "json", "xml"},
}
```

### ETL Pipeline Configuration

```go
ETLPipelines: map[string]*webhook.ETLPipeline{
    "hl7": {
        Name:        "HL7 Patient Data Pipeline",
        Description: "Process HL7 patient data and store in database",
        DataType:    "hl7",
        Source: config.DataConfig{
            Type:   "webhook",
            Driver: "memory",
        },
        Destination: config.DataConfig{
            Type:     "postgresql",
            Driver:   "postgres",
            Host:     "localhost",
            Username: "postgres",
            Password: "postgres",
            Port:     5432,
            Database: "healthcare_db",
        },
        Mapping: config.TableMapping{
            OldName:         "hl7_data",
            NewName:         "patient_records",
            AutoCreateTable: true,
            Mapping: map[string]string{
                "PID.3": "patient_id",
                "PID.5": "patient_name",
                "PID.7": "date_of_birth",
                "PID.8": "gender",
                "PID.11": "address",
            },
        },
        Enabled: true,
    },
}
```

## Usage Examples

### Starting the Webhook Server

```go
server := webhook.NewWebhookServer(webhookConfig)
if err := server.Start(); err != nil {
    log.Fatalf("Failed to start webhook server: %v", err)
}
```

### Sending Test Data

#### HL7 Data
```bash
curl -X POST http://localhost:8080/webhook \
     -H 'Content-Type: text/plain' \
     -d 'MSH|^~\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20230924120000||ADT^A01|123456|P|2.5|||AL|NE
EVN|A01|20230924120000
PID|1||123456^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|I|WARD1^BED1||||DRSMITH|||MED|||||ADM||SELF||||||20230924120000'
```

#### JSON Data
```bash
curl -X POST http://localhost:8080/webhook \
     -H 'Content-Type: application/json' \
     -d '{"id": 123, "name": "John Doe", "email": "john@example.com"}'
```

#### With HMAC Signature
```bash
# Calculate HMAC signature
secret="your-webhook-secret"
payload='{"id": 123, "name": "John Doe"}'
signature=$(echo -n "$payload" | openssl dgst -sha256 -hmac "$secret" -binary | xxd -p | tr -d '\n')

curl -X POST http://localhost:8080/webhook \
     -H "X-Hub-Signature: sha256=$signature" \
     -H 'Content-Type: application/json' \
     -d "$payload"
```

## HL7 vs Mirth Comparison

| Feature | Current HL7 Parser | Mirth Connect |
|---------|-------------------|---------------|
| **Parsing** | ✅ Full HL7 v2.x support | ✅ Full HL7 support |
| **Message Types** | ✅ ADT, ORU, ORM, DFT, SIU | ✅ All HL7 message types |
| **Routing** | ❌ Basic | ✅ Advanced routing engine |
| **Transformation** | ⚠️ Limited (via ETL) | ✅ Built-in transformers |
| **Connectors** | ❌ Limited | ✅ 300+ connectors |
| **GUI** | ❌ None | ✅ Web-based GUI |
| **Deployment** | ✅ Lightweight Go binary | ⚠️ Java-based (heavier) |
| **Integration** | ✅ ETL-focused | ✅ Broad integration |

## Data Flow

1. **Webhook Reception**: Receives HTTP POST requests with data
2. **Signature Verification**: Validates HMAC signatures if configured
3. **Parser Detection**: Automatically detects data format (HL7, JSON, XML)
4. **Data Parsing**: Parses data into structured format
5. **Field Extraction**: Extracts relevant fields for processing
6. **ETL Processing**: Transforms and loads data according to pipeline configuration
7. **Storage**: Stores processed data in configured destination

## Supported Data Types

### HL7 Messages
- **ADT^A01**: Patient Admission
- **ORU^R01**: Lab Results
- **ORM^O01**: Pharmacy Orders
- **DFT^P03**: Financial Transactions
- **SIU^S12**: Appointment Scheduling

### Other Formats
- **JSON**: Generic JSON data
- **XML**: XML documents
- **Plain Text**: Raw text data

## ETL Pipeline Features

- **Source Configuration**: Define data sources (webhook, files, databases)
- **Destination Configuration**: Configure storage targets (PostgreSQL, MySQL, etc.)
- **Field Mapping**: Map source fields to destination columns
- **Data Transformation**: Apply transformations during ETL process
- **Schema Management**: Auto-create tables and manage schemas

## Monitoring and Health Checks

- **Health Endpoint**: `GET /health` for service health
- **Webhook Endpoint**: `POST /webhook` for data ingestion
- **Comprehensive Logging**: Detailed logs for debugging and monitoring

## Production Considerations

1. **Security**: Always use HMAC signatures in production
2. **Scalability**: Adjust MaxWorkers based on load
3. **Database Connection Pooling**: Configure appropriate connection limits
4. **Error Handling**: Implement proper error handling and retries
5. **Monitoring**: Add metrics collection and alerting
6. **Backup**: Regular backups of processed data

## Example Implementation

See `examples/webhook_etl_server.go` for a complete working example that demonstrates:

- Webhook server configuration
- ETL pipeline setup for HL7 and JSON data
- Database integration
- Field mapping and transformation

This system provides a robust, scalable solution for webhook-based data ingestion with powerful ETL capabilities, making it suitable for healthcare integrations, API data processing, and other webhook-driven workflows.</content>
<parameter name="filePath">/home/sujit/Projects/sql/WEBHOOK_ETL_README.md
