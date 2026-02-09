# Modern ETL Service Roadmap

## Phase 1: Distributed Core (Scalability & Reliability)
- [ ] **Distributed State Manager**:
  - Change `StateManager` to interface backed by Redis/Postgres.
  - Remove local file dependency for `checkpoints`.
- [ ] **Work Queue**:
  - Decouple `Submit()` in Manager.
  - Implement NATS/RabbitMQ producer for jobs.
  - Create `cmd/worker` for consuming and executing jobs.
- [ ] **Leader Election**:
  - Ensure only one Scheduler instance is active using a distributed lock (etcd/Redis).

## Phase 2: Orchestration & Workflow (Flexibility)
- [x] **DAG Engine (`pkg/workflow`)**:
  - [x] Support `dependencies: [job_id]` in configuration.
  - [x] Implement Topological Sort algorithm to determine execution order.
- [ ] **Event Triggers**:
  - Trigger pipelines not just by Schedule (Cron) but by `FileArrival`, `Webhook`, or `DatasetUpdated` events.

## Phase 3: Data Quality & Contracts (Trust)
- [x] **Quality Framework (`pkg/quality`)**:
  - [x] Implement `Validator` interface.
  - [x] Support rules: `NotNull`, `Unique`, `Range`, `Regex`, `CustomQuery`.
  - [x] Add "Circuit Breakers" that stop pipelines if error rate > X%.
- [x] **Schema Registry**:
  - [x] Store valid schemas versions.
  - [x] Detect Schema Drift automatically.

## Phase 4: Observability (Future Proofing)
- [ ] **OpenTelemetry Integration**:
  - Replace custom `Metrics` with OTEL Metrics SDK.
  - Add Distributed Tracing (link HTTP request -> Job -> DB Query).
- [ ] **Data Lineage (`pkg/lineage`)**:
  - Graph storing `Field Level Lineage`.
  - Visualizer APIs.

## Phase 5: Advanced Connectors
- [ ] **CDC Support**: Add `binlog` readers for MySQL/Postgres.
- [ ] **Data Lake Formats**: Support writing to Parquet/Delta Lake.
- [ ] **AI Transformations**: Validated LLM-based cleanups (e.g., "Normalize address using AI").

## Examples & Usage

### 1. Job Dependencies (DAG)
```yaml
id: "daily_report"
dependencies: ["ingest_sales", "ingest_users"]
source: ...
```
To run:
```go
dag := workflow.NewDAG()
dag.AddNode("ingest_sales")
dag.AddNode("ingest_users")
dag.AddNode("daily_report", "ingest_sales", "ingest_users")

plan, _ := dag.GetExecutionPlan()
// plan = [["ingest_sales", "ingest_users"], ["daily_report"]]
```

### 2. Quality Rules & Circuit Breaker
```yaml
quality:
  circuit_breaker:
    error_threshold: 5.0 # Stop if > 5% errors
    min_record_count: 100
  rules:
    - field: "email"
      rule: "Email"
    - field: "age"
      rule: "Range"
      args: [18, 120]
```

### 3. Schema Drift Detection
```go
registry := quality.NewSchemaRegistry()
// Record 1
registry.Register(quality.InferSchema(rec1))

// Record 2 (different structure)
isDrift, newSchema := registry.DetectDrift(rec2)
if isDrift {
    log.Println("Schema changed!")
}
```
