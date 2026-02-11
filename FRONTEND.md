# Frontend Architecture & Requirements for Enterprise ETL Platform

## 1. Executive Summary
This document outlines the comprehensive requirements for building a production-grade frontend for the existing ETL engine. The goal is to evolve the current `views/etl.html` prototype into a fully featured Single Page Application (SPA) that exposes the full power of the backend (DAGs, Quality Rules, Distributed Execution, Observability).

## 2. Core Feature Requirements

### 2.1. Unified Dashboard
**Goal:** Provide a high-level overview of system health and activity.
- **Global Stats:** Total active pipelines, messages processed/sec, global error rate.
- **Worker Cluster Status:** Display active workers, CPU/Memory usage (if available via metrics), and leader election status.
- **Recent Activity Feed:** Real-time log of started/completed jobs and critical alerts.
- **Circuit Breaker Status:** Visual indicators for open/half-open circuit breakers across services.

### 2.2. Visual Pipeline Designer (Next-Gen)
**Goal:** A no-code/low-code builder for complex ETL Directed Acyclic Graphs (DAGs).
- **Canvas:** Infinite canvas with drag-and-drop nodes.
- **Node Types:**
  - **Source:** Configurable adapters (SQL, CSV, HL7, API).
  - **Transform:** Field mapping (visual connector lines), specialized transforms (Regex, Norm).
  - **Quality:** Drag-and-drop validation rules (NotNull, Email, Range).
  - **Logic:** Branching, Filters, Aggregations.
  - **Load:** Destination configuration.
- **Configuration Panel:** Sidebar that opens when a node is selected to edit arbitrary JSON/YAML properties.
- **Dependencies:** Visually draw lines to establish `dag.AddDependency()` relationships.
- **Code View Split:** Bi-directional syncing between Visual Canvas and YAML/BCL editor (Monaco).

### 2.3. Orchestration & Execution Monitor
**Goal:** Visualize the execution plan of complex DAGs.
- **DAG Visualizer:** Render the topological sort layers (Plan Layer 1 -> Layer 2).
- **Real-time Progress:**
  - Nodes change color based on status (Pending=Gray, Running=Blue, Success=Green, Failed=Red).
  - Progress bars per node (Record Count / Total).
- **Live Logs:** Streaming logs from the specific node execution.
- **Manual Triggers:** Button to "Run Now" with custom runtime parameters (e.g., specific date range).

### 2.4. Observability & Debugging
- **Metric Charts:** Time-series visualization of `Extracted`, `Transformed`, `Loaded` counts.
- **Dead Letter Queue (DLQ) Explorer:**
  - Table view of failed messages.
  - "Inspect" modal to see the payload and error message.
  - "Replay" action to send messages back to the source queue.
- **Trace View:** If OpenTelemetry is integrated, show waterfall traces of a single job.

### 2.5. Data Quality & Schema Management
- **Rule Builder:** UI to compose validation logic without writing code (e.g., "Email" rule on "user_email" field).
- **Drift Detection:** Alerting UI showing diffs between expected schema and incoming data.
- **Report Card:** summary of data quality scores per pipeline.

## 3. Detailed UI/UX Specifications

### 3.1. Tech Stack (Recommended)
- **Framework:** React or Vue.3 (to leverage ecosystem).
- **State Management:** TanStack Query (React Query) for API syncing + Zustand/Pinia for UI state.
- **Visualization:** React Flow / Vue Flow for the DAG canvas. Recharts/Chart.js for metrics.
- **Editor:** Monaco Editor (VS Code core) for config editing.
- **Styling:** Tailwind CSS + ShadcnUI (or similar component library) for consistency.

### 3.2. Navigation Structure
1.  **Overview** (Dashboard)
2.  **Pipeline Studio** (List & Designer)
3.  **Runs** (History of executions)
4.  **Connectors** (Manage Source/Dest credentials)
5.  **Quality** (Rules & Drifts)
6.  **Settings** (Cluster config, Users)

### 3.3. Key Screen Layouts

#### **Pipeline Designer**
| Section | Layout |
| :--- | :--- |
| **Top Bar** | Name, Status Toggle, "Save", "Validate", "Deploy" |
| **Left Sidebar** | Component Palette (Sources, Transforms, Logic) - Draggable |
| **Center** | Infinite Grid Canvas (React Flow) |
| **Right Sidebar** | Contextual Properties Panel (Edit selected node config) |
| **Bottom Panel** | Validation errors, Dry-run results console |

#### **Execution Details**
| Section | Layout |
| :--- | :--- |
| **Header** | Run ID, Trigger Source (Cron/Manual), Duration, Status |
| **Main View** | Read-only version of DAG. Nodes colored by completion status. |
| **Side Panel** | Clicking a node shows: `Input Records`, `Output Records`, `Error Logs`. |

## 4. API Integration Requirements
To support this frontend, the Go backend must expose the following REST/WebSocket endpoints:

### **Pipeline Management**
- `GET /api/pipelines` - List summaries.
- `GET /api/pipelines/:id` - Get full config/DAG.
- `POST /api/pipelines` - Create/Update.
- `POST /api/pipelines/:id/validate` - Dry-run validation.

### **Orchestration**
- `POST /api/run/:pipeline_id` - Trigger execution.
- `GET /api/runs/:pipeline_id` - History.
- `WS /api/ws/runs/:run_id` - WebSocket for real-time node status updates.

### **Observability**
- `GET /api/metrics/global` - Aggregated stats.
- `GET /api/dlq` - List failed items.
- `POST /api/dlq/replay` - Retry items.

## 5. Mobile Responsiveness
- **Primary Focus:** Desktop/Tablet (Landscape). Complex DAG editing is not required on mobile.
- **Mobile View:** Read-only Dashboard & Status checking. Ability to "Cancel" or "Trigger" jobs.

## 6. Future Considerations (Roadmap Phase 4 & 5)
- **AI Integration:** Chat interface to "Generate Pipeline from description".
- **Lineage Graph:** A global graph showing how data flows across *all* pipelines (Table A -> Pipeline X -> Table B).
