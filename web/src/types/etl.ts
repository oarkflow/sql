export type IntegrationType = "file" | "database" | "service";

export type IntegrationStatus = "connected" | "disconnected" | "error";

export interface Integration {
  id: string;
  name: string;
  type: IntegrationType;
  status: IntegrationStatus;
  description: string;
  config: Record<string, any>;
  createdAt: string;
  lastUsed?: string;
}

export type RunStatus = "running" | "success" | "failed" | "pending";

export interface ETLRun {
  id: string;
  query: string;
  status: RunStatus;
  startTime: string;
  endTime?: string;
  duration?: number;
  rowsProcessed?: number;
  error?: string;
  integrations: string[];
}

export interface QueryResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
  executionTime: number;
}

export interface ETLSource {
  key: string;
  type: string;
  format?: string;
  host?: string;
  port?: number;
  driver?: string;
  username?: string;
  password?: string;
  database?: string;
  file?: string;
  disablelogger?: boolean;
}

export interface ETLDestination {
  key: string;
  type: string;
  format?: string;
  host?: string;
  port?: number;
  driver?: string;
  username?: string;
  password?: string;
  database?: string;
  file?: string;
  disablelogger?: boolean;
}

export interface Deduplication {
  enabled: boolean;
  field: string;
}

export interface Checkpoint {
  file: string;
  field: string;
}

export interface Lookup {
  type: string;
  file: string;
  key: string;
}

export interface Aggregation {
  source_field: string;
  func: "count" | "sum" | "min" | "max" | "avg";
  output_field: string;
}

export interface Aggregator {
  group_by: string[];
  aggregations: Aggregation[];
}

export interface ETLTableMapping {
  name: string;
  old_name?: string;
  dest_key: string;
  migrate: boolean;
  clone_source: boolean;
  truncate_dest: boolean;
  key_value_table?: boolean;
  auto_create_table?: boolean;
  enable_batch?: boolean;
  batch_size: number;
  update_sequence?: boolean;
  skip_store_error?: boolean;
  mapping: Record<string, string>;
  normalize_schema?: Record<string, string>;
  extra_values?: Record<string, string>;
  key_field?: string;
  value_field?: string;
  include_fields?: string[];
  exclude_fields?: string[];
  aggregator?: Aggregator;
}

export interface ETLPipeline {
  id: string;
  name: string;
  description: string;
  sources: ETLSource[];
  destinations: ETLDestination[];
  tables: ETLTableMapping[];
  deduplication?: Deduplication;
  checkpoint?: Checkpoint;
  lookups?: Lookup[];
  streaming_mode?: boolean;
  distributed_mode?: boolean;
  status: "active" | "inactive" | "draft";
  createdAt: string;
  lastRun?: string;
}
