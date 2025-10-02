import { Integration, ETLRun, QueryResult, ETLPipeline } from "@/types/etl";

// Mock data
const mockIntegrations: Integration[] = [
  {
    id: "1",
    name: "users.csv",
    type: "file",
    status: "connected",
    description: "User data CSV file",
    config: { path: "/data/users.csv", format: "csv" },
    createdAt: "2025-09-15T10:30:00Z",
    lastUsed: "2025-09-28T14:20:00Z",
  },
  {
    id: "2",
    name: "PostgreSQL DB",
    type: "database",
    status: "connected",
    description: "Production database",
    config: { host: "localhost", port: 5432, database: "prod_db" },
    createdAt: "2025-09-10T08:00:00Z",
    lastUsed: "2025-09-29T09:15:00Z",
  },
  {
    id: "3",
    name: "Salesforce API",
    type: "service",
    status: "connected",
    description: "Salesforce data integration",
    config: { endpoint: "https://api.salesforce.com", version: "v52.0" },
    createdAt: "2025-09-12T12:00:00Z",
    lastUsed: "2025-09-27T16:45:00Z",
  },
  {
    id: "4",
    name: "orders.json",
    type: "file",
    status: "connected",
    description: "Order records JSON",
    config: { path: "/data/orders.json", format: "json" },
    createdAt: "2025-09-18T11:20:00Z",
  },
];

const mockRuns: ETLRun[] = [
  {
    id: "1",
    query: "SELECT id, first_name FROM read_file('users.csv') WHERE id = 1",
    status: "success",
    startTime: "2025-09-29T10:30:00Z",
    endTime: "2025-09-29T10:30:02Z",
    duration: 2.3,
    rowsProcessed: 1,
    integrations: ["users.csv"],
  },
  {
    id: "2",
    query: "SELECT * FROM read_db('PostgreSQL DB', 'customers') LIMIT 100",
    status: "success",
    startTime: "2025-09-29T09:15:00Z",
    endTime: "2025-09-29T09:15:05Z",
    duration: 5.1,
    rowsProcessed: 100,
    integrations: ["PostgreSQL DB"],
  },
  {
    id: "3",
    query: "SELECT id FROM read_file('users.csv') WHERE id > 4 UNION SELECT id FROM read_file('orders.json')",
    status: "failed",
    startTime: "2025-09-28T14:20:00Z",
    endTime: "2025-09-28T14:20:03Z",
    duration: 3.5,
    error: "Column type mismatch in UNION",
    integrations: ["users.csv", "orders.json"],
  },
  {
    id: "4",
    query: "SELECT * FROM read_service('Salesforce API', 'accounts')",
    status: "running",
    startTime: "2025-09-29T11:00:00Z",
    integrations: ["Salesforce API"],
  },
];

const mockPipelines: ETLPipeline[] = [
  {
    id: "1",
    name: "User Data Migration",
    description: "Migrate user data from CSV to PostgreSQL",
    sources: [
      { key: "users_csv", type: "csv", file: "/data/users.csv" }
    ],
    destinations: [
      { key: "prod_db", type: "postgres", host: "localhost", port: 5432, database: "prod_db", username: "admin" }
    ],
    tables: [
      {
        name: "users",
        old_name: "users",
        dest_key: "prod_db",
        migrate: true,
        clone_source: false,
        truncate_dest: false,
        key_value_table: false,
        batch_size: 1000,
        mapping: { id: "user_id", name: "full_name", email: "email_address" }
      }
    ],
    status: "active",
    createdAt: "2025-09-20T10:00:00Z",
    lastRun: "2025-09-29T08:30:00Z"
  },
  {
    id: "2",
    name: "Sales Data Sync",
    description: "Sync sales data from Salesforce to MySQL",
    sources: [
      { key: "salesforce", type: "service" }
    ],
    destinations: [
      { key: "analytics_db", type: "mysql", host: "localhost", port: 3306, database: "analytics" }
    ],
    tables: [
      {
        name: "sales",
        dest_key: "analytics_db",
        migrate: true,
        clone_source: true,
        truncate_dest: true,
        key_value_table: false,
        batch_size: 500,
        mapping: {}
      }
    ],
    status: "active",
    createdAt: "2025-09-18T14:00:00Z",
    lastRun: "2025-09-29T10:00:00Z"
  }
];

// Mock API functions
export const mockApi = {
  // Integrations
  getIntegrations: async (): Promise<Integration[]> => {
    await delay(300);
    return mockIntegrations;
  },

  getIntegration: async (id: string): Promise<Integration | undefined> => {
    await delay(200);
    return mockIntegrations.find((i) => i.id === id);
  },

  createIntegration: async (integration: Omit<Integration, "id" | "createdAt">): Promise<Integration> => {
    await delay(500);
    const newIntegration: Integration = {
      ...integration,
      id: String(mockIntegrations.length + 1),
      createdAt: new Date().toISOString(),
    };
    mockIntegrations.push(newIntegration);
    return newIntegration;
  },

  updateIntegration: async (id: string, updates: Partial<Integration>): Promise<Integration> => {
    await delay(400);
    const index = mockIntegrations.findIndex((i) => i.id === id);
    if (index === -1) throw new Error("Integration not found");
    mockIntegrations[index] = { ...mockIntegrations[index], ...updates };
    return mockIntegrations[index];
  },

  deleteIntegration: async (id: string): Promise<void> => {
    await delay(300);
    const index = mockIntegrations.findIndex((i) => i.id === id);
    if (index !== -1) {
      mockIntegrations.splice(index, 1);
    }
  },

  // ETL Runs
  getRuns: async (): Promise<ETLRun[]> => {
    await delay(300);
    return mockRuns.sort((a, b) => new Date(b.startTime).getTime() - new Date(a.startTime).getTime());
  },

  getRun: async (id: string): Promise<ETLRun | undefined> => {
    await delay(200);
    return mockRuns.find((r) => r.id === id);
  },

  // Query execution
  executeQuery: async (query: string): Promise<QueryResult> => {
    await delay(1500);

    // Simulate query execution
    const mockResult: QueryResult = {
      columns: ["id", "first_name", "last_name", "email"],
      rows: [
        [1, "John", "Doe", "john@example.com"],
        [2, "Jane", "Smith", "jane@example.com"],
        [3, "Bob", "Johnson", "bob@example.com"],
      ],
      rowCount: 3,
      executionTime: 1.5,
    };

    // Create a run record
    const newRun: ETLRun = {
      id: String(mockRuns.length + 1),
      query,
      status: "success",
      startTime: new Date().toISOString(),
      endTime: new Date(Date.now() + 1500).toISOString(),
      duration: 1.5,
      rowsProcessed: mockResult.rowCount,
      integrations: extractIntegrationsFromQuery(query),
    };
    mockRuns.unshift(newRun);

    return mockResult;
  },

  // ETL Pipelines
  getPipelines: async (): Promise<ETLPipeline[]> => {
    await delay(300);
    return mockPipelines;
  },

  getPipeline: async (id: string): Promise<ETLPipeline | undefined> => {
    await delay(200);
    return mockPipelines.find((p) => p.id === id);
  },

  createPipeline: async (pipeline: Omit<ETLPipeline, "id" | "createdAt">): Promise<ETLPipeline> => {
    await delay(500);
    const newPipeline: ETLPipeline = {
      ...pipeline,
      id: String(mockPipelines.length + 1),
      createdAt: new Date().toISOString(),
    };
    mockPipelines.push(newPipeline);
    return newPipeline;
  },

  updatePipeline: async (id: string, updates: Partial<ETLPipeline>): Promise<ETLPipeline> => {
    await delay(400);
    const index = mockPipelines.findIndex((p) => p.id === id);
    if (index === -1) throw new Error("Pipeline not found");
    mockPipelines[index] = { ...mockPipelines[index], ...updates };
    return mockPipelines[index];
  },

  deletePipeline: async (id: string): Promise<void> => {
    await delay(300);
    const index = mockPipelines.findIndex((p) => p.id === id);
    if (index !== -1) {
      mockPipelines.splice(index, 1);
    }
  },

  runPipeline: async (id: string): Promise<ETLRun> => {
    await delay(1000);
    const pipeline = mockPipelines.find((p) => p.id === id);
    if (!pipeline) throw new Error("Pipeline not found");

    const newRun: ETLRun = {
      id: String(mockRuns.length + 1),
      query: `Pipeline: ${pipeline.name}`,
      status: "success",
      startTime: new Date().toISOString(),
      endTime: new Date(Date.now() + 1000).toISOString(),
      duration: 1.0,
      rowsProcessed: Math.floor(Math.random() * 1000),
      integrations: pipeline.sources.map(s => s.key),
    };
    mockRuns.unshift(newRun);

    // Update last run
    const index = mockPipelines.findIndex((p) => p.id === id);
    if (index !== -1) {
      mockPipelines[index].lastRun = newRun.startTime;
    }

    return newRun;
  },
};

// Helper functions
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractIntegrationsFromQuery(query: string): string[] {
  const integrations: string[] = [];
  const fileMatches = query.match(/read_file\(['"]([^'"]+)['"]\)/g);
  const dbMatches = query.match(/read_db\(['"]([^'"]+)['"]/g);
  const serviceMatches = query.match(/read_service\(['"]([^'"]+)['"]/g);

  if (fileMatches) {
    fileMatches.forEach((match) => {
      const name = match.match(/['"]([^'"]+)['"]/)?.[1];
      if (name) integrations.push(name);
    });
  }
  if (dbMatches) {
    dbMatches.forEach((match) => {
      const name = match.match(/['"]([^'"]+)['"]/)?.[1];
      if (name) integrations.push(name);
    });
  }
  if (serviceMatches) {
    serviceMatches.forEach((match) => {
      const name = match.match(/['"]([^'"]+)['"]/)?.[1];
      if (name) integrations.push(name);
    });
  }

  return integrations;
}
