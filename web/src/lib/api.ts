import { Integration, ETLRun, QueryResult, ETLPipeline } from "@/types/etl";

const API_BASE_URL = 'http://localhost:3000';

class ApiError extends Error {
    constructor(public status: number, message: string) {
        super(message);
        this.name = 'ApiError';
    }
}

async function apiRequest<T>(
    endpoint: string,
    options: RequestInit = {}
): Promise<T> {
    const url = `${API_BASE_URL}${endpoint}`;

    const response = await fetch(url, {
        headers: {
            'Content-Type': 'application/json',
            ...options.headers,
        },
        ...options,
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new ApiError(response.status, errorText || `HTTP ${response.status}`);
    }

    return response.json();
}

export const api = {
    // Integrations
    getIntegrations: (): Promise<Integration[]> =>
        apiRequest('/api/integrations'),

    getIntegration: (id: string): Promise<Integration> =>
        apiRequest(`/api/integrations/${id}`),

    createIntegration: (integration: Omit<Integration, 'id' | 'createdAt'>): Promise<Integration> =>
        apiRequest('/api/integrations', {
            method: 'POST',
            body: JSON.stringify(integration),
        }),

    updateIntegration: (id: string, updates: Partial<Integration>): Promise<Integration> =>
        apiRequest(`/api/integrations/${id}`, {
            method: 'PUT',
            body: JSON.stringify(updates),
        }),

    deleteIntegration: (id: string): Promise<void> =>
        apiRequest(`/api/integrations/${id}`, {
            method: 'DELETE',
        }),

    // ETL Runs
    getRuns: (): Promise<ETLRun[]> =>
        apiRequest('/api/runs'),

    getRun: (id: string): Promise<ETLRun> =>
        apiRequest(`/api/runs/${id}`),

    // Query execution
    executeQuery: (query: string): Promise<QueryResult> =>
        apiRequest('/api/query', {
            method: 'POST',
            body: JSON.stringify({ query }),
        }),

    // Query validation (for intellisense)
    validateQuery: (query: string): Promise<{ valid: boolean; errors: string[]; suggestions: string[] }> =>
        apiRequest('/api/query/validate', {
            method: 'POST',
            body: JSON.stringify({ query }),
        }),

    // Schema information for autocomplete
    getSchema: (integrationName: string): Promise<{ tables: string[]; columns: Record<string, string[]> }> =>
        apiRequest(`/api/schema/${integrationName}`),

    // ETL Pipelines
    getPipelines: (): Promise<ETLPipeline[]> =>
        apiRequest('/api/pipelines'),

    getPipeline: (id: string): Promise<ETLPipeline> =>
        apiRequest(`/api/pipelines/${id}`),

    createPipeline: (pipeline: Omit<ETLPipeline, 'id' | 'createdAt'>): Promise<ETLPipeline> =>
        apiRequest('/api/pipelines', {
            method: 'POST',
            body: JSON.stringify(pipeline),
        }),

    updatePipeline: (id: string, updates: Partial<ETLPipeline>): Promise<ETLPipeline> =>
        apiRequest(`/api/pipelines/${id}`, {
            method: 'PUT',
            body: JSON.stringify(updates),
        }),

    deletePipeline: (id: string): Promise<void> =>
        apiRequest(`/api/pipelines/${id}`, {
            method: 'DELETE',
        }),

    runPipeline: (id: string): Promise<ETLRun> =>
        apiRequest(`/api/pipelines/${id}/run`, {
            method: 'POST',
        }),

    // Query history
    getQueryHistory: (): Promise<{ id: string; query: string; timestamp: string; success: boolean }[]> =>
        apiRequest('/api/query/history'),

    saveQuery: (query: string, name?: string): Promise<{ id: string }> =>
        apiRequest('/api/query/save', {
            method: 'POST',
            body: JSON.stringify({ query, name }),
        }),

    // Health check
    health: (): Promise<{ status: string; version: string }> =>
        apiRequest('/api/health'),
};
