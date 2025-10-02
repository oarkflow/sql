import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Play, Save, FileText, BarChart3, Table as TableIcon, History, Bookmark, Keyboard } from "lucide-react";
import { api } from "@/lib/api";
import { QueryResult } from "@/types/etl";
import { toast } from "@/hooks/use-toast";
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import SQLEditor from "@/components/SQLEditor";

export default function QueryEditor() {
    const [query, setQuery] = useState(
        "SELECT id, first_name FROM read_file('users.csv') WHERE id = 1;"
    );
    const [isExecuting, setIsExecuting] = useState(false);
    const [result, setResult] = useState<QueryResult | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [viewMode, setViewMode] = useState<"table" | "chart">("table");
    const [queryHistory, setQueryHistory] = useState<any[]>([]);
    const [savedQueries, setSavedQueries] = useState<any[]>([]);
    const [activeTab, setActiveTab] = useState("editor");

    const CHART_COLORS = ["hsl(var(--primary))", "hsl(var(--chart-2))", "hsl(var(--chart-3))", "hsl(var(--chart-4))", "hsl(var(--chart-5))"];

    useEffect(() => {
        // Load query history and saved queries
        loadQueryHistory();
        loadSavedQueries();
    }, []);

    const loadQueryHistory = async () => {
        try {
            const history = await api.getQueryHistory();
            setQueryHistory(history);
        } catch (error) {
            console.error('Failed to load query history:', error);
        }
    };

    const loadSavedQueries = async () => {
        // For now, use localStorage for saved queries
        const saved = localStorage.getItem('savedQueries');
        if (saved) {
            setSavedQueries(JSON.parse(saved));
        }
    };

    const saveQuery = () => {
        const queryName = prompt('Enter a name for this query:');
        if (queryName) {
            const newSavedQuery = {
                id: Date.now().toString(),
                name: queryName,
                query: query,
                timestamp: new Date().toISOString(),
            };
            const updated = [...savedQueries, newSavedQuery];
            setSavedQueries(updated);
            localStorage.setItem('savedQueries', JSON.stringify(updated));
            toast({
                title: "Query Saved",
                description: `Query "${queryName}" has been saved.`,
            });
        }
    };

    const loadSavedQuery = (savedQuery: any) => {
        setQuery(savedQuery.query);
        setActiveTab("editor");
        toast({
            title: "Query Loaded",
            description: `Loaded "${savedQuery.name}"`,
        });
    };

    const loadHistoryQuery = (historyItem: any) => {
        setQuery(historyItem.query);
        setActiveTab("editor");
    };

    const prepareChartData = () => {
        if (!result || result.rows.length === 0) return [];

        return result.rows.map((row) => {
            const dataPoint: any = {};
            result.columns.forEach((col, idx) => {
                dataPoint[col] = row[idx];
            });
            return dataPoint;
        });
    };

    const handleExecute = async () => {
        if (!query.trim()) {
            toast({
                title: "Empty Query",
                description: "Please enter a SQL query to execute",
                variant: "destructive",
            });
            return;
        }

        setIsExecuting(true);
        setError(null);
        setResult(null);

        try {
            const queryResult = await api.executeQuery(query);
            setResult(queryResult);
            toast({
                title: "Query Executed",
                description: `Processed ${queryResult.rowCount} rows in ${queryResult.executionTime}s`,
            });
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : "Failed to execute query";
            setError(errorMessage);
            toast({
                title: "Execution Failed",
                description: errorMessage,
                variant: "destructive",
            });
        } finally {
            setIsExecuting(false);
        }
    };

    const exampleQueries = [
        "SELECT id, first_name FROM read_file('users.csv') WHERE id = 1;",
        "SELECT * FROM read_db('PostgreSQL DB', 'customers') LIMIT 100;",
        "SELECT id FROM read_file('users.csv') WHERE id > 4 UNION SELECT id FROM read_file('orders.json');",
        "SELECT * FROM read_service('Salesforce API', 'accounts');",
    ];

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Query Editor</h1>
                <p className="text-muted-foreground">Write and execute SQL queries with ETL functions</p>
            </div>

            <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
                <TabsList>
                    <TabsTrigger value="editor" className="flex items-center gap-2">
                        <FileText className="h-4 w-4" />
                        Editor
                    </TabsTrigger>
                    <TabsTrigger value="history" className="flex items-center gap-2">
                        <History className="h-4 w-4" />
                        History
                    </TabsTrigger>
                    <TabsTrigger value="saved" className="flex items-center gap-2">
                        <Bookmark className="h-4 w-4" />
                        Saved Queries
                    </TabsTrigger>
                    <TabsTrigger value="shortcuts" className="flex items-center gap-2">
                        <Keyboard className="h-4 w-4" />
                        Shortcuts
                    </TabsTrigger>
                </TabsList>

                <TabsContent value="editor" className="space-y-6">
                    <div className="grid gap-6 lg:grid-cols-3">
                        <div className="lg:col-span-2 space-y-6">
                            {/* Query Editor */}
                            <Card>
                                <CardHeader>
                                    <CardTitle>SQL Editor</CardTitle>
                                    <CardDescription>
                                        Use read_file(), read_db(), and read_service() functions
                                    </CardDescription>
                                </CardHeader>
                                <CardContent className="space-y-4">
                                    <SQLEditor
                                        value={query}
                                        onChange={setQuery}
                                        onExecute={handleExecute}
                                        height="300px"
                                    />

                                    <div className="flex gap-2">
                                        <Button onClick={handleExecute} disabled={isExecuting}>
                                            {isExecuting ? (
                                                <>
                                                    <span className="animate-spin mr-2">⏳</span>
                                                    Executing...
                                                </>
                                            ) : (
                                                <>
                                                    <Play className="mr-2 h-4 w-4" />
                                                    Execute Query
                                                </>
                                            )}
                                        </Button>
                                        <Button variant="outline" onClick={saveQuery}>
                                            <Save className="mr-2 h-4 w-4" />
                                            Save Query
                                        </Button>
                                    </div>
                                </CardContent>
                            </Card>

                            {/* Results */}
                            {result && (
                                <Card>
                                    <CardHeader>
                                        <div className="flex items-center justify-between">
                                            <div>
                                                <CardTitle>Query Results</CardTitle>
                                                <CardDescription>
                                                    {result.rowCount} rows • {result.executionTime}s execution time
                                                </CardDescription>
                                            </div>
                                            <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as "table" | "chart")}>
                                                <TabsList>
                                                    <TabsTrigger value="table" className="flex items-center gap-2">
                                                        <TableIcon className="h-4 w-4" />
                                                        Table
                                                    </TabsTrigger>
                                                    <TabsTrigger value="chart" className="flex items-center gap-2">
                                                        <BarChart3 className="h-4 w-4" />
                                                        Charts
                                                    </TabsTrigger>
                                                </TabsList>
                                            </Tabs>
                                        </div>
                                    </CardHeader>
                                    <CardContent>
                                        {viewMode === "table" ? (
                                            <div className="overflow-x-auto">
                                                <table className="w-full text-sm">
                                                    <thead>
                                                        <tr className="border-b border-border">
                                                            {result.columns.map((col) => (
                                                                <th
                                                                    key={col}
                                                                    className="text-left py-2 px-4 font-medium bg-muted"
                                                                >
                                                                    {col}
                                                                </th>
                                                            ))}
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {result.rows.map((row, idx) => (
                                                            <tr key={idx} className="border-b border-border hover:bg-muted/50">
                                                                {row.map((cell, cellIdx) => (
                                                                    <td key={cellIdx} className="py-2 px-4">
                                                                        {String(cell)}
                                                                    </td>
                                                                ))}
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        ) : (
                                            <div className="space-y-6">
                                                <div>
                                                    <h3 className="text-sm font-medium mb-4">Bar Chart</h3>
                                                    <ResponsiveContainer width="100%" height={300}>
                                                        <BarChart data={prepareChartData()}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                                                            <XAxis
                                                                dataKey={result.columns[0]}
                                                                stroke="hsl(var(--muted-foreground))"
                                                            />
                                                            <YAxis stroke="hsl(var(--muted-foreground))" />
                                                            <Tooltip
                                                                contentStyle={{
                                                                    backgroundColor: "hsl(var(--popover))",
                                                                    border: "1px solid hsl(var(--border))",
                                                                    borderRadius: "6px",
                                                                }}
                                                            />
                                                            <Legend />
                                                            {result.columns.slice(1).map((col, idx) => (
                                                                <Bar
                                                                    key={col}
                                                                    dataKey={col}
                                                                    fill={CHART_COLORS[idx % CHART_COLORS.length]}
                                                                />
                                                            ))}
                                                        </BarChart>
                                                    </ResponsiveContainer>
                                                </div>

                                                <div>
                                                    <h3 className="text-sm font-medium mb-4">Line Chart</h3>
                                                    <ResponsiveContainer width="100%" height={300}>
                                                        <LineChart data={prepareChartData()}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                                                            <XAxis
                                                                dataKey={result.columns[0]}
                                                                stroke="hsl(var(--muted-foreground))"
                                                            />
                                                            <YAxis stroke="hsl(var(--muted-foreground))" />
                                                            <Tooltip
                                                                contentStyle={{
                                                                    backgroundColor: "hsl(var(--popover))",
                                                                    border: "1px solid hsl(var(--border))",
                                                                    borderRadius: "6px",
                                                                }}
                                                            />
                                                            <Legend />
                                                            {result.columns.slice(1).map((col, idx) => (
                                                                <Line
                                                                    key={col}
                                                                    type="monotone"
                                                                    dataKey={col}
                                                                    stroke={CHART_COLORS[idx % CHART_COLORS.length]}
                                                                    strokeWidth={2}
                                                                />
                                                            ))}
                                                        </LineChart>
                                                    </ResponsiveContainer>
                                                </div>

                                                {result.columns.length === 2 && (
                                                    <div>
                                                        <h3 className="text-sm font-medium mb-4">Pie Chart</h3>
                                                        <ResponsiveContainer width="100%" height={300}>
                                                            <PieChart>
                                                                <Pie
                                                                    data={prepareChartData()}
                                                                    dataKey={result.columns[1]}
                                                                    nameKey={result.columns[0]}
                                                                    cx="50%"
                                                                    cy="50%"
                                                                    outerRadius={100}
                                                                    label
                                                                >
                                                                    {prepareChartData().map((entry, index) => (
                                                                        <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                                                    ))}
                                                                </Pie>
                                                                <Tooltip
                                                                    contentStyle={{
                                                                        backgroundColor: "hsl(var(--popover))",
                                                                        border: "1px solid hsl(var(--border))",
                                                                        borderRadius: "6px",
                                                                    }}
                                                                />
                                                                <Legend />
                                                            </PieChart>
                                                        </ResponsiveContainer>
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </CardContent>
                                </Card>
                            )}

                            {error && (
                                <Card className="border-destructive">
                                    <CardHeader>
                                        <CardTitle className="text-destructive">Error</CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <pre className="text-sm text-destructive bg-destructive/10 p-4 rounded">
                                            {error}
                                        </pre>
                                    </CardContent>
                                </Card>
                            )}
                        </div>

                        {/* Sidebar with examples */}
                        <div className="space-y-6">
                            <Card>
                                <CardHeader>
                                    <CardTitle className="text-base">Example Queries</CardTitle>
                                    <CardDescription>Click to load into editor</CardDescription>
                                </CardHeader>
                                <CardContent className="space-y-2">
                                    {exampleQueries.map((example, idx) => (
                                        <button
                                            key={idx}
                                            onClick={() => setQuery(example)}
                                            className="w-full text-left p-3 text-xs font-mono bg-code-background hover:bg-muted border border-code-border rounded transition-colors"
                                        >
                                            {example}
                                        </button>
                                    ))}
                                </CardContent>
                            </Card>

                            <Card>
                                <CardHeader>
                                    <CardTitle className="text-base flex items-center gap-2">
                                        <FileText className="h-4 w-4" />
                                        ETL Functions
                                    </CardTitle>
                                </CardHeader>
                                <CardContent className="space-y-3 text-sm">
                                    <div>
                                        <code className="text-primary font-mono">read_file()</code>
                                        <p className="text-muted-foreground text-xs mt-1">
                                            Read data from CSV, JSON, or other file formats
                                        </p>
                                    </div>
                                    <div>
                                        <code className="text-primary font-mono">read_db()</code>
                                        <p className="text-muted-foreground text-xs mt-1">
                                            Query data from connected databases
                                        </p>
                                    </div>
                                    <div>
                                        <code className="text-primary font-mono">read_service()</code>
                                        <p className="text-muted-foreground text-xs mt-1">
                                            Fetch data from external APIs and services
                                        </p>
                                    </div>
                                </CardContent>
                            </Card>
                        </div>
                    </div>
                </TabsContent>

                <TabsContent value="history" className="space-y-6">
                    <Card>
                        <CardHeader>
                            <CardTitle>Query History</CardTitle>
                            <CardDescription>Previously executed queries</CardDescription>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-2">
                                {queryHistory.length === 0 ? (
                                    <p className="text-muted-foreground">No query history available</p>
                                ) : (
                                    queryHistory.map((item, idx) => (
                                        <div key={idx} className="flex items-center justify-between p-3 border rounded hover:bg-muted/50">
                                            <div className="flex-1">
                                                <code className="text-sm font-mono text-primary">{item.query}</code>
                                                <p className="text-xs text-muted-foreground mt-1">
                                                    {new Date(item.timestamp).toLocaleString()}
                                                </p>
                                            </div>
                                            <Button
                                                variant="outline"
                                                size="sm"
                                                onClick={() => loadHistoryQuery(item)}
                                            >
                                                Load
                                            </Button>
                                        </div>
                                    ))
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="saved" className="space-y-6">
                    <Card>
                        <CardHeader>
                            <CardTitle>Saved Queries</CardTitle>
                            <CardDescription>Your saved SQL queries</CardDescription>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-2">
                                {savedQueries.length === 0 ? (
                                    <p className="text-muted-foreground">No saved queries</p>
                                ) : (
                                    savedQueries.map((item) => (
                                        <div key={item.id} className="flex items-center justify-between p-3 border rounded hover:bg-muted/50">
                                            <div className="flex-1">
                                                <h4 className="font-medium">{item.name}</h4>
                                                <code className="text-sm font-mono text-primary block mt-1">{item.query}</code>
                                                <p className="text-xs text-muted-foreground mt-1">
                                                    Saved {new Date(item.timestamp).toLocaleString()}
                                                </p>
                                            </div>
                                            <Button
                                                variant="outline"
                                                size="sm"
                                                onClick={() => loadSavedQuery(item)}
                                            >
                                                Load
                                            </Button>
                                        </div>
                                    ))
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="shortcuts" className="space-y-6">
                    <Card>
                        <CardHeader>
                            <CardTitle>Keyboard Shortcuts</CardTitle>
                            <CardDescription>Productivity shortcuts for the SQL editor</CardDescription>
                        </CardHeader>
                        <CardContent>
                            <div className="grid gap-4 md:grid-cols-2">
                                <div className="space-y-3">
                                    <h4 className="font-medium">Execution</h4>
                                    <div className="space-y-2 text-sm">
                                        <div className="flex justify-between">
                                            <span>Execute Query</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + Enter</kbd>
                                        </div>
                                    </div>
                                </div>

                                <div className="space-y-3">
                                    <h4 className="font-medium">Editing</h4>
                                    <div className="space-y-2 text-sm">
                                        <div className="flex justify-between">
                                            <span>Format Document</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + Shift + F</kbd>
                                        </div>
                                        <div className="flex justify-between">
                                            <span>Toggle Comment</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + /</kbd>
                                        </div>
                                        <div className="flex justify-between">
                                            <span>Multi-cursor</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + Click</kbd>
                                        </div>
                                    </div>
                                </div>

                                <div className="space-y-3">
                                    <h4 className="font-medium">Navigation</h4>
                                    <div className="space-y-2 text-sm">
                                        <div className="flex justify-between">
                                            <span>Find</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + F</kbd>
                                        </div>
                                        <div className="flex justify-between">
                                            <span>Replace</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + H</kbd>
                                        </div>
                                        <div className="flex justify-between">
                                            <span>Go to Line</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + G</kbd>
                                        </div>
                                    </div>
                                </div>

                                <div className="space-y-3">
                                    <h4 className="font-medium">IntelliSense</h4>
                                    <div className="space-y-2 text-sm">
                                        <div className="flex justify-between">
                                            <span>Trigger Suggestions</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl/Cmd + Space</kbd>
                                        </div>
                                        <div className="flex justify-between">
                                            <span>Accept Suggestion</span>
                                            <kbd className="px-2 py-1 bg-muted rounded text-xs">Tab / Enter</kbd>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </div>
    );
}
