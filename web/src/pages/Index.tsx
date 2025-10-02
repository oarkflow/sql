import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Database, FileText, Play, TrendingUp } from "lucide-react";
import { api } from "@/lib/api";
import { Integration, ETLRun } from "@/types/etl";
import { StatusBadge } from "@/components/StatusBadge";

export default function Index() {
    const [integrations, setIntegrations] = useState<Integration[]>([]);
    const [recentRuns, setRecentRuns] = useState<ETLRun[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const loadData = async () => {
            try {
                const [integrationsData, runsData] = await Promise.all([
                    api.getIntegrations(),
                    api.getRuns(),
                ]);
                setIntegrations(integrationsData);
                setRecentRuns(runsData.slice(0, 5));
            } catch (error) {
                console.error("Failed to load dashboard data:", error);
            } finally {
                setLoading(false);
            }
        };

        loadData();
    }, []);

    const stats = {
        totalIntegrations: integrations.length,
        connectedIntegrations: integrations.filter((i) => i.status === "connected").length,
        totalRuns: recentRuns.length,
        successRate: recentRuns.length > 0
            ? Math.round((recentRuns.filter((r) => r.status === "success").length / recentRuns.length) * 100)
            : 0,
    };

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
                <p className="text-muted-foreground">Monitor your ETL pipelines and integrations</p>
            </div>

            {/* Stats Grid */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Total Integrations</CardTitle>
                        <Database className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats.totalIntegrations}</div>
                        <p className="text-xs text-muted-foreground">
                            {stats.connectedIntegrations} connected
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Recent Runs</CardTitle>
                        <Play className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats.totalRuns}</div>
                        <p className="text-xs text-muted-foreground">Last 5 executions</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
                        <TrendingUp className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats.successRate}%</div>
                        <p className="text-xs text-muted-foreground">From recent runs</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Quick Actions</CardTitle>
                        <FileText className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <Link to="/query-editor">
                            <Button className="w-full" size="sm">
                                New Query
                            </Button>
                        </Link>
                    </CardContent>
                </Card>
            </div>

            {/* Recent Runs */}
            <Card>
                <CardHeader>
                    <CardTitle>Recent ETL Runs</CardTitle>
                    <CardDescription>Latest query executions</CardDescription>
                </CardHeader>
                <CardContent>
                    {loading ? (
                        <div className="text-sm text-muted-foreground">Loading...</div>
                    ) : recentRuns.length === 0 ? (
                        <div className="text-sm text-muted-foreground">No runs yet. Start by creating a query.</div>
                    ) : (
                        <div className="space-y-3">
                            {recentRuns.map((run) => (
                                <div
                                    key={run.id}
                                    className="flex items-center justify-between p-3 border border-border rounded-lg hover:bg-muted/50 transition-colors"
                                >
                                    <div className="flex-1 min-w-0">
                                        <code className="text-sm font-mono text-foreground truncate block">
                                            {run.query}
                                        </code>
                                        <div className="flex items-center gap-3 mt-1 text-xs text-muted-foreground">
                                            <span>{new Date(run.startTime).toLocaleString()}</span>
                                            {run.duration && <span>{run.duration}s</span>}
                                            {run.rowsProcessed !== undefined && <span>{run.rowsProcessed} rows</span>}
                                        </div>
                                    </div>
                                    <div className="ml-4">
                                        <StatusBadge status={run.status} size="sm" />
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                    <div className="mt-4">
                        <Link to="/runs">
                            <Button variant="outline" className="w-full">
                                View All Runs
                            </Button>
                        </Link>
                    </div>
                </CardContent>
            </Card>

            {/* Active Integrations */}
            <Card>
                <CardHeader>
                    <CardTitle>Active Integrations</CardTitle>
                    <CardDescription>Connected data sources</CardDescription>
                </CardHeader>
                <CardContent>
                    {loading ? (
                        <div className="text-sm text-muted-foreground">Loading...</div>
                    ) : integrations.filter((i) => i.status === "connected").length === 0 ? (
                        <div className="text-sm text-muted-foreground">
                            No active integrations. Add one to get started.
                        </div>
                    ) : (
                        <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
                            {integrations
                                .filter((i) => i.status === "connected")
                                .map((integration) => (
                                    <div
                                        key={integration.id}
                                        className="p-3 border border-border rounded-lg hover:bg-muted/50 transition-colors"
                                    >
                                        <div className="flex items-start justify-between">
                                            <div className="flex-1 min-w-0">
                                                <h4 className="font-medium text-sm truncate">{integration.name}</h4>
                                                <p className="text-xs text-muted-foreground mt-1">{integration.description}</p>
                                            </div>
                                            <StatusBadge status={integration.status} size="sm" />
                                        </div>
                                    </div>
                                ))}
                        </div>
                    )}
                    <div className="mt-4">
                        <Link to="/integrations">
                            <Button variant="outline" className="w-full">
                                Manage Integrations
                            </Button>
                        </Link>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
