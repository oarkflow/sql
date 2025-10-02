import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Clock, CheckCircle, XCircle, Play, Database } from "lucide-react";
import { api } from "@/lib/api";
import { ETLRun } from "@/types/etl";
import { StatusBadge } from "@/components/StatusBadge";

export default function Runs() {
    const [runs, setRuns] = useState<ETLRun[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        loadRuns();
    }, []);

    const loadRuns = async () => {
        try {
            const data = await api.getRuns();
            setRuns(data);
        } catch (error) {
            console.error("Failed to load runs:", error);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">ETL Runs</h1>
                <p className="text-muted-foreground">View execution history and performance metrics</p>
            </div>

            {loading ? (
                <div className="text-muted-foreground">Loading runs...</div>
            ) : runs.length === 0 ? (
                <Card>
                    <CardContent className="flex flex-col items-center justify-center py-12">
                        <Database className="h-12 w-12 text-muted-foreground mb-4" />
                        <h3 className="text-lg font-medium mb-2">No runs yet</h3>
                        <p className="text-sm text-muted-foreground">
                            Execute your first query to see results here
                        </p>
                    </CardContent>
                </Card>
            ) : (
                <div className="space-y-4">
                    {runs.map((run) => (
                        <Card key={run.id} className="hover:shadow-md transition-shadow">
                            <CardHeader>
                                <div className="flex items-start justify-between">
                                    <div className="flex-1 min-w-0">
                                        <CardTitle className="text-base font-mono break-all">
                                            {run.query}
                                        </CardTitle>
                                        <CardDescription className="flex items-center gap-4 mt-2">
                                            <span className="flex items-center gap-1">
                                                <Clock className="h-3 w-3" />
                                                {new Date(run.startTime).toLocaleString()}
                                            </span>
                                            {run.integrations.length > 0 && (
                                                <span className="flex items-center gap-1">
                                                    <Database className="h-3 w-3" />
                                                    {run.integrations.join(", ")}
                                                </span>
                                            )}
                                        </CardDescription>
                                    </div>
                                    <StatusBadge status={run.status} />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                    <div>
                                        <p className="text-muted-foreground text-xs">Duration</p>
                                        <p className="font-medium">
                                            {run.duration ? `${run.duration}s` : "—"}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-muted-foreground text-xs">Rows Processed</p>
                                        <p className="font-medium">
                                            {run.rowsProcessed !== undefined ? run.rowsProcessed.toLocaleString() : "—"}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-muted-foreground text-xs">Start Time</p>
                                        <p className="font-medium">
                                            {new Date(run.startTime).toLocaleTimeString()}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-muted-foreground text-xs">End Time</p>
                                        <p className="font-medium">
                                            {run.endTime ? new Date(run.endTime).toLocaleTimeString() : "—"}
                                        </p>
                                    </div>
                                </div>

                                {run.error && (
                                    <div className="mt-4 p-3 bg-destructive/10 border border-destructive/20 rounded">
                                        <p className="text-sm text-destructive font-medium">Error:</p>
                                        <p className="text-sm text-destructive/90 mt-1">{run.error}</p>
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    ))}
                </div>
            )}
        </div>
    );
}
