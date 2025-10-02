import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api } from "@/lib/api";
import { ETLPipeline } from "@/types/etl";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Plus, Play, Settings, Trash2, Clock, Database } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
} from "@/components/ui/alert-dialog";

export default function ETLPipelinesList() {
    const [deletingId, setDeletingId] = useState<string | null>(null);
    const { toast } = useToast();
    const queryClient = useQueryClient();
    const navigate = useNavigate();

    const { data: pipelines = [], isLoading } = useQuery({
        queryKey: ["pipelines"],
        queryFn: api.getPipelines,
    });

    const runMutation = useMutation({
        mutationFn: api.runPipeline,
        onSuccess: (run) => {
            toast({
                title: "Pipeline Started",
                description: `Pipeline is running. Run ID: ${run.id}`,
            });
            queryClient.invalidateQueries({ queryKey: ["pipelines"] });
            queryClient.invalidateQueries({ queryKey: ["runs"] });
        },
        onError: () => {
            toast({
                title: "Error",
                description: "Failed to run pipeline",
                variant: "destructive",
            });
        },
    });

    const deleteMutation = useMutation({
        mutationFn: api.deletePipeline,
        onSuccess: () => {
            toast({
                title: "Pipeline Deleted",
                description: "Pipeline has been removed successfully",
            });
            queryClient.invalidateQueries({ queryKey: ["pipelines"] });
            setDeletingId(null);
        },
        onError: () => {
            toast({
                title: "Error",
                description: "Failed to delete pipeline",
                variant: "destructive",
            });
        },
    });

    const handleRun = (id: string) => {
        runMutation.mutate(id);
    };

    const getStatusColor = (status: ETLPipeline["status"]) => {
        switch (status) {
            case "active": return "default";
            case "inactive": return "secondary";
            case "draft": return "outline";
        }
    };

    return (
        <div className="p-8 space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <h1 className="text-3xl font-bold text-foreground">ETL Pipelines</h1>
                    <p className="text-muted-foreground mt-1">
                        Create and manage data transformation pipelines
                    </p>
                </div>
                <Button onClick={() => navigate("/pipelines/new")} size="lg">
                    <Plus className="mr-2 h-4 w-4" />
                    Create Pipeline
                </Button>
            </div>

            {isLoading ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {[1, 2, 3].map((i) => (
                        <Card key={i} className="p-6 animate-pulse">
                            <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
                            <div className="h-4 bg-muted rounded w-full mb-2"></div>
                            <div className="h-4 bg-muted rounded w-2/3"></div>
                        </Card>
                    ))}
                </div>
            ) : pipelines.length === 0 ? (
                <Card className="p-12 text-center">
                    <Database className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <h3 className="text-lg font-semibold mb-2">No Pipelines Yet</h3>
                    <p className="text-muted-foreground mb-4">
                        Create your first ETL pipeline to start transforming data
                    </p>
                    <Button onClick={() => navigate("/pipelines/new")}>
                        <Plus className="mr-2 h-4 w-4" />
                        Create Pipeline
                    </Button>
                </Card>
            ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {pipelines.map((pipeline) => (
                        <Card key={pipeline.id} className="p-6 hover:border-primary/50 transition-colors">
                            <div className="flex justify-between items-start mb-4">
                                <div>
                                    <h3 className="text-lg font-semibold text-foreground mb-1">
                                        {pipeline.name}
                                    </h3>
                                    <Badge variant={getStatusColor(pipeline.status)}>
                                        {pipeline.status}
                                    </Badge>
                                </div>
                            </div>

                            <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
                                {pipeline.description}
                            </p>

                            <div className="space-y-2 text-sm mb-4">
                                <div className="flex items-center text-muted-foreground">
                                    <Database className="h-4 w-4 mr-2" />
                                    <span>{pipeline.sources.length} source(s)</span>
                                    <span className="mx-2">→</span>
                                    <span>{pipeline.destinations.length} destination(s)</span>
                                </div>
                                {pipeline.lastRun && (
                                    <div className="flex items-center text-muted-foreground">
                                        <Clock className="h-4 w-4 mr-2" />
                                        <span className="text-xs">
                                            Last run: {new Date(pipeline.lastRun).toLocaleString()}
                                        </span>
                                    </div>
                                )}
                            </div>

                            <div className="flex gap-2">
                                <Button
                                    size="sm"
                                    onClick={() => handleRun(pipeline.id)}
                                    disabled={runMutation.isPending}
                                    className="flex-1"
                                >
                                    <Play className="h-3 w-3 mr-1" />
                                    Run
                                </Button>
                                <Button
                                    size="sm"
                                    variant="outline"
                                    onClick={() => navigate(`/pipelines/${pipeline.id}/edit`)}
                                >
                                    <Settings className="h-3 w-3" />
                                </Button>
                                <Button
                                    size="sm"
                                    variant="outline"
                                    onClick={() => setDeletingId(pipeline.id)}
                                >
                                    <Trash2 className="h-3 w-3" />
                                </Button>
                            </div>
                        </Card>
                    ))}
                </div>
            )}

            <AlertDialog open={!!deletingId} onOpenChange={() => setDeletingId(null)}>
                <AlertDialogContent>
                    <AlertDialogHeader>
                        <AlertDialogTitle>Delete Pipeline</AlertDialogTitle>
                        <AlertDialogDescription>
                            Are you sure you want to delete this pipeline? This action cannot be undone.
                        </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                        <AlertDialogCancel>Cancel</AlertDialogCancel>
                        <AlertDialogAction
                            onClick={() => deletingId && deleteMutation.mutate(deletingId)}
                        >
                            Delete
                        </AlertDialogAction>
                    </AlertDialogFooter>
                </AlertDialogContent>
            </AlertDialog>
        </div>
    );
}
