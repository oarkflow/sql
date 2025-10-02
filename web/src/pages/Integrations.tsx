import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Plus, Database, FileText, Globe } from "lucide-react";
import { api } from "@/lib/api";
import { Integration } from "@/types/etl";
import { StatusBadge } from "@/components/StatusBadge";
import { AddIntegrationDialog } from "@/components/AddIntegrationDialog";
import { ConfigureIntegrationDialog } from "@/components/ConfigureIntegrationDialog";
import { toast } from "@/hooks/use-toast";

export default function Integrations() {
    const [integrations, setIntegrations] = useState<Integration[]>([]);
    const [loading, setLoading] = useState(true);
    const [addDialogOpen, setAddDialogOpen] = useState(false);
    const [configureDialogOpen, setConfigureDialogOpen] = useState(false);
    const [selectedIntegration, setSelectedIntegration] = useState<Integration | null>(null);

    useEffect(() => {
        loadIntegrations();
    }, []);

    const loadIntegrations = async () => {
        try {
            const data = await api.getIntegrations();
            setIntegrations(data);
        } catch (error) {
            console.error("Failed to load integrations:", error);
        } finally {
            setLoading(false);
        }
    };

    const handleTest = async (integration: Integration) => {
        toast({
            title: "Testing Connection",
            description: `Testing ${integration.name}...`,
        });

        // Simulate test
        setTimeout(() => {
            toast({
                title: "Test Successful",
                description: `${integration.name} is working correctly`,
            });
        }, 1500);
    };

    const handleConfigure = (integration: Integration) => {
        setSelectedIntegration(integration);
        setConfigureDialogOpen(true);
    };

    const getIcon = (type: string) => {
        switch (type) {
            case "file":
                return <FileText className="h-5 w-5" />;
            case "database":
                return <Database className="h-5 w-5" />;
            case "service":
                return <Globe className="h-5 w-5" />;
            default:
                return <Database className="h-5 w-5" />;
        }
    };

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Integrations</h1>
                    <p className="text-muted-foreground">Manage your data source connections</p>
                </div>
                <Button onClick={() => setAddDialogOpen(true)}>
                    <Plus className="mr-2 h-4 w-4" />
                    Add Integration
                </Button>
            </div>

            {loading ? (
                <div className="text-muted-foreground">Loading integrations...</div>
            ) : (
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                    {integrations.map((integration) => (
                        <Card key={integration.id} className="hover:shadow-md transition-shadow">
                            <CardHeader>
                                <div className="flex items-start justify-between">
                                    <div className="flex items-center gap-3">
                                        <div className="p-2 rounded-lg bg-primary/10 text-primary">
                                            {getIcon(integration.type)}
                                        </div>
                                        <div>
                                            <CardTitle className="text-base">{integration.name}</CardTitle>
                                            <CardDescription className="text-xs capitalize">
                                                {integration.type}
                                            </CardDescription>
                                        </div>
                                    </div>
                                    <StatusBadge status={integration.status} size="sm" />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <p className="text-sm text-muted-foreground mb-4">{integration.description}</p>

                                <div className="space-y-2 text-xs">
                                    <div className="flex justify-between">
                                        <span className="text-muted-foreground">Created:</span>
                                        <span className="font-medium">
                                            {new Date(integration.createdAt).toLocaleDateString()}
                                        </span>
                                    </div>
                                    {integration.lastUsed && (
                                        <div className="flex justify-between">
                                            <span className="text-muted-foreground">Last Used:</span>
                                            <span className="font-medium">
                                                {new Date(integration.lastUsed).toLocaleDateString()}
                                            </span>
                                        </div>
                                    )}
                                </div>

                                <div className="mt-4 flex gap-2">
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        className="flex-1"
                                        onClick={() => handleConfigure(integration)}
                                    >
                                        Configure
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        className="flex-1"
                                        onClick={() => handleTest(integration)}
                                    >
                                        Test
                                    </Button>
                                </div>
                            </CardContent>
                        </Card>
                    ))}
                </div>
            )}

            {!loading && integrations.length === 0 && (
                <Card>
                    <CardContent className="flex flex-col items-center justify-center py-12">
                        <Database className="h-12 w-12 text-muted-foreground mb-4" />
                        <h3 className="text-lg font-medium mb-2">No integrations yet</h3>
                        <p className="text-sm text-muted-foreground mb-4">
                            Get started by adding your first data source
                        </p>
                        <Button onClick={() => setAddDialogOpen(true)}>
                            <Plus className="mr-2 h-4 w-4" />
                            Add Integration
                        </Button>
                    </CardContent>
                </Card>
            )}

            <AddIntegrationDialog
                open={addDialogOpen}
                onOpenChange={setAddDialogOpen}
                onSuccess={loadIntegrations}
            />

            <ConfigureIntegrationDialog
                integration={selectedIntegration}
                open={configureDialogOpen}
                onOpenChange={setConfigureDialogOpen}
            />
        </div>
    );
}
