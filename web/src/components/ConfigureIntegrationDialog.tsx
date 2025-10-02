import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Integration } from "@/types/etl";
import { ScrollArea } from "@/components/ui/scroll-area";

interface ConfigureIntegrationDialogProps {
  integration: Integration | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ConfigureIntegrationDialog({
  integration,
  open,
  onOpenChange,
}: ConfigureIntegrationDialogProps) {
  if (!integration) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Configure {integration.name}</DialogTitle>
          <DialogDescription>
            View and modify integration settings
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="max-h-[60vh]">
          <div className="space-y-4">
            <div>
              <h3 className="text-sm font-medium mb-2">Configuration</h3>
              <pre className="bg-code-background p-4 rounded-lg text-xs overflow-x-auto">
                {JSON.stringify(integration.config, null, 2)}
              </pre>
            </div>

            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Type:</span>
                <p className="font-medium capitalize">{integration.type}</p>
              </div>
              <div>
                <span className="text-muted-foreground">Status:</span>
                <p className="font-medium capitalize">{integration.status}</p>
              </div>
              <div>
                <span className="text-muted-foreground">Created:</span>
                <p className="font-medium">
                  {new Date(integration.createdAt).toLocaleString()}
                </p>
              </div>
              {integration.lastUsed && (
                <div>
                  <span className="text-muted-foreground">Last Used:</span>
                  <p className="font-medium">
                    {new Date(integration.lastUsed).toLocaleString()}
                  </p>
                </div>
              )}
            </div>
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
