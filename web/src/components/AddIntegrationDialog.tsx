import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Database, Globe, FileUp } from "lucide-react";
import { DatabaseForm } from "./integrations/DatabaseForm";
import { HttpServiceForm } from "./integrations/HttpServiceForm";
import { FileUploadForm } from "./integrations/FileUploadForm";

interface AddIntegrationDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSuccess: () => void;
}

export function AddIntegrationDialog({
  open,
  onOpenChange,
  onSuccess,
}: AddIntegrationDialogProps) {
  const [activeTab, setActiveTab] = useState("database");

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Add Integration</DialogTitle>
          <DialogDescription>
            Connect to a database, HTTP service, or upload a file
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="database" className="flex items-center gap-2">
              <Database className="h-4 w-4" />
              Database
            </TabsTrigger>
            <TabsTrigger value="service" className="flex items-center gap-2">
              <Globe className="h-4 w-4" />
              HTTP Service
            </TabsTrigger>
            <TabsTrigger value="file" className="flex items-center gap-2">
              <FileUp className="h-4 w-4" />
              File
            </TabsTrigger>
          </TabsList>

          <TabsContent value="database" className="mt-4">
            <DatabaseForm onSuccess={() => { onSuccess(); onOpenChange(false); }} />
          </TabsContent>

          <TabsContent value="service" className="mt-4">
            <HttpServiceForm onSuccess={() => { onSuccess(); onOpenChange(false); }} />
          </TabsContent>

          <TabsContent value="file" className="mt-4">
            <FileUploadForm onSuccess={() => { onSuccess(); onOpenChange(false); }} />
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
