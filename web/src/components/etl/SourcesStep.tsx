import { ETLSource } from "@/types/etl";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";

interface SourcesStepProps {
  sources: ETLSource[];
  onChange: (sources: ETLSource[]) => void;
}

export function SourcesStep({ sources, onChange }: SourcesStepProps) {
  const addSource = () => {
    onChange([
      ...sources,
      {
        key: "",
        type: "postgres",
        host: "",
        port: 5432,
        username: "",
        password: "",
        database: "",
      },
    ]);
  };

  const removeSource = (index: number) => {
    onChange(sources.filter((_, i) => i !== index));
  };

  const updateSource = (index: number, field: keyof ETLSource, value: any) => {
    const updated = [...sources];
    updated[index] = { ...updated[index], [field]: value };
    onChange(updated);
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center mb-4">
        <div>
          <h3 className="text-lg font-semibold">Configure Data Sources</h3>
          <p className="text-sm text-muted-foreground">
            Add one or more data sources to pull data from
          </p>
        </div>
        <Button onClick={addSource} size="sm">
          <Plus className="h-4 w-4 mr-2" />
          Add Source
        </Button>
      </div>

      {sources.length === 0 && (
        <Card className="p-8 text-center">
          <p className="text-muted-foreground mb-4">No sources configured yet</p>
          <Button onClick={addSource}>
            <Plus className="h-4 w-4 mr-2" />
            Add Your First Source
          </Button>
        </Card>
      )}

      {sources.map((source, index) => (
        <Card key={index} className="p-6">
          <div className="flex justify-between items-start mb-4">
            <h4 className="font-medium">Source #{index + 1}</h4>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => removeSource(index)}
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Source Key *</Label>
              <Input
                value={source.key}
                onChange={(e) => updateSource(index, "key", e.target.value)}
                placeholder="e.g., users_db"
              />
            </div>

            <div className="space-y-2">
              <Label>Type *</Label>
              <Select
                value={source.type}
                onValueChange={(value) => updateSource(index, "type", value)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="postgres">PostgreSQL</SelectItem>
                  <SelectItem value="mysql">MySQL</SelectItem>
                  <SelectItem value="mongodb">MongoDB</SelectItem>
                  <SelectItem value="csv">CSV</SelectItem>
                  <SelectItem value="json">JSON</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {["postgres", "mysql", "mongodb"].includes(source.type) && (
              <>
                <div className="space-y-2">
                  <Label>Driver</Label>
                  <Input
                    value={source.driver || source.type}
                    onChange={(e) => updateSource(index, "driver", e.target.value)}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Host</Label>
                  <Input
                    value={source.host || ""}
                    onChange={(e) => updateSource(index, "host", e.target.value)}
                    placeholder="localhost"
                  />
                </div>

                <div className="space-y-2">
                  <Label>Port</Label>
                  <Input
                    type="number"
                    value={source.port || ""}
                    onChange={(e) => updateSource(index, "port", parseInt(e.target.value))}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Username</Label>
                  <Input
                    value={source.username || ""}
                    onChange={(e) => updateSource(index, "username", e.target.value)}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Password</Label>
                  <Input
                    type="password"
                    value={source.password || ""}
                    onChange={(e) => updateSource(index, "password", e.target.value)}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Database</Label>
                  <Input
                    value={source.database || ""}
                    onChange={(e) => updateSource(index, "database", e.target.value)}
                  />
                </div>

                <div className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    id={`disablelogger-${index}`}
                    checked={source.disablelogger || false}
                    onChange={(e) => updateSource(index, "disablelogger", e.target.checked)}
                    className="h-4 w-4"
                  />
                  <Label htmlFor={`disablelogger-${index}`} className="cursor-pointer">
                    Disable Logger
                  </Label>
                </div>
              </>
            )}

            {["csv", "json"].includes(source.type) && (
              <div className="space-y-2 col-span-2">
                <Label>File Path</Label>
                <Input
                  value={source.file || ""}
                  onChange={(e) => updateSource(index, "file", e.target.value)}
                  placeholder="/path/to/file"
                />
              </div>
            )}
          </div>
        </Card>
      ))}
    </div>
  );
}
