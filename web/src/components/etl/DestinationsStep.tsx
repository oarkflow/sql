import { ETLDestination } from "@/types/etl";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";

interface DestinationsStepProps {
  destinations: ETLDestination[];
  onChange: (destinations: ETLDestination[]) => void;
}

export function DestinationsStep({ destinations, onChange }: DestinationsStepProps) {
  const addDestination = () => {
    onChange([
      ...destinations,
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

  const removeDestination = (index: number) => {
    onChange(destinations.filter((_, i) => i !== index));
  };

  const updateDestination = (index: number, field: keyof ETLDestination, value: any) => {
    const updated = [...destinations];
    updated[index] = { ...updated[index], [field]: value };
    onChange(updated);
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center mb-4">
        <div>
          <h3 className="text-lg font-semibold">Configure Data Destinations</h3>
          <p className="text-sm text-muted-foreground">
            Add one or more destinations to write data to
          </p>
        </div>
        <Button onClick={addDestination} size="sm">
          <Plus className="h-4 w-4 mr-2" />
          Add Destination
        </Button>
      </div>

      {destinations.length === 0 && (
        <Card className="p-8 text-center">
          <p className="text-muted-foreground mb-4">No destinations configured yet</p>
          <Button onClick={addDestination}>
            <Plus className="h-4 w-4 mr-2" />
            Add Your First Destination
          </Button>
        </Card>
      )}

      {destinations.map((dest, index) => (
        <Card key={index} className="p-6">
          <div className="flex justify-between items-start mb-4">
            <h4 className="font-medium">Destination #{index + 1}</h4>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => removeDestination(index)}
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Destination Key *</Label>
              <Input
                value={dest.key}
                onChange={(e) => updateDestination(index, "key", e.target.value)}
                placeholder="e.g., analytics_db"
              />
            </div>

            <div className="space-y-2">
              <Label>Type *</Label>
              <Select
                value={dest.type}
                onValueChange={(value) => updateDestination(index, "type", value)}
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

            {["postgres", "mysql", "mongodb"].includes(dest.type) && (
              <>
                <div className="space-y-2">
                  <Label>Host</Label>
                  <Input
                    value={dest.host || ""}
                    onChange={(e) => updateDestination(index, "host", e.target.value)}
                    placeholder="localhost"
                  />
                </div>

                <div className="space-y-2">
                  <Label>Port</Label>
                  <Input
                    type="number"
                    value={dest.port || ""}
                    onChange={(e) => updateDestination(index, "port", parseInt(e.target.value))}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Username</Label>
                  <Input
                    value={dest.username || ""}
                    onChange={(e) => updateDestination(index, "username", e.target.value)}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Password</Label>
                  <Input
                    type="password"
                    value={dest.password || ""}
                    onChange={(e) => updateDestination(index, "password", e.target.value)}
                  />
                </div>

                <div className="space-y-2 col-span-2">
                  <Label>Database</Label>
                  <Input
                    value={dest.database || ""}
                    onChange={(e) => updateDestination(index, "database", e.target.value)}
                  />
                </div>
              </>
            )}

            {["csv", "json"].includes(dest.type) && (
              <div className="space-y-2 col-span-2">
                <Label>File Path</Label>
                <Input
                  value={dest.file || ""}
                  onChange={(e) => updateDestination(index, "file", e.target.value)}
                  placeholder="/path/to/output/file"
                />
              </div>
            )}
          </div>
        </Card>
      ))}
    </div>
  );
}
