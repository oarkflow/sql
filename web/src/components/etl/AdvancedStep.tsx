import { Deduplication, Checkpoint, Lookup } from "@/types/etl";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";

interface AdvancedStepProps {
  deduplication?: Deduplication;
  checkpoint?: Checkpoint;
  lookups: Lookup[];
  streamingMode: boolean;
  distributedMode: boolean;
  onDeduplicationChange: (deduplication?: Deduplication) => void;
  onCheckpointChange: (checkpoint?: Checkpoint) => void;
  onLookupsChange: (lookups: Lookup[]) => void;
  onStreamingModeChange: (enabled: boolean) => void;
  onDistributedModeChange: (enabled: boolean) => void;
}

export function AdvancedStep({
  deduplication,
  checkpoint,
  lookups,
  streamingMode,
  distributedMode,
  onDeduplicationChange,
  onCheckpointChange,
  onLookupsChange,
  onStreamingModeChange,
  onDistributedModeChange,
}: AdvancedStepProps) {
  const addLookup = () => {
    onLookupsChange([
      ...lookups,
      { type: "csv", file: "", key: "" },
    ]);
  };

  const removeLookup = (index: number) => {
    onLookupsChange(lookups.filter((_, i) => i !== index));
  };

  const updateLookup = (index: number, field: keyof Lookup, value: string) => {
    const updated = [...lookups];
    updated[index] = { ...updated[index], [field]: value };
    onLookupsChange(updated);
  };

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold mb-2">Advanced Configuration</h3>
        <p className="text-sm text-muted-foreground">
          Optional advanced features for your pipeline
        </p>
      </div>

      {/* Processing Modes */}
      <Card className="p-6">
        <h4 className="font-medium mb-4">Processing Modes</h4>
        <div className="space-y-3">
          <div className="flex items-center space-x-2">
            <Checkbox
              id="streaming"
              checked={streamingMode}
              onCheckedChange={onStreamingModeChange}
            />
            <Label htmlFor="streaming" className="cursor-pointer">
              Enable Streaming Mode
            </Label>
          </div>
          <div className="flex items-center space-x-2">
            <Checkbox
              id="distributed"
              checked={distributedMode}
              onCheckedChange={onDistributedModeChange}
            />
            <Label htmlFor="distributed" className="cursor-pointer">
              Enable Distributed Mode
            </Label>
          </div>
        </div>
      </Card>

      {/* Deduplication */}
      <Card className="p-6">
        <div className="flex items-center justify-between mb-4">
          <h4 className="font-medium">Deduplication</h4>
          <Checkbox
            checked={!!deduplication}
            onCheckedChange={(checked) => {
              if (checked) {
                onDeduplicationChange({ enabled: true, field: "" });
              } else {
                onDeduplicationChange(undefined);
              }
            }}
          />
        </div>
        
        {deduplication && (
          <div className="space-y-4">
            <div className="space-y-2">
              <Label>Deduplication Field</Label>
              <Input
                value={deduplication.field}
                onChange={(e) =>
                  onDeduplicationChange({ ...deduplication, field: e.target.value })
                }
                placeholder="e.g., user_id"
              />
            </div>
          </div>
        )}
      </Card>

      {/* Checkpoint */}
      <Card className="p-6">
        <div className="flex items-center justify-between mb-4">
          <h4 className="font-medium">Checkpoint</h4>
          <Checkbox
            checked={!!checkpoint}
            onCheckedChange={(checked) => {
              if (checked) {
                onCheckpointChange({ file: "checkpoint.txt", field: "" });
              } else {
                onCheckpointChange(undefined);
              }
            }}
          />
        </div>
        
        {checkpoint && (
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Checkpoint File</Label>
              <Input
                value={checkpoint.file}
                onChange={(e) =>
                  onCheckpointChange({ ...checkpoint, file: e.target.value })
                }
                placeholder="checkpoint.txt"
              />
            </div>
            <div className="space-y-2">
              <Label>Checkpoint Field</Label>
              <Input
                value={checkpoint.field}
                onChange={(e) =>
                  onCheckpointChange({ ...checkpoint, field: e.target.value })
                }
                placeholder="e.g., id"
              />
            </div>
          </div>
        )}
      </Card>

      {/* Lookups */}
      <Card className="p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h4 className="font-medium">Lookups</h4>
            <p className="text-sm text-muted-foreground">
              Reference data for transformations
            </p>
          </div>
          <Button onClick={addLookup} size="sm">
            <Plus className="h-4 w-4 mr-2" />
            Add Lookup
          </Button>
        </div>

        {lookups.length > 0 && (
          <div className="space-y-4">
            {lookups.map((lookup, index) => (
              <div key={index} className="p-4 border rounded-lg">
                <div className="flex justify-between items-start mb-3">
                  <span className="text-sm font-medium">Lookup #{index + 1}</span>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => removeLookup(index)}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>

                <div className="grid grid-cols-3 gap-3">
                  <div className="space-y-2">
                    <Label>Type</Label>
                    <Select
                      value={lookup.type}
                      onValueChange={(value) => updateLookup(index, "type", value)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="csv">CSV</SelectItem>
                        <SelectItem value="json">JSON</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>File</Label>
                    <Input
                      value={lookup.file}
                      onChange={(e) => updateLookup(index, "file", e.target.value)}
                      placeholder="data.csv"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Key</Label>
                    <Input
                      value={lookup.key}
                      onChange={(e) => updateLookup(index, "key", e.target.value)}
                      placeholder="lookup_key"
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}
