import { ETLTableMapping, ETLDestination } from "@/types/etl";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";

interface TablesStepProps {
  tables: ETLTableMapping[];
  destinations: ETLDestination[];
  onChange: (tables: ETLTableMapping[]) => void;
}

export function TablesStep({ tables, destinations, onChange }: TablesStepProps) {
  const addTable = () => {
    onChange([
      ...tables,
      {
        name: "",
        old_name: "",
        dest_key: destinations[0]?.key || "",
        migrate: true,
        clone_source: false,
        truncate_dest: false,
        key_value_table: false,
        batch_size: 1000,
        mapping: {},
      },
    ]);
  };

  const removeTable = (index: number) => {
    onChange(tables.filter((_, i) => i !== index));
  };

  const updateTable = (index: number, field: keyof ETLTableMapping, value: any) => {
    const updated = [...tables];
    updated[index] = { ...updated[index], [field]: value };
    onChange(updated);
  };

  const updateMapping = (index: number, mappingText: string) => {
    const mapping: Record<string, string> = {};
    mappingText.split("\n").forEach((line) => {
      const [key, value] = line.split("=");
      if (key && value) {
        mapping[key.trim()] = value.trim();
      }
    });
    updateTable(index, "mapping", mapping);
  };

  const getMappingText = (mapping: Record<string, string>) => {
    return Object.entries(mapping)
      .map(([key, value]) => `${key}=${value}`)
      .join("\n");
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center mb-4">
        <div>
          <h3 className="text-lg font-semibold">Configure Table Mappings</h3>
          <p className="text-sm text-muted-foreground">
            Define how tables should be transformed and mapped
          </p>
        </div>
        <Button onClick={addTable} size="sm" disabled={destinations.length === 0}>
          <Plus className="h-4 w-4 mr-2" />
          Add Table
        </Button>
      </div>

      {destinations.length === 0 && (
        <Card className="p-8 text-center">
          <p className="text-muted-foreground">
            Please configure destinations first before adding table mappings
          </p>
        </Card>
      )}

      {tables.length === 0 && destinations.length > 0 && (
        <Card className="p-8 text-center">
          <p className="text-muted-foreground mb-4">No table mappings configured yet</p>
          <Button onClick={addTable}>
            <Plus className="h-4 w-4 mr-2" />
            Add Your First Table Mapping
          </Button>
        </Card>
      )}

      {tables.map((table, index) => (
        <Card key={index} className="p-6">
          <div className="flex justify-between items-start mb-4">
            <h4 className="font-medium">Table Mapping #{index + 1}</h4>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => removeTable(index)}
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>

          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>Table Name *</Label>
                <Input
                  value={table.name}
                  onChange={(e) => updateTable(index, "name", e.target.value)}
                  placeholder="e.g., users"
                />
              </div>

              <div className="space-y-2">
                <Label>Old Name (Source)</Label>
                <Input
                  value={table.old_name || ""}
                  onChange={(e) => updateTable(index, "old_name", e.target.value)}
                  placeholder="Leave empty if same as table name"
                />
              </div>

              <div className="space-y-2">
                <Label>Destination Key *</Label>
                <Select
                  value={table.dest_key}
                  onValueChange={(value) => updateTable(index, "dest_key", value)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {destinations.map((dest) => (
                      <SelectItem key={dest.key} value={dest.key}>
                        {dest.key} ({dest.type})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Batch Size</Label>
                <Input
                  type="number"
                  value={table.batch_size}
                  onChange={(e) => updateTable(index, "batch_size", parseInt(e.target.value))}
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`migrate-${index}`}
                  checked={table.migrate}
                  onCheckedChange={(checked) => updateTable(index, "migrate", checked)}
                />
                <Label htmlFor={`migrate-${index}`} className="cursor-pointer">
                  Migrate data
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`clone-${index}`}
                  checked={table.clone_source}
                  onCheckedChange={(checked) => updateTable(index, "clone_source", checked)}
                />
                <Label htmlFor={`clone-${index}`} className="cursor-pointer">
                  Clone source schema
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`truncate-${index}`}
                  checked={table.truncate_dest}
                  onCheckedChange={(checked) => updateTable(index, "truncate_dest", checked)}
                />
                <Label htmlFor={`truncate-${index}`} className="cursor-pointer">
                  Truncate destination
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`keyvalue-${index}`}
                  checked={table.key_value_table || false}
                  onCheckedChange={(checked) => updateTable(index, "key_value_table", checked)}
                />
                <Label htmlFor={`keyvalue-${index}`} className="cursor-pointer">
                  Key-value table
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`autocreate-${index}`}
                  checked={table.auto_create_table || false}
                  onCheckedChange={(checked) => updateTable(index, "auto_create_table", checked)}
                />
                <Label htmlFor={`autocreate-${index}`} className="cursor-pointer">
                  Auto create table
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`enablebatch-${index}`}
                  checked={table.enable_batch || false}
                  onCheckedChange={(checked) => updateTable(index, "enable_batch", checked)}
                />
                <Label htmlFor={`enablebatch-${index}`} className="cursor-pointer">
                  Enable batch
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`updateseq-${index}`}
                  checked={table.update_sequence || false}
                  onCheckedChange={(checked) => updateTable(index, "update_sequence", checked)}
                />
                <Label htmlFor={`updateseq-${index}`} className="cursor-pointer">
                  Update sequence
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Checkbox
                  id={`skipstore-${index}`}
                  checked={table.skip_store_error || false}
                  onCheckedChange={(checked) => updateTable(index, "skip_store_error", checked)}
                />
                <Label htmlFor={`skipstore-${index}`} className="cursor-pointer">
                  Skip store error
                </Label>
              </div>
            </div>

            <div className="space-y-2">
              <Label>Field Mapping (key=value pairs, one per line)</Label>
              <Textarea
                value={getMappingText(table.mapping)}
                onChange={(e) => updateMapping(index, e.target.value)}
                placeholder="source_field=dest_field&#10;old_name=new_name"
                rows={4}
                className="font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">
                Leave empty to use automatic field mapping
              </p>
            </div>
          </div>
        </Card>
      ))}
    </div>
  );
}
