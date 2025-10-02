import { ETLSource, ETLDestination, ETLTableMapping, Deduplication, Checkpoint, Lookup } from "@/types/etl";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Database, ArrowRight, Table, Settings } from "lucide-react";

interface ReviewStepProps {
  name: string;
  description: string;
  sources: ETLSource[];
  destinations: ETLDestination[];
  tables: ETLTableMapping[];
  deduplication?: Deduplication;
  checkpoint?: Checkpoint;
  lookups?: Lookup[];
  streamingMode: boolean;
  distributedMode: boolean;
}

export function ReviewStep({
  name,
  description,
  sources,
  destinations,
  tables,
  deduplication,
  checkpoint,
  lookups,
  streamingMode,
  distributedMode,
}: ReviewStepProps) {
  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold mb-4">Review Your Pipeline</h3>
        <p className="text-sm text-muted-foreground">
          Please review the configuration before creating the pipeline
        </p>
      </div>

      {/* Basic Info */}
      <Card className="p-6">
        <h4 className="font-medium mb-3">Basic Information</h4>
        <div className="space-y-2">
          <div>
            <span className="text-sm text-muted-foreground">Name:</span>
            <p className="font-medium">{name}</p>
          </div>
          {description && (
            <div>
              <span className="text-sm text-muted-foreground">Description:</span>
              <p className="text-sm">{description}</p>
            </div>
          )}
        </div>
      </Card>

      {/* Sources */}
      <Card className="p-6">
        <div className="flex items-center gap-2 mb-3">
          <Database className="h-4 w-4 text-primary" />
          <h4 className="font-medium">Sources ({sources.length})</h4>
        </div>
        <div className="space-y-3">
          {sources.map((source, index) => (
            <div key={index} className="flex items-center justify-between p-3 bg-muted rounded-lg">
              <div>
                <p className="font-medium text-sm">{source.key}</p>
                <p className="text-xs text-muted-foreground">
                  {source.type.toUpperCase()}
                  {source.host && ` - ${source.host}:${source.port}`}
                  {source.file && ` - ${source.file}`}
                </p>
              </div>
              <Badge variant="outline">{source.type}</Badge>
            </div>
          ))}
        </div>
      </Card>

      {/* Destinations */}
      <Card className="p-6">
        <div className="flex items-center gap-2 mb-3">
          <Database className="h-4 w-4 text-primary" />
          <h4 className="font-medium">Destinations ({destinations.length})</h4>
        </div>
        <div className="space-y-3">
          {destinations.map((dest, index) => (
            <div key={index} className="flex items-center justify-between p-3 bg-muted rounded-lg">
              <div>
                <p className="font-medium text-sm">{dest.key}</p>
                <p className="text-xs text-muted-foreground">
                  {dest.type.toUpperCase()}
                  {dest.host && ` - ${dest.host}:${dest.port}`}
                  {dest.file && ` - ${dest.file}`}
                </p>
              </div>
              <Badge variant="outline">{dest.type}</Badge>
            </div>
          ))}
        </div>
      </Card>

      {/* Tables */}
      <Card className="p-6">
        <div className="flex items-center gap-2 mb-3">
          <Table className="h-4 w-4 text-primary" />
          <h4 className="font-medium">Table Mappings ({tables.length})</h4>
        </div>
        <div className="space-y-3">
          {tables.map((table, index) => (
            <div key={index} className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <p className="font-medium">{table.name}</p>
                <ArrowRight className="h-4 w-4 text-muted-foreground" />
                <Badge variant="secondary">{table.dest_key}</Badge>
              </div>
              <div className="flex flex-wrap gap-2 text-xs">
                {table.migrate && <Badge variant="outline">Migrate</Badge>}
                {table.clone_source && <Badge variant="outline">Clone Source</Badge>}
                {table.truncate_dest && <Badge variant="outline">Truncate</Badge>}
                {table.key_value_table && <Badge variant="outline">Key-Value</Badge>}
                {table.auto_create_table && <Badge variant="outline">Auto Create</Badge>}
                <Badge variant="outline">Batch: {table.batch_size}</Badge>
              </div>
              {Object.keys(table.mapping).length > 0 && (
                <div className="mt-2 pt-2 border-t">
                  <p className="text-xs text-muted-foreground mb-1">Field Mappings:</p>
                  <div className="space-y-1">
                    {Object.entries(table.mapping).map(([key, value]) => (
                      <p key={key} className="text-xs font-mono">
                        {key} → {value}
                      </p>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </Card>

      {/* Advanced Options */}
      {(deduplication || checkpoint || lookups?.length || streamingMode || distributedMode) && (
        <Card className="p-6">
          <div className="flex items-center gap-2 mb-3">
            <Settings className="h-4 w-4 text-primary" />
            <h4 className="font-medium">Advanced Configuration</h4>
          </div>
          <div className="space-y-3">
            {streamingMode && (
              <div className="flex items-center gap-2">
                <Badge variant="secondary">Streaming Mode Enabled</Badge>
              </div>
            )}
            {distributedMode && (
              <div className="flex items-center gap-2">
                <Badge variant="secondary">Distributed Mode Enabled</Badge>
              </div>
            )}
            {deduplication && (
              <div className="p-3 bg-muted rounded-lg">
                <p className="text-sm font-medium mb-1">Deduplication</p>
                <p className="text-xs text-muted-foreground">Field: {deduplication.field}</p>
              </div>
            )}
            {checkpoint && (
              <div className="p-3 bg-muted rounded-lg">
                <p className="text-sm font-medium mb-1">Checkpoint</p>
                <p className="text-xs text-muted-foreground">
                  File: {checkpoint.file}, Field: {checkpoint.field}
                </p>
              </div>
            )}
            {lookups && lookups.length > 0 && (
              <div className="p-3 bg-muted rounded-lg">
                <p className="text-sm font-medium mb-1">Lookups ({lookups.length})</p>
                <div className="space-y-1">
                  {lookups.map((lookup, i) => (
                    <p key={i} className="text-xs text-muted-foreground">
                      {lookup.key}: {lookup.file} ({lookup.type})
                    </p>
                  ))}
                </div>
              </div>
            )}
          </div>
        </Card>
      )}
    </div>
  );
}
