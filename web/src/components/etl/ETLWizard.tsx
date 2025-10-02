import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { mockApi } from "@/lib/mockApi";
import { ETLPipeline, ETLSource, ETLDestination, ETLTableMapping, Deduplication, Checkpoint, Lookup } from "@/types/etl";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import { Progress } from "@/components/ui/progress";
import { ArrowLeft, ArrowRight, Check } from "lucide-react";
import { SourcesStep } from "./SourcesStep";
import { DestinationsStep } from "./DestinationsStep";
import { TablesStep } from "./TablesStep";
import { AdvancedStep } from "./AdvancedStep";
import { ReviewStep } from "./ReviewStep";
import { useToast } from "@/hooks/use-toast";

interface ETLWizardProps {
  pipeline?: ETLPipeline;
}

export function ETLWizard({ pipeline }: ETLWizardProps) {
  const [currentStep, setCurrentStep] = useState(0);
  const [name, setName] = useState(pipeline?.name || "");
  const [description, setDescription] = useState(pipeline?.description || "");
  const [sources, setSources] = useState<ETLSource[]>(pipeline?.sources || []);
  const [destinations, setDestinations] = useState<ETLDestination[]>(pipeline?.destinations || []);
  const [tables, setTables] = useState<ETLTableMapping[]>(pipeline?.tables || []);
  const [deduplication, setDeduplication] = useState<Deduplication | undefined>(pipeline?.deduplication);
  const [checkpoint, setCheckpoint] = useState<Checkpoint | undefined>(pipeline?.checkpoint);
  const [lookups, setLookups] = useState<Lookup[]>(pipeline?.lookups || []);
  const [streamingMode, setStreamingMode] = useState(pipeline?.streaming_mode || false);
  const [distributedMode, setDistributedMode] = useState(pipeline?.distributed_mode || false);
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const steps = [
    { label: "Basic Info" },
    { label: "Sources" },
    { label: "Destinations" },
    { label: "Tables" },
    { label: "Advanced" },
    { label: "Review" },
  ];

  const progress = ((currentStep + 1) / steps.length) * 100;

  const createMutation = useMutation({
    mutationFn: (data: Omit<ETLPipeline, "id" | "createdAt">) =>
      pipeline ? mockApi.updatePipeline(pipeline.id, data) : mockApi.createPipeline(data),
    onSuccess: () => {
      toast({
        title: pipeline ? "Pipeline Updated" : "Pipeline Created",
        description: pipeline ? "Your pipeline has been updated successfully" : "Your new pipeline is ready to run",
      });
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      navigate("/pipelines");
    },
    onError: () => {
      toast({
        title: "Error",
        description: "Failed to save pipeline",
        variant: "destructive",
      });
    },
  });

  const handleNext = () => {
    if (currentStep < steps.length - 1) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handlePrevious = () => {
    if (currentStep > 0) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleSubmit = () => {
    const pipelineData: Omit<ETLPipeline, "id" | "createdAt"> = {
      name,
      description,
      sources,
      destinations,
      tables,
      deduplication,
      checkpoint,
      lookups: lookups.length > 0 ? lookups : undefined,
      streaming_mode: streamingMode,
      distributed_mode: distributedMode,
      status: pipeline?.status || "draft",
      lastRun: pipeline?.lastRun,
    };
    createMutation.mutate(pipelineData);
  };

  const canProceed = () => {
    switch (currentStep) {
      case 0:
        return name.trim() !== "";
      case 1:
        return sources.length > 0;
      case 2:
        return destinations.length > 0;
      case 3:
        return tables.length > 0;
      case 4:
        return true; // Advanced step is optional
      default:
        return true;
    }
  };

  const renderStep = () => {
    switch (currentStep) {
      case 0:
        return (
          <div className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="name">Pipeline Name *</Label>
              <Input
                id="name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g., User Data Migration"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="description">Description</Label>
              <Textarea
                id="description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Describe what this pipeline does..."
                rows={4}
              />
            </div>
          </div>
        );
      case 1:
        return <SourcesStep sources={sources} onChange={setSources} />;
      case 2:
        return <DestinationsStep destinations={destinations} onChange={setDestinations} />;
      case 3:
        return <TablesStep tables={tables} onChange={setTables} destinations={destinations} />;
      case 4:
        return (
          <AdvancedStep
            deduplication={deduplication}
            checkpoint={checkpoint}
            lookups={lookups}
            streamingMode={streamingMode}
            distributedMode={distributedMode}
            onDeduplicationChange={setDeduplication}
            onCheckpointChange={setCheckpoint}
            onLookupsChange={setLookups}
            onStreamingModeChange={setStreamingMode}
            onDistributedModeChange={setDistributedMode}
          />
        );
      case 5:
        return (
          <ReviewStep
            name={name}
            description={description}
            sources={sources}
            destinations={destinations}
            tables={tables}
            deduplication={deduplication}
            checkpoint={checkpoint}
            lookups={lookups}
            streamingMode={streamingMode}
            distributedMode={distributedMode}
          />
        );
      default:
        return null;
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        <div className="mb-8">
          <Button variant="ghost" onClick={() => navigate("/pipelines")} className="mb-4">
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Pipelines
          </Button>
          <h1 className="text-3xl font-bold text-foreground mb-2">
            {pipeline ? "Edit Pipeline" : "Create ETL Pipeline"}
          </h1>
          <p className="text-muted-foreground">
            Configure your data transformation pipeline step by step
          </p>
        </div>

        <Card className="p-8">
          {/* Progress Bar */}
          <div className="mb-8">
            <div className="flex justify-between mb-3">
              {steps.map((step, index) => (
                <div
                  key={index}
                  className={`text-sm font-medium transition-colors ${
                    index === currentStep
                      ? "text-primary"
                      : index < currentStep
                      ? "text-primary/60"
                      : "text-muted-foreground"
                  }`}
                >
                  {step.label}
                </div>
              ))}
            </div>
            <Progress value={progress} className="h-2" />
          </div>

          {/* Step Content */}
          <div className="min-h-[400px] mb-8">{renderStep()}</div>

          {/* Navigation */}
          <div className="flex justify-between">
            <Button
              variant="outline"
              onClick={handlePrevious}
              disabled={currentStep === 0}
            >
              <ArrowLeft className="h-4 w-4 mr-2" />
              Previous
            </Button>

            {currentStep === steps.length - 1 ? (
              <Button
                onClick={handleSubmit}
                disabled={createMutation.isPending || !canProceed()}
              >
                <Check className="h-4 w-4 mr-2" />
                {pipeline ? "Update Pipeline" : "Create Pipeline"}
              </Button>
            ) : (
              <Button onClick={handleNext} disabled={!canProceed()}>
                Next
                <ArrowRight className="h-4 w-4 ml-2" />
              </Button>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}
