package orchestrator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/events"
	"github.com/oarkflow/sql/pkg/workflow"
)

// Orchestrator sits above the ETL runners and manages complex workflows (DAGs)
// triggered by events.
type Orchestrator struct {
	runner     etl.Runner
	eventBus   *events.EventBus
	workflows  map[string]*workflow.DAG
	mu         sync.RWMutex
}

func New(runner etl.Runner, bus *events.EventBus) *Orchestrator {
	return &Orchestrator{
		runner:    runner,
		eventBus:  bus,
		workflows: make(map[string]*workflow.DAG),
	}
}

// RegisterWorkflow stores a DAG definition with a unique ID
func (o *Orchestrator) RegisterWorkflow(id string, dag *workflow.DAG) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workflows[id] = dag
}

// RegisterTrigger binds an event to a workflow execution
func (o *Orchestrator) RegisterTrigger(eventType events.EventType, workflowID string) {
	o.eventBus.Subscribe(eventType, func(ctx context.Context, e events.Event) error {
		log.Printf("[Orchestrator] Triggered workflow '%s' by event %s (Source: %s)", workflowID, e.Type, e.Source)
		// Run workflow in background
		go o.RunWorkflow(context.Background(), workflowID)
		return nil
	})
}

// RunWorkflow executes a stored DAG
func (o *Orchestrator) RunWorkflow(ctx context.Context, workflowID string) error {
	o.mu.RLock()
	dag, exists := o.workflows[workflowID]
	o.mu.RUnlock()

	if !exists {
		return fmt.Errorf("workflow %s not found", workflowID)
	}

	plan, err := dag.GetExecutionPlan()
	if err != nil {
		return fmt.Errorf("invalid DAG plan: %w", err)
	}

	log.Printf("[Orchestrator] Starting workflow '%s' execution with %d layers", workflowID, len(plan))

	for i, layer := range plan {
		// Execute layer in parallel
		log.Printf("  [Layer %d] Executing %d jobs...", i+1, len(layer))

		var wg sync.WaitGroup
		errChan := make(chan error, len(layer))

		for _, nodeID := range layer {
			wg.Add(1)
			go func(nid string) {
				defer wg.Done()
				if err := o.executeJob(ctx, dag, nid); err != nil {
					errChan <- fmt.Errorf("job %s failed: %w", nid, err)
				}
			}(nodeID)
		}

		wg.Wait()
		close(errChan)

		// Check for failures in this layer
		if len(errChan) > 0 {
			// One or more jobs failed.
			// In a future version, we handle 'ContinueOnFailure' or 'Rollback'
			return <-errChan // Return first error
		}
	}

	log.Printf("[Orchestrator] Workflow '%s' completed successfully", workflowID)

	// Publish Success Event
	o.eventBus.Publish(ctx, events.Event{
		Type:      events.EventJobCompleted,
		Source:    "orchestrator",
		Payload:   map[string]any{"workflow_id": workflowID},
		Timestamp: time.Now(),
	})

	return nil
}

func (o *Orchestrator) executeJob(ctx context.Context, dag *workflow.DAG, nodeID string) error {
	// 1. Retrieve job definition from Node Payload
	node := dag.GetNode(nodeID)
	if node == nil {
		return fmt.Errorf("node %s not found in DAG", nodeID)
	}

	// Assuming Payload is *config.Config or similar structure that we can pass to Job
	var jobConfig *config.Config
	if cfg, ok := node.Payload.(*config.Config); ok {
		jobConfig = cfg
	} else {
		// If payload is not a config, maybe we can create a dummy one or fail
		// For demo safety, we skip if nil but logging it
		if node.Payload != nil {
			log.Printf("Warning: Node %s payload is not *config.Config", nodeID)
		}
	}

	job := &etl.Job{
		ID:        nodeID,
		Name:      "Orchestrated Job - " + nodeID,
		Status:    etl.JobStatusPending,
		CreatedAt: time.Now(),
		Config:    jobConfig,
	}

	// 2. Submit to actual Worker/Runner
	if o.runner == nil {
		log.Printf("    -> [Simulated] Executing Job %s", nodeID)
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	if err := o.runner.Submit(ctx, job); err != nil {
		return err
	}

	// 3. Wait for completion
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentJob, err := o.runner.GetJob(nodeID)
			if err != nil {
				return err
			}

			switch currentJob.Status {
			case etl.JobStatusCompleted:
				log.Printf("    Job %s completed", nodeID)
				return nil
			case etl.JobStatusFailed:
				return fmt.Errorf("job %s failed: %s", nodeID, currentJob.Error)
			case etl.JobStatusCancelled:
				return fmt.Errorf("job %s cancelled", nodeID)
			}
			// Continue waiting if Pending/Running
		}
	}
}
