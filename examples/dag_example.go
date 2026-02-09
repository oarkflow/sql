package main

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/sql/pkg/workflow"
)

func main() {
	// Create a new Directed Acyclic Graph (DAG)
	dag := workflow.NewDAG()

	// Add nodes (Jobs) to the DAG.
	// Syntax: dag.AddNode(jobID, dependencyID1, dependencyID2, ...)

	// 'extraction' has no dependencies (Root node)
	dag.AddNode("extract_users", nil)
	dag.AddNode("extract_orders", nil)

	// 'transform' depends on 'extraction'
	dag.AddNode("transform_users", nil, "extract_users")
	dag.AddNode("transform_orders", nil, "extract_orders")

	// 'enrich' depends on both transforms (e.g. joining users and orders)
	dag.AddNode("enrich_data", nil, "transform_users", "transform_orders")

	// 'load' depends on 'enrich'
	dag.AddNode("load_dw", nil, "enrich_data")

	// 'notify' happens at the end
	dag.AddNode("send_notification", nil, "load_dw")

	fmt.Println("--- DAG Structure ---")
	// In a real scenario, you'd inspect the graph or visualize it.

	// Get the execution plan (Layers)
	// The planner groups jobs that can run in parallel into layers.
	plan, err := dag.GetExecutionPlan()
	if err != nil {
		log.Fatalf("Failed to generate plan: %v", err)
	}

	fmt.Println("\n--- Execution Plan (Topological Sort) ---")
	for i, layer := range plan {
		fmt.Printf("Layer %d: %v\n", i+1, layer)
	}

	fmt.Println("\n--- Simulating Execution ---")
	simulateExecution(plan)
}

func simulateExecution(plan [][]string) {
	for i, layer := range plan {
		fmt.Printf("Executing Layer %d...\n", i+1)

		// In a real engine, these would be goroutines/workers
		done := make(chan string)
		for _, jobID := range layer {
			go func(id string) {
				// Simulate work
				time.Sleep(100 * time.Millisecond)
				fmt.Printf("  ✅ Job '%s' completed\n", id)
				done <- id
			}(jobID)
		}

		// Wait for layer to complete before moving to next
		for range layer {
			<-done
		}
		fmt.Printf("Layer %d complete.\n\n", i+1)
	}
	fmt.Println("🎉 Workflow completed successfully!")
}
