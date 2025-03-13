// File: example_all_cases.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/etl/pkg/resilience"
)

// -----------------------------
// Dummy Resource Implementations
// -----------------------------

// DummyResource implements TransactionalResource and can simulate failures.
type DummyResource struct {
	Name         string
	FailCommit   bool
	FailRollback bool
}

func (dr *DummyResource) Commit(ctx context.Context) error {
	if dr.FailCommit {
		return fmt.Errorf("resource %s commit failed", dr.Name)
	}
	log.Printf("DummyResource %s committed", dr.Name)
	return nil
}

func (dr *DummyResource) Rollback(ctx context.Context) error {
	if dr.FailRollback {
		return fmt.Errorf("resource %s rollback failed", dr.Name)
	}
	log.Printf("DummyResource %s rolled back", dr.Name)
	return nil
}

// DummyPreparableResource implements PreparableResource.
type DummyPreparableResource struct {
	Name        string
	FailPrepare bool
}

func (dpr *DummyPreparableResource) Prepare(ctx context.Context) error {
	if dpr.FailPrepare {
		return fmt.Errorf("resource %s prepare failed", dpr.Name)
	}
	log.Printf("DummyPreparableResource %s prepared", dpr.Name)
	return nil
}

func (dpr *DummyPreparableResource) Commit(ctx context.Context) error {
	log.Printf("DummyPreparableResource %s committed", dpr.Name)
	return nil
}

func (dpr *DummyPreparableResource) Rollback(ctx context.Context) error {
	log.Printf("DummyPreparableResource %s rolled back", dpr.Name)
	return nil
}

// -----------------------------
// Dummy Tracer Implementation
// -----------------------------

// DummyTracer implements the Tracer interface.
type DummyTracer struct{}

type DummySpan struct {
	name string
}

func (dt *DummyTracer) StartSpan(ctx context.Context, name string) (context.Context, resilience.Span) {
	log.Printf("Starting span: %s", name)
	return ctx, &DummySpan{name: name}
}

func (ds *DummySpan) End() {
	log.Printf("Ending span: %s", ds.name)
}

func (ds *DummySpan) SetAttributes(attrs map[string]interface{}) {
	log.Printf("Span %s attributes: %v", ds.name, attrs)
}

// -----------------------------
// Dummy Distributed Coordinator
// -----------------------------

type DummyDistributedCoordinator struct{}

func (ddc *DummyDistributedCoordinator) BeginDistributed(tx *resilience.Transaction) error {
	log.Printf("DistributedCoordinator: Begin transaction %d", tx.GetID())
	return nil
}

func (ddc *DummyDistributedCoordinator) CommitDistributed(tx *resilience.Transaction) error {
	log.Printf("DistributedCoordinator: Commit transaction %d", tx.GetID())
	return nil
}

func (ddc *DummyDistributedCoordinator) RollbackDistributed(tx *resilience.Transaction) error {
	log.Printf("DistributedCoordinator: Rollback transaction %d", tx.GetID())
	return nil
}

// -----------------------------
// Custom Lifecycle Hooks
// -----------------------------

var lifecycleHooks = &resilience.LifecycleHooks{
	OnBegin: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d begun", txID)
	},
	OnBeforeCommit: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d before commit", txID)
	},
	OnAfterCommit: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d after commit", txID)
	},
	OnBeforeRollback: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d before rollback", txID)
	},
	OnAfterRollback: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d after rollback", txID)
	},
	OnClose: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d closed", txID)
	},
}

// -----------------------------
// Custom Retry Policy for Per-Action Usage
// -----------------------------

var customRetryPolicy = resilience.RetryPolicy{
	MaxRetries: 2,
	Delay:      200 * time.Millisecond,
	ShouldRetry: func(err error) bool {
		return true
	},
	BackoffStrategy: func(attempt int) time.Duration {
		return time.Duration(100*(1<<attempt)) * time.Millisecond
	},
}

// -----------------------------
// Example Functions
// -----------------------------

// Example 1: Basic Successful Transaction
func exampleBasicTransaction() {
	log.Println("=== Example 1: Basic Successful Transaction ===")
	tx := resilience.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Basic commit action executed")
		return nil
	})
	res := &DummyResource{Name: "BasicResource"}
	tx.RegisterResource(res)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Printf("Transaction %d state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 2: Simulated Commit Failure
func exampleSimulatedCommitFailure() {
	log.Println("=== Example 2: Simulated Commit Failure ===")
	tx := resilience.NewTransaction()
	ctx := context.Background()
	tx.SetTestHooks(&resilience.TestHooks{SimulateCommitFailure: true})
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action (will be ignored due to simulated failure)")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected commit failure: %v", err)
		log.Printf("Transaction state: %s", tx.GetState().String())
	}
}

// Example 3: Simulated Prepare Failure
func exampleSimulatedPrepareFailure() {
	log.Println("=== Example 3: Simulated Prepare Failure ===")
	tx := resilience.NewTransaction()
	ctx := context.Background()
	tx.SetTestHooks(&resilience.TestHooks{SimulatePrepareFailure: true})
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action (ignored due to prepare failure)")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected prepare failure: %v", err)
	}
}

// Example 4: Simulated Rollback Failure
func exampleSimulatedRollbackFailure() {
	log.Println("=== Example 4: Simulated Rollback Failure ===")
	tx := resilience.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	// Force a commit action error to trigger rollback.
	tx.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated commit error to trigger rollback")
	})
	tx.SetTestHooks(&resilience.TestHooks{SimulateRollbackFailure: true})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected rollback failure: %v", err)
	}
}

// Example 5: Nested Transactions and Savepoints
func exampleNestedTransactions() {
	log.Println("=== Example 5: Nested Transactions and Savepoints ===")
	tx := resilience.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	// Create an unnamed savepoint.
	sp, err := tx.CreateSavepoint(ctx)
	if err != nil {
		log.Printf("Error creating savepoint: %v", err)
		return
	}
	log.Printf("Created unnamed savepoint at index %d", sp)
	// Create a named savepoint.
	if err := tx.CreateNamedSavepoint("namedSP"); err != nil {
		log.Printf("Error creating named savepoint: %v", err)
	}
	// Begin a nested transaction.
	nestedTx, err := tx.BeginNested(ctx)
	if err != nil {
		log.Printf("Error beginning nested transaction: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Nested commit action executed")
		return nil
	})
	if err := nestedTx.Commit(ctx); err != nil {
		log.Printf("Nested transaction commit error: %v", err)
	} else {
		log.Println("Nested transaction committed")
	}
	// Rollback to the named savepoint.
	if err := tx.RollbackToNamedSavepoint(ctx, "namedSP"); err != nil {
		log.Printf("Error rolling back to named savepoint: %v", err)
	} else {
		log.Println("Rolled back to named savepoint 'namedSP'")
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Final commit error after nested transactions: %v", err)
	} else {
		log.Printf("Final transaction committed, state: %s", tx.GetState().String())
	}
}

// Example 6: Asynchronous Commit and Rollback
func exampleAsyncOperations() {
	log.Println("=== Example 6: Asynchronous Operations ===")
	// Asynchronous Commit
	tx := resilience.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Async commit action executed")
		return nil
	})
	res := &DummyResource{Name: "AsyncResource"}
	tx.RegisterResource(res)
	asyncCommit := tx.AsyncCommitWithResult(ctx)
	result := <-asyncCommit
	if result.Err != nil {
		log.Printf("Async commit error: %v", result.Err)
	} else {
		log.Printf("Async commit succeeded, state: %s", result.State.String())
	}
	// Asynchronous Rollback Example
	tx2 := resilience.NewTransaction()
	if err := tx2.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx2.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated error to trigger async rollback")
	})
	asyncRollback := tx2.AsyncRollbackWithResult(ctx)
	rbResult := <-asyncRollback
	if rbResult.Err != nil {
		log.Printf("Async rollback error: %v", rbResult.Err)
	} else {
		log.Printf("Async rollback succeeded, state: %s", rbResult.State.String())
	}
}

// Example 7: RunInTransaction Helper
func exampleRunInTransaction() {
	log.Println("=== Example 7: RunInTransaction Helper ===")
	ctx := context.Background()
	err := resilience.RunInTransaction(ctx, func(tx *resilience.Transaction) error {
		tx.RegisterCommit(func(ctx context.Context) error {
			log.Println("RunInTransaction commit action executed")
			return nil
		})
		res := &DummyResource{Name: "RunInTransResource"}
		tx.RegisterResource(res)
		return nil
	})
	if err != nil {
		log.Printf("RunInTransaction error: %v", err)
	} else {
		log.Println("RunInTransaction succeeded")
	}
}

// Example 8: Custom Logger with Correlation ID
func exampleCustomLoggerWithCorrelation() {
	log.Println("=== Example 8: Custom Logger with Correlation ID ===")
	// Create a context with a correlation id.
	ctx := context.WithValue(context.Background(), resilience.ContextKeyCorrelationID, "corr-12345")
	tx := resilience.NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed with custom logger and correlation id")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Println("Transaction committed successfully with correlation id")
	}
}

// Example 9: Distributed Coordinator Usage and Dynamic Tracing
func exampleDistributedCoordinator() {
	log.Println("=== Example 9: Distributed Coordinator and Tracing ===")
	opts := resilience.TransactionOptions{
		IsolationLevel:         "distributed",
		Timeout:                5 * time.Second,
		RetryPolicy:            resilience.RetryPolicy{MaxRetries: 2, Delay: 100 * time.Millisecond, ShouldRetry: func(err error) bool { return true }, BackoffStrategy: func(attempt int) time.Duration { return time.Duration(100*(1<<attempt)) * time.Millisecond }},
		Logger:                 &resilience.DefaultLogger{Level: resilience.InfoLevel},
		Metrics:                &resilience.NoopMetricsCollector{},
		DistributedCoordinator: &DummyDistributedCoordinator{},
		CaptureStackTrace:      true,
		Tracer:                 &DummyTracer{},
		LifecycleHooks:         lifecycleHooks,
	}
	tx := resilience.NewTransactionWithOptions(opts)
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Distributed commit action executed")
		return nil
	})
	res := &DummyResource{Name: "DistributedResource"}
	tx.RegisterResource(res)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Distributed transaction commit error: %v", err)
	} else {
		log.Printf("Distributed transaction committed, state: %s", tx.GetState().String())
	}
}

// Example 10: Per-Action Retry Policies
func examplePerActionRetryPolicies() {
	log.Println("=== Example 10: Per-Action Retry Policies ===")
	tx := resilience.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	// Register a commit action with a custom retry policy.
	err := tx.RegisterCommitWithRetryPolicy(func(ctx context.Context) error {
		log.Println("Custom retry commit action executed")
		return nil
	}, customRetryPolicy)
	if err != nil {
		log.Printf("Error registering commit action with custom policy: %v", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Transaction commit error with custom retry policy: %v", err)
	} else {
		log.Printf("Transaction with custom retry policies committed, state: %s", tx.GetState().String())
	}
}

// -----------------------------
// Main: Run All Examples
// -----------------------------

func main() {
	exampleBasicTransaction()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedCommitFailure()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedPrepareFailure()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedRollbackFailure()
	time.Sleep(500 * time.Millisecond)

	exampleNestedTransactions()
	time.Sleep(500 * time.Millisecond)

	exampleAsyncOperations()
	time.Sleep(500 * time.Millisecond)

	exampleRunInTransaction()
	time.Sleep(500 * time.Millisecond)

	exampleCustomLoggerWithCorrelation()
	time.Sleep(500 * time.Millisecond)

	exampleDistributedCoordinator()
	time.Sleep(500 * time.Millisecond)

	examplePerActionRetryPolicies()
}
