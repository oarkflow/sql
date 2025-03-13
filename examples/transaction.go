// File: example_all_use_cases.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/etl/pkg/transactions"
)

// DummyResource implements TransactionalResource and optionally fails commit/rollback.
type DummyResource struct {
	Name         string
	FailCommit   bool
	FailRollback bool
}

func (dr *DummyResource) Commit(_ context.Context) error {
	if dr.FailCommit {
		return fmt.Errorf("resource %s commit failed", dr.Name)
	}
	log.Printf("DummyResource %s committed", dr.Name)
	return nil
}

func (dr *DummyResource) Rollback(_ context.Context) error {
	if dr.FailRollback {
		return fmt.Errorf("resource %s rollback failed", dr.Name)
	}
	log.Printf("DummyResource %s rolled back", dr.Name)
	return nil
}

// SimpleMetricsCollector implements MetricsCollector and logs metric events.
type SimpleMetricsCollector struct{}

func (s *SimpleMetricsCollector) IncCommitCount() {
	log.Println("Metrics: Commit count incremented")
}
func (s *SimpleMetricsCollector) IncRollbackCount() {
	log.Println("Metrics: Rollback count incremented")
}
func (s *SimpleMetricsCollector) RecordCommitDuration(d time.Duration) {
	log.Printf("Metrics: Commit duration recorded: %v", d)
}
func (s *SimpleMetricsCollector) RecordRollbackDuration(d time.Duration) {
	log.Printf("Metrics: Rollback duration recorded: %v", d)
}
func (s *SimpleMetricsCollector) IncErrorCount() {
	log.Println("Metrics: Error count incremented")
}
func (s *SimpleMetricsCollector) IncRetryCount() {
	log.Println("Metrics: Retry count incremented")
}
func (s *SimpleMetricsCollector) RecordActionDuration(action string, d time.Duration) {
	log.Printf("Metrics: Action %s duration: %v", action, d)
}

// exponentialBackoff returns a delay duration for a given retry attempt.
func exponentialBackoff(attempt int) time.Duration {
	return time.Duration(100*(1<<attempt)) * time.Millisecond
}

// Example 1: Basic successful transaction.
func exampleSuccessfulTransaction() {
	log.Println("=== Example 1: Basic Successful Transaction ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed successfully")
		return nil
	})
	if err != nil {
		return
	}
	res := &DummyResource{Name: "Resource1"}
	err = tx.RegisterResource(res)
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Printf("Transaction %d state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 2: Simulated commit failure triggering rollback.
func exampleSimulatedCommitFailure() {
	log.Println("=== Example 2: Simulated Commit Failure ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	tx.SetTestHooks(&transactions.TestHooks{SimulateCommitFailure: true})
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed")
		return nil
	})
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected commit failure: %v", err)
		log.Printf("Transaction state: %s", tx.GetState().String())
	}
}

// Example 3: Simulated prepare failure.
func exampleSimulatedPrepareFailure() {
	log.Println("=== Example 3: Simulated Prepare Failure ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	tx.SetTestHooks(&transactions.TestHooks{SimulatePrepareFailure: true})
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed")
		return nil
	})
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected prepare failure: %v", err)
	}
}

// Example 4: Simulated rollback failure.
func exampleSimulatedRollbackFailure() {
	log.Println("=== Example 4: Simulated Rollback Failure ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	// Register a commit action that fails, triggering a rollback.
	err := tx.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated commit error to trigger rollback")
	})
	if err != nil {
		return
	}
	tx.SetTestHooks(&transactions.TestHooks{SimulateRollbackFailure: true})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected rollback failure: %v", err)
	}
}

// Example 5: Nested transactions and savepoints.
func exampleNestedTransactions() {
	log.Println("=== Example 5: Nested Transactions and Savepoints ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	// Create an unnamed savepoint.
	sp, err := tx.CreateSavepoint(ctx)
	if err != nil {
		log.Printf("Savepoint creation error: %v", err)
		return
	}
	log.Printf("Created savepoint at index %d", sp)
	// Begin a nested transaction.
	nestedTx, err := tx.BeginNested(ctx)
	if err != nil {
		log.Printf("Nested transaction error: %v", err)
		return
	}
	// Register a commit action within the nested transaction.
	err = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Nested commit action executed")
		return nil
	})
	if err != nil {
		return
	}
	// Commit the nested transaction.
	if err := nestedTx.Commit(ctx); err != nil {
		log.Printf("Nested transaction commit error: %v", err)
	} else {
		log.Println("Nested transaction committed")
	}
	// Create a named savepoint.
	if err := tx.CreateNamedSavepoint("sp1"); err != nil {
		log.Printf("Named savepoint creation error: %v", err)
	}
	// Rollback to the named savepoint.
	if err := tx.RollbackToNamedSavepoint(ctx, "sp1"); err != nil {
		log.Printf("Rollback to named savepoint error: %v", err)
	} else {
		log.Println("Rolled back to named savepoint 'sp1'")
	}
	// Commit the main transaction.
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Main transaction commit error: %v", err)
	} else {
		log.Printf("Main transaction committed, state: %s", tx.GetState().String())
	}
}

// Example 6: Asynchronous commit and rollback.
func exampleAsyncOperations() {
	log.Println("=== Example 6: Asynchronous Commit and Rollback ===")
	// Asynchronous commit.
	tx := transactions.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Async commit action executed")
		return nil
	})
	if err != nil {
		return
	}
	res := &DummyResource{Name: "AsyncResource"}
	err = tx.RegisterResource(res)
	if err != nil {
		return
	}
	asyncResultCh := tx.AsyncCommitWithResult(ctx)
	result := <-asyncResultCh
	if result.Err != nil {
		log.Printf("Async commit error: %v", result.Err)
	} else {
		log.Printf("Async commit succeeded, state: %s", result.State.String())
	}
	// Asynchronous rollback example.
	tx2 := transactions.NewTransaction()
	if err := tx2.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	// Register a commit action that fails to trigger async rollback.
	err = tx2.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated commit error for async rollback")
	})
	if err != nil {
		return
	}
	asyncRbCh := tx2.AsyncRollbackWithResult(ctx)
	rbResult := <-asyncRbCh
	if rbResult.Err != nil {
		log.Printf("Async rollback error: %v", rbResult.Err)
	} else {
		log.Printf("Async rollback succeeded, state: %s", rbResult.State.String())
	}
}

// Example 7: RunInTransaction helper usage.
func exampleRunInTransaction() {
	log.Println("=== Example 7: RunInTransaction Helper ===")
	ctx := context.Background()
	err := transactions.RunInTransaction(ctx, func(tx *transactions.Transaction) error {
		err := tx.RegisterCommit(func(ctx context.Context) error {
			log.Println("RunInTransaction commit action executed")
			return nil
		})
		if err != nil {
			return err
		}
		res := &DummyResource{Name: "RunInTransResource"}
		err = tx.RegisterResource(res)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("RunInTransaction error: %v", err)
	} else {
		log.Println("RunInTransaction succeeded")
	}
}

// Example 8: Custom Logger with Correlation ID usage.
func exampleCustomLoggerWithCorrelation() {
	log.Println("=== Example 8: Custom Logger with Correlation ID ===")
	// Create a context that carries a correlation id.
	ctx := context.WithValue(context.Background(), transactions.ContextKeyCorrelationID, "corr-12345")
	tx := transactions.NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed with custom logger")
		return nil
	})
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Println("Transaction committed successfully with correlation id")
	}
}

// Example 9: Distributed Coordinator usage.
func exampleDistributedCoordinator() {
	log.Println("=== Example 9: Distributed Coordinator Usage ===")
	opts := transactions.TransactionOptions{
		IsolationLevel:         "distributed",
		Timeout:                5 * time.Second,
		RetryPolicy:            transactions.RetryPolicy{MaxRetries: 2, Delay: 100 * time.Millisecond, ShouldRetry: func(err error) bool { return true }, BackoffStrategy: exponentialBackoff},
		Logger:                 &transactions.DefaultLogger{Level: transactions.InfoLevel},
		Metrics:                &SimpleMetricsCollector{},
		DistributedCoordinator: &transactions.TwoPhaseCoordinator{},
		CaptureStackTrace:      true,
	}
	tx := transactions.NewTransactionWithOptions(opts)
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Distributed commit action executed")
		return nil
	})
	if err != nil {
		return
	}
	res := &DummyResource{Name: "DistributedResource"}
	err = tx.RegisterResource(res)
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Distributed transaction commit error: %v", err)
	} else {
		log.Printf("Distributed transaction committed, state: %s", tx.GetState().String())
	}
}

// Example 10: Lifecycle Hooks usage.
func exampleLifecycleHooks() {
	log.Println("=== Example 10: Lifecycle Hooks Usage ===")
	hooks := &transactions.LifecycleHooks{
		OnBegin: func(txID int64, ctx context.Context) {
			log.Printf("Lifecycle Hook: Transaction %d has begun", txID)
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
	opts := transactions.TransactionOptions{
		IsolationLevel:    "hooks",
		Timeout:           5 * time.Second,
		Logger:            &transactions.DefaultLogger{Level: transactions.InfoLevel},
		Metrics:           &SimpleMetricsCollector{},
		LifecycleHooks:    hooks,
		CaptureStackTrace: true,
	}
	tx := transactions.NewTransactionWithOptions(opts)
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	err := tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Lifecycle commit action executed")
		return nil
	})
	if err != nil {
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	}
}

// Main function to run all examples.
func main() {
	exampleSuccessfulTransaction()
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
	exampleLifecycleHooks()
}
