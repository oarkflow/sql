package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/etl/pkg/transactions"
)

// DummyDistributedCoordinator implements the DistributedCoordinator interface.
type DummyDistributedCoordinator struct{}

func (ddc *DummyDistributedCoordinator) BeginDistributed(tx *transactions.Transaction) error {
	log.Printf("DistributedCoordinator: Begin transaction %d", tx.GetID())
	return nil
}

func (ddc *DummyDistributedCoordinator) CommitDistributed(tx *transactions.Transaction) error {
	log.Printf("DistributedCoordinator: Commit transaction %d", tx.GetID())
	return nil
}

func (ddc *DummyDistributedCoordinator) RollbackDistributed(tx *transactions.Transaction) error {
	log.Printf("DistributedCoordinator: Rollback transaction %d", tx.GetID())
	return nil
}

// exponentialBackoff returns an exponential backoff duration with each retry.
func exponentialBackoff(attempt int) time.Duration {
	base := 100 * time.Millisecond
	return base * time.Duration(1<<attempt)
}

// Lifecycle hook functions.
func onBegin(txID int64, _ context.Context) {
	log.Printf("Lifecycle Hook: Transaction %d began", txID)
}
func onBeforeCommit(txID int64, _ context.Context) {
	log.Printf("Lifecycle Hook: Before commit transaction %d", txID)
}
func onAfterCommit(txID int64, _ context.Context) {
	log.Printf("Lifecycle Hook: After commit transaction %d", txID)
}
func onBeforeRollback(txID int64, _ context.Context) {
	log.Printf("Lifecycle Hook: Before rollback transaction %d", txID)
}
func onAfterRollback(txID int64, _ context.Context) {
	log.Printf("Lifecycle Hook: After rollback transaction %d", txID)
}
func onClose(txID int64, _ context.Context) {
	log.Printf("Lifecycle Hook: Transaction %d closed", txID)
}

// DummyResource implements the TransactionalResource interface.
type DummyResource struct {
	name         string
	failCommit   bool
	failRollback bool
}

func (dr *DummyResource) Commit(_ context.Context) error {
	if dr.failCommit {
		return fmt.Errorf("dummy resource %s commit failed", dr.name)
	}
	log.Printf("DummyResource %s committed", dr.name)
	return nil
}

func (dr *DummyResource) Rollback(_ context.Context) error {
	if dr.failRollback {
		return fmt.Errorf("dummy resource %s rollback failed", dr.name)
	}
	log.Printf("DummyResource %s rolled back", dr.name)
	return nil
}

// Example 1: Successful Transaction with All Options.
func exampleSuccessfulTransaction() {
	log.Println("=== Example 1: Successful Transaction with All Options ===")
	opts := transactions.TransactionOptions{
		IsolationLevel:   "Serializable",
		Timeout:          5 * time.Second,
		PrepareTimeout:   2 * time.Second,
		CommitTimeout:    2 * time.Second,
		RollbackTimeout:  2 * time.Second,
		ParallelCommit:   true,
		ParallelRollback: true,
		RetryPolicy: transactions.RetryPolicy{
			MaxRetries:      3,
			Delay:           100 * time.Millisecond,
			ShouldRetry:     func(err error) bool { return true },
			BackoffStrategy: exponentialBackoff,
		},
		LifecycleHooks: &transactions.LifecycleHooks{
			OnBegin:          onBegin,
			OnBeforeCommit:   onBeforeCommit,
			OnAfterCommit:    onAfterCommit,
			OnBeforeRollback: onBeforeRollback,
			OnAfterRollback:  onAfterRollback,
			OnClose:          onClose,
		},
		DistributedCoordinator: &DummyDistributedCoordinator{},
		Logger:                 &transactions.DefaultLogger{}, // using default logger
		Metrics:                &transactions.NoopMetricsCollector{},
	}
	tx := transactions.NewTransactionWithOptions(opts)
	ctx := context.Background()

	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}

	// Register a commit action.
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed successfully.")
		return nil
	})
	// Register a rollback action (this one will not be executed).
	_ = tx.RegisterRollback(func(ctx context.Context) error {
		log.Println("Rollback action executed.")
		return nil
	})
	// Register a dummy resource.
	resource := &DummyResource{name: "Resource1"}
	_ = tx.RegisterResource(resource)

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Printf("Transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 2: Nested Transactions and Savepoints.
func exampleNestedTransactions() {
	log.Println("=== Example 2: Nested Transactions and Savepoints ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Create an unnamed savepoint.
	sp, err := tx.CreateSavepoint(ctx)
	if err != nil {
		log.Printf("Error creating savepoint: %v", err)
		return
	}
	log.Printf("Created savepoint at index %d", sp)

	// Register a rollback action for the main transaction.
	_ = tx.RegisterRollback(func(ctx context.Context) error {
		log.Println("Main transaction rollback action executed.")
		return nil
	})

	// Begin a nested transaction.
	nestedTx, err := tx.BeginNested(ctx)
	if err != nil {
		log.Printf("Error beginning nested transaction: %v", err)
		return
	}
	log.Println("Nested transaction started.")

	// Register a commit action in the nested transaction.
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Nested transaction commit action executed.")
		return nil
	})

	// Begin a second-level nested transaction.
	nestedTx2, err := nestedTx.BeginNested(ctx)
	if err != nil {
		log.Printf("Error beginning second-level nested transaction: %v", err)
		return
	}
	log.Println("Second-level nested transaction started.")

	// Commit second-level nested transaction.
	if err := nestedTx2.Commit(ctx); err != nil {
		log.Printf("Second-level nested transaction commit error: %v", err)
	} else {
		log.Println("Second-level nested transaction committed.")
	}

	// Rollback first-level nested transaction.
	if err := nestedTx.Rollback(ctx); err != nil {
		log.Printf("Nested transaction rollback error: %v", err)
	} else {
		log.Println("Nested transaction rolled back.")
	}

	// Create a named savepoint.
	if err := tx.CreateNamedSavepoint("my_savepoint"); err != nil {
		log.Printf("Error creating named savepoint: %v", err)
	}
	log.Println("Named savepoint 'my_savepoint' created.")

	// Rollback to the named savepoint.
	if err := tx.RollbackToNamedSavepoint(ctx, "my_savepoint"); err != nil {
		log.Printf("Error rolling back to named savepoint: %v", err)
	} else {
		log.Println("Rolled back to named savepoint 'my_savepoint'.")
	}

	// Commit the main transaction.
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Main transaction commit error: %v", err)
	} else {
		log.Printf("Main transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 3: Asynchronous Operations.
func exampleAsyncOperations() {
	log.Println("=== Example 3: Asynchronous Commit and Rollback ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Register a commit action.
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Async commit action executed.")
		return nil
	})
	resource := &DummyResource{name: "ResourceAsync"}
	_ = tx.RegisterResource(resource)

	// Asynchronous commit with structured result.
	resultCh := tx.AsyncCommitWithResult(ctx)
	result := <-resultCh
	if result.Err != nil {
		log.Printf("Async commit error: %v", result.Err)
	} else {
		log.Printf("Async commit succeeded, transaction %d state: %s", result.TxID, result.State.String())
	}

	// Demonstrate asynchronous rollback.
	tx2 := transactions.NewTransaction()
	_ = tx2.Begin(ctx)
	// Register a commit action that forces an error.
	_ = tx2.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated commit failure")
	})
	resultCh2 := tx2.AsyncRollbackWithResult(ctx)
	result2 := <-resultCh2
	if result2.Err != nil {
		log.Printf("Async rollback error: %v", result2.Err)
	} else {
		log.Printf("Async rollback succeeded, transaction %d state: %s", result2.TxID, result2.State.String())
	}
}

// Example 4: Simulated Failures via Test Hooks.
func exampleSimulatedFailures() {
	log.Println("=== Example 4: Simulated Failures with Test Hooks ===")
	ctx := context.Background()

	// Simulate prepare failure.
	txPrepareFail := transactions.NewTransaction()
	_ = txPrepareFail.Begin(ctx)
	txPrepareFail.SetTestHooks(&transactions.TestHooks{SimulatePrepareFailure: true})
	_ = txPrepareFail.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed in prepare failure test.")
		return nil
	})
	if err := txPrepareFail.Commit(ctx); err != nil {
		log.Printf("Expected prepare failure: %v", err)
	}

	// Simulate commit failure.
	txCommitFail := transactions.NewTransaction()
	_ = txCommitFail.Begin(ctx)
	txCommitFail.SetTestHooks(&transactions.TestHooks{SimulateCommitFailure: true})
	_ = txCommitFail.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed in commit failure test.")
		return nil
	})
	if err := txCommitFail.Commit(ctx); err != nil {
		log.Printf("Expected commit failure: %v", err)
	}

	// Simulate rollback failure.
	txRollbackFail := transactions.NewTransaction()
	_ = txRollbackFail.Begin(ctx)
	txRollbackFail.SetTestHooks(&transactions.TestHooks{SimulateRollbackFailure: true})
	_ = txRollbackFail.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated error to trigger rollback")
	})
	if err := txRollbackFail.Commit(ctx); err != nil {
		log.Printf("Expected rollback failure: %v", err)
	}
}

// Example 5: Custom Logger with Structured Logging.
func exampleCustomLogger() {
	log.Println("=== Example 5: Custom Logger with Structured Logging ===")
	// Use the default logger and add structured fields.
	baseLogger := &transactions.DefaultLogger{}
	customLogger := baseLogger.WithFields(map[string]interface{}{"app": "ExampleApp", "module": "Transaction"})

	tx := transactions.NewTransaction()
	// Set the custom logger on the transaction.
	tx.SetLogger(customLogger)
	ctx := context.Background()
	_ = tx.Begin(ctx)

	_ = tx.RegisterCommit(func(ctx context.Context) error {
		customLogger.Info("Commit action executed with custom logger.")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		customLogger.Error("Transaction commit error: %v", err)
	} else {
		customLogger.Info("Transaction committed successfully.")
	}
}

func main() {
	exampleSuccessfulTransaction()
	time.Sleep(500 * time.Millisecond)
	exampleNestedTransactions()
	time.Sleep(500 * time.Millisecond)
	exampleAsyncOperations()
	time.Sleep(500 * time.Millisecond)
	exampleSimulatedFailures()
	time.Sleep(500 * time.Millisecond)
	exampleCustomLogger()
}
