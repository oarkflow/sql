// File: example_transactions.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/etl/pkg/transactions"
)

// DummyResource implements the TransactionalResource interface for testing.
type DummyResource struct {
	name         string
	failCommit   bool
	failRollback bool
}

func (r *DummyResource) Commit(ctx context.Context) error {
	if r.failCommit {
		return fmt.Errorf("dummy resource %s commit failed", r.name)
	}
	log.Printf("DummyResource %s committed", r.name)
	return nil
}

func (r *DummyResource) Rollback(ctx context.Context) error {
	if r.failRollback {
		return fmt.Errorf("dummy resource %s rollback failed", r.name)
	}
	log.Printf("DummyResource %s rolled back", r.name)
	return nil
}

// Example 1: Successful Transaction Commit.
func exampleSuccessfulCommit() {
	log.Println("=== Example 1: Successful Transaction Commit ===")
	tx := transactions.NewTransaction()
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
	// Register a rollback action (won't be executed if commit succeeds).
	_ = tx.RegisterRollback(func(ctx context.Context) error {
		log.Println("Rollback action executed.")
		return nil
	})
	// Register a dummy resource.
	res := &DummyResource{name: "Resource1"}
	_ = tx.RegisterResource(res)

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Printf("Transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 2: Transaction with a Failed Commit Action (triggers rollback).
func exampleFailedCommitAction() {
	log.Println("=== Example 2: Commit Action Failure Triggers Rollback ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Register a commit action that fails.
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action (that will fail) executed.")
		return errors.New("simulated commit action failure")
	})
	// Register a rollback action to observe the rollback.
	_ = tx.RegisterRollback(func(ctx context.Context) error {
		log.Println("Rollback action executed due to commit failure.")
		return nil
	})
	// Register a dummy resource.
	res := &DummyResource{name: "Resource2"}
	_ = tx.RegisterResource(res)

	err := tx.Commit(ctx)
	if err != nil {
		log.Printf("Expected commit error: %v", err)
		log.Printf("Transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 3: Simulated Commit Failure Using Test Hooks.
func exampleSimulatedCommitFailure() {
	log.Println("=== Example 3: Simulated Commit Failure via TestHooks ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Set test hook to simulate commit failure.
	tx.SetTestHooks(&transactions.TestHooks{SimulateCommitFailure: true})

	// Register a commit action (won't matter because of the simulation).
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed.")
		return nil
	})
	err := tx.Commit(ctx)
	if err != nil {
		log.Printf("Expected simulated commit failure: %v", err)
		log.Printf("Transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 4: Asynchronous Commit.
func exampleAsyncCommit() {
	log.Println("=== Example 4: Asynchronous Commit ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Async commit action executed.")
		return nil
	})
	// Register a dummy resource.
	res := &DummyResource{name: "Resource3"}
	_ = tx.RegisterResource(res)

	resultCh := tx.AsyncCommit(ctx)
	if err := <-resultCh; err != nil {
		log.Printf("Async commit error: %v", err)
	} else {
		log.Printf("Transaction %d async commit succeeded with state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 5: Asynchronous Rollback.
func exampleAsyncRollback() {
	log.Println("=== Example 5: Asynchronous Rollback ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Force a rollback by calling Abort.
	go func() {
		time.Sleep(100 * time.Millisecond) // simulate some work
		_ = tx.Abort(ctx)
	}()
	resultCh := tx.AsyncRollback(ctx)
	if err := <-resultCh; err != nil {
		log.Printf("Async rollback error: %v", err)
	} else {
		log.Printf("Transaction %d async rollback succeeded with state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 6: Using Savepoints (Named and Unnamed).
func exampleSavepoints() {
	log.Println("=== Example 6: Savepoints Usage ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Register a rollback action that should always be executed on rollback.
	_ = tx.RegisterRollback(func(ctx context.Context) error {
		log.Println("Rollback action 1 executed.")
		return nil
	})

	// Create an unnamed savepoint.
	sp, err := tx.CreateSavepoint()
	if err != nil {
		log.Printf("Error creating savepoint: %v", err)
		return
	}
	log.Printf("Unnamed savepoint created at index %d", sp)

	// Register additional rollback actions after savepoint.
	_ = tx.RegisterRollback(func(ctx context.Context) error {
		log.Println("Rollback action 2 executed (post-savepoint).")
		return nil
	})

	// Create a named savepoint.
	if err := tx.CreateNamedSavepoint("named_sp"); err != nil {
		log.Printf("Error creating named savepoint: %v", err)
		return
	}

	// Now, simulate an error by rolling back to the named savepoint.
	if err := tx.RollbackToNamedSavepoint(ctx, "named_sp"); err != nil {
		log.Printf("Error during rollback to named savepoint: %v", err)
		return
	}
	log.Println("Rolled back to named savepoint successfully.")

	// Finally, commit the transaction.
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error after savepoint rollback: %v", err)
	} else {
		log.Printf("Transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 7: Nested Transaction with Commit.
func exampleNestedTransactionCommit() {
	log.Println("=== Example 7: Nested Transaction Commit ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Begin a nested transaction.
	nestedTx, err := tx.BeginNested(ctx)
	if err != nil {
		log.Printf("Error beginning nested transaction: %v", err)
		return
	}

	// Within nested, register a commit action.
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Nested transaction commit action executed.")
		return nil
	})

	// Commit the nested transaction (which releases the savepoint).
	if err := nestedTx.Commit(ctx); err != nil {
		log.Printf("Nested transaction commit error: %v", err)
		return
	}
	log.Println("Nested transaction committed.")

	// Commit the parent transaction.
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Parent commit error: %v", err)
	} else {
		log.Printf("Parent transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 8: Nested Transaction with Rollback.
func exampleNestedTransactionRollback() {
	log.Println("=== Example 8: Nested Transaction Rollback ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Begin a nested transaction.
	nestedTx, err := tx.BeginNested(ctx)
	if err != nil {
		log.Printf("Error beginning nested transaction: %v", err)
		return
	}

	// Within nested, register a commit action (that we decide later not to keep).
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Nested transaction commit action executed.")
		return nil
	})

	// Rollback the nested transaction.
	if err := nestedTx.Rollback(ctx); err != nil {
		log.Printf("Nested transaction rollback error: %v", err)
		return
	}
	log.Println("Nested transaction rolled back to savepoint.")

	// Commit the parent transaction.
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Parent commit error: %v", err)
	} else {
		log.Printf("Parent transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

// Example 9: Using RunInTransaction Helper.
func exampleRunInTransaction() {
	log.Println("=== Example 9: RunInTransaction Helper (Successful Case) ===")
	ctx := context.Background()
	err := transactions.RunInTransaction(ctx, func(tx *transactions.Transaction) error {
		_ = tx.RegisterCommit(func(ctx context.Context) error {
			log.Println("RunInTransaction commit action executed.")
			return nil
		})
		// Register a dummy resource.
		res := &DummyResource{name: "Resource_Run"}
		_ = tx.RegisterResource(res)
		return nil
	})
	if err != nil {
		log.Printf("RunInTransaction error: %v", err)
	} else {
		log.Println("RunInTransaction succeeded.")
	}

	log.Println("=== Example 9: RunInTransaction Helper (Failing Case) ===")
	err = transactions.RunInTransaction(ctx, func(tx *transactions.Transaction) error {
		// This function returns an error to force rollback.
		return errors.New("simulated error in RunInTransaction")
	})
	if err != nil {
		log.Printf("Expected RunInTransaction error: %v", err)
	}
}

// Example 10: Simulated Rollback Failure via TestHooks.
func exampleSimulatedRollbackFailure() {
	log.Println("=== Example 10: Simulated Rollback Failure via TestHooks ===")
	tx := transactions.NewTransaction()
	ctx := context.Background()
	_ = tx.Begin(ctx)

	// Register a commit action that fails so that rollback will be triggered.
	_ = tx.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated commit failure to trigger rollback")
	})
	// Set test hook to simulate rollback failure.
	tx.SetTestHooks(&transactions.TestHooks{SimulateRollbackFailure: true})
	err := tx.Commit(ctx)
	if err != nil {
		log.Printf("Expected error (simulated rollback failure): %v", err)
		log.Printf("Transaction %d final state: %s", tx.GetID(), tx.GetState().String())
	}
}

func main() {
	// Run all the example cases.
	exampleSuccessfulCommit()
	time.Sleep(500 * time.Millisecond)

	exampleFailedCommitAction()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedCommitFailure()
	time.Sleep(500 * time.Millisecond)

	exampleAsyncCommit()
	time.Sleep(500 * time.Millisecond)

	exampleAsyncRollback()
	time.Sleep(500 * time.Millisecond)

	exampleSavepoints()
	time.Sleep(500 * time.Millisecond)

	exampleNestedTransactionCommit()
	time.Sleep(500 * time.Millisecond)

	exampleNestedTransactionRollback()
	time.Sleep(500 * time.Millisecond)

	exampleRunInTransaction()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedRollbackFailure()
}
