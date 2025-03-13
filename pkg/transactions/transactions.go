package transactions

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type TxState int

const (
	StateInitialized TxState = iota
	StateInProgress
	StateCommitted
	StateRolledBack
	StateFailed
)

func (s TxState) String() string {
	switch s {
	case StateInitialized:
		return "Initialized"
	case StateInProgress:
		return "InProgress"
	case StateCommitted:
		return "Committed"
	case StateRolledBack:
		return "RolledBack"
	case StateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type TransactionalResource interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

var txCounter int64

// Transaction represents a unit of work with commit/rollback and cleanup capabilities.
type Transaction struct {
	mu              sync.Mutex
	id              int64
	state           TxState
	rollbackActions []func(ctx context.Context) error
	commitActions   []func(ctx context.Context) error
	cleanupActions  []func(ctx context.Context) error
	resources       []TransactionalResource
	cleanupCalled   bool
}

// NewTransaction creates a new Transaction with a unique id.
func NewTransaction() *Transaction {
	t := &Transaction{
		id:              atomic.AddInt64(&txCounter, 1),
		state:           StateInitialized,
		rollbackActions: make([]func(ctx context.Context) error, 0),
		commitActions:   make([]func(ctx context.Context) error, 0),
		cleanupActions:  make([]func(ctx context.Context) error, 0),
		resources:       make([]TransactionalResource, 0),
	}
	return t
}

// Begin starts the transaction.
func (t *Transaction) Begin(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	batch := ctx.Value("batch")
	if t.state != StateInitialized {
		return fmt.Errorf("cannot begin transaction %d in state %s", t.id, t.state)
	}
	t.state = StateInProgress
	t.rollbackActions = make([]func(ctx context.Context) error, 0)
	t.commitActions = make([]func(ctx context.Context) error, 0)
	t.cleanupActions = make([]func(ctx context.Context) error, 0)
	t.resources = make([]TransactionalResource, 0)
	log.Printf("Transaction %d begun", t.id)
	if batch != nil {
		log.Printf("Transaction %d begun for batch %v", t.id, batch)
	}
	return nil
}

// RegisterRollback registers a rollback callback. The callback must not be nil.
func (t *Transaction) RegisterRollback(fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("rollback action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register rollback action in transaction %d state %s", t.id, t.state)
	}
	t.rollbackActions = append(t.rollbackActions, fn)
	log.Printf("Transaction %d: Registered rollback action", t.id)
	return nil
}

// RegisterCommit registers a commit callback. The callback must not be nil.
func (t *Transaction) RegisterCommit(fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("commit action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register commit action in transaction %d state %s", t.id, t.state)
	}
	t.commitActions = append(t.commitActions, fn)
	log.Printf("Transaction %d: Registered commit action", t.id)
	return nil
}

// RegisterCleanup registers a cleanup callback that will always be run when the transaction is closed.
func (t *Transaction) RegisterCleanup(fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("cleanup action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register cleanup action in transaction %d state %s", t.id, t.state)
	}
	t.cleanupActions = append(t.cleanupActions, fn)
	log.Printf("Transaction %d: Registered cleanup action", t.id)
	return nil
}

// RegisterResource registers an external transactional resource.
func (t *Transaction) RegisterResource(res TransactionalResource) error {
	if res == nil {
		return errors.New("transactional resource cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register resource in transaction %d state %s", t.id, t.state)
	}
	t.resources = append(t.resources, res)
	log.Printf("Transaction %d: Registered transactional resource", t.id)
	return nil
}

// Commit executes all commit callbacks and commits all registered resources.
// If any commit action fails, the transaction is marked failed and rollback is attempted.
func (t *Transaction) Commit(ctx context.Context) error {
	// Check state and context error
	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot commit transaction %d in state %s", t.id, t.state)
	}
	batch := ctx.Value("batch")
	if err := ctx.Err(); err != nil {
		t.mu.Unlock()
		return err
	}
	t.mu.Unlock()

	// Execute commit actions in order
	for i, action := range t.commitActions {
		if err := safeAction(ctx, action, fmt.Sprintf("commit action %d in transaction %d", i, t.id)); err != nil {
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			log.Printf("Transaction %d: Commit action failed: %v", t.id, err)
			if rbErr := t.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("transaction %d commit failed: %v; additionally rollback failed: %v", t.id, err, rbErr)
			}
			return fmt.Errorf("transaction %d commit action failed: %v", t.id, err)
		}
	}

	// Commit external resources
	for i, res := range t.resources {
		if err := safeResourceCommit(ctx, res, i, t.id); err != nil {
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			log.Printf("Transaction %d: Resource commit failed: %v", t.id, err)
			if rbErr := t.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("transaction %d resource commit failed: %v; rollback also failed: %v", t.id, err, rbErr)
			}
			return fmt.Errorf("transaction %d resource commit failed: %v", t.id, err)
		}
	}

	t.mu.Lock()
	t.state = StateCommitted
	t.rollbackActions = nil
	t.mu.Unlock()
	if batch != nil {
		log.Printf("Transaction %d committed successfully for batch %v", t.id, batch)
	} else {
		log.Printf("Transaction %d committed successfully", t.id)
	}
	// Run cleanup actions if any
	if err := t.runCleanup(ctx); err != nil {
		return fmt.Errorf("transaction %d commit cleanup error: %v", t.id, err)
	}
	return nil
}

// Rollback undoes all rollback callbacks and registered resources.
// It aggregates errors from rollback actions if any.
func (t *Transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	if t.state != StateInProgress && t.state != StateFailed {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback transaction %d in state %s", t.id, t.state)
	}

	if err := ctx.Err(); err != nil {
		t.mu.Unlock()
		return err
	}
	actions := t.rollbackActions
	resources := t.resources
	t.rollbackActions = nil
	t.resources = nil
	t.state = StateRolledBack
	t.mu.Unlock()

	var errs []error
	log.Printf("Transaction %d: Rolling back", t.id)

	// Rollback registered actions in reverse order.
	for i := len(actions) - 1; i >= 0; i-- {
		if err := safeAction(ctx, actions[i], fmt.Sprintf("rollback action %d in transaction %d", i, t.id)); err != nil {
			errs = append(errs, err)
			log.Printf("Transaction %d: Error during rollback action %d: %v", t.id, i, err)
		}
	}

	// Rollback external resources in reverse order.
	for i := len(resources) - 1; i >= 0; i-- {
		if err := safeResourceRollback(ctx, resources[i], i, t.id); err != nil {
			errs = append(errs, err)
			log.Printf("Transaction %d: Error during resource rollback %d: %v", t.id, i, err)
		}
	}

	// Run cleanup actions regardless of rollback errors.
	if cleanupErr := t.runCleanup(ctx); cleanupErr != nil {
		errs = append(errs, cleanupErr)
	}

	if len(errs) > 0 {
		return fmt.Errorf("transaction %d rollback encountered errors: %v", t.id, errors.Join(errs...))
	}
	return nil
}

// Close finalizes the transaction. If the transaction was neither committed nor rolled back,
// it will automatically perform a rollback.
func (t *Transaction) Close() error {
	t.mu.Lock()
	currentState := t.state
	t.mu.Unlock()
	if currentState == StateCommitted || currentState == StateRolledBack {
		return nil
	}
	log.Printf("Transaction %d: Closing - performing rollback", t.id)
	return t.Rollback(context.Background())
}

// CreateSavepoint returns the current index in rollback actions as a savepoint marker.
func (t *Transaction) CreateSavepoint() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return 0, fmt.Errorf("cannot create savepoint in transaction %d state %s", t.id, t.state)
	}
	sp := len(t.rollbackActions)
	log.Printf("Transaction %d: Created savepoint at index %d", t.id, sp)
	return sp, nil
}

// RollbackToSavepoint rolls back all actions registered after the specified savepoint.
func (t *Transaction) RollbackToSavepoint(ctx context.Context, sp int) error {
	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback to savepoint in transaction %d state %s", t.id, t.state)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		t.mu.Unlock()
		return fmt.Errorf("invalid savepoint index %d for transaction %d", sp, t.id)
	}
	actionsToRollback := t.rollbackActions[sp:]
	t.rollbackActions = t.rollbackActions[:sp]
	t.mu.Unlock()

	var errs []error
	log.Printf("Transaction %d: Rolling back to savepoint at index %d", t.id, sp)
	for i := len(actionsToRollback) - 1; i >= 0; i-- {
		if err := safeAction(ctx, actionsToRollback[i], fmt.Sprintf("rollback savepoint action %d in transaction %d", i, t.id)); err != nil {
			errs = append(errs, err)
			log.Printf("Transaction %d: Error during rollback action at savepoint: %v", t.id, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("transaction %d rollback to savepoint encountered errors: %v", t.id, errors.Join(errs...))
	}
	return nil
}

// ReleaseSavepoint discards all rollback actions after the given savepoint.
func (t *Transaction) ReleaseSavepoint(sp int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot release savepoint in transaction %d state %s", t.id, t.state)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		return fmt.Errorf("invalid savepoint index %d for transaction %d", sp, t.id)
	}
	t.rollbackActions = t.rollbackActions[:sp]
	log.Printf("Transaction %d: Released savepoint at index %d", t.id, sp)
	return nil
}

// GetState returns the current state of the transaction.
func (t *Transaction) GetState() TxState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// GetID returns the unique identifier of the transaction.
func (t *Transaction) GetID() int64 {
	return t.id
}

// RunInTransaction wraps a function call within a transaction, automatically rolling back if an error occurs.
func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		return err
	}
	defer func() {
		// Ensure cleanup is performed.
		_ = tx.Close()
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("transaction %d rollback error: %v (original error: %v)", tx.id, rbErr, err)
		}
		return err
	}
	return tx.Commit(ctx)
}

// runCleanup executes all registered cleanup actions if they have not been run already.
func (t *Transaction) runCleanup(ctx context.Context) error {
	t.mu.Lock()
	if t.cleanupCalled {
		t.mu.Unlock()
		return nil
	}
	actions := t.cleanupActions
	// Mark cleanup as done to avoid duplicate calls.
	t.cleanupCalled = true
	t.mu.Unlock()

	var errs []error
	for i, action := range actions {
		if err := safeAction(ctx, action, fmt.Sprintf("cleanup action %d in transaction %d", i, t.id)); err != nil {
			errs = append(errs, err)
			log.Printf("Transaction %d: Error during cleanup action %d: %v", t.id, i, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// safeAction executes an action and recovers from any panic.
func safeAction(ctx context.Context, action func(ctx context.Context) error, description string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s: %v", description, r)
		}
	}()
	return action(ctx)
}

// safeResourceCommit wraps the Commit call of a TransactionalResource with panic recovery.
func safeResourceCommit(ctx context.Context, res TransactionalResource, index int, txID int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in resource commit %d for transaction %d: %v", index, txID, r)
		}
	}()
	return res.Commit(ctx)
}

// safeResourceRollback wraps the Rollback call of a TransactionalResource with panic recovery.
func safeResourceRollback(ctx context.Context, res TransactionalResource, index int, txID int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in resource rollback %d for transaction %d: %v", index, txID, r)
		}
	}()
	return res.Rollback(ctx)
}
