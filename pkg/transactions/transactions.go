package transactions

import (
	"context"
	"fmt"
	"log"
	"sync"
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

type Transaction struct {
	mu              sync.Mutex
	state           TxState
	rollbackActions []func(ctx context.Context) error
	commitActions   []func(ctx context.Context) error
	resources       []TransactionalResource
}

func NewTransaction() *Transaction {
	return &Transaction{
		state:           StateInitialized,
		rollbackActions: make([]func(ctx context.Context) error, 0),
		commitActions:   make([]func(ctx context.Context) error, 0),
		resources:       make([]TransactionalResource, 0),
	}
}

func (t *Transaction) Begin(_ context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInitialized {
		return fmt.Errorf("cannot begin transaction in state %s", t.state)
	}
	t.state = StateInProgress
	t.rollbackActions = make([]func(ctx context.Context) error, 0)
	t.commitActions = make([]func(ctx context.Context) error, 0)
	t.resources = make([]TransactionalResource, 0)
	log.Println("Transaction begun")
	return nil
}

func (t *Transaction) RegisterRollback(fn func(ctx context.Context) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register rollback action in state %s", t.state)
	}
	t.rollbackActions = append(t.rollbackActions, fn)
	log.Println("Registered rollback action")
	return nil
}

func (t *Transaction) RegisterCommit(fn func(ctx context.Context) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register commit action in state %s", t.state)
	}
	t.commitActions = append(t.commitActions, fn)
	log.Println("Registered commit action")
	return nil
}

func (t *Transaction) RegisterResource(res TransactionalResource) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register resource in state %s", t.state)
	}
	t.resources = append(t.resources, res)
	log.Println("Registered transactional resource")
	return nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot commit transaction in state %s", t.state)
	}

	if err := ctx.Err(); err != nil {
		t.mu.Unlock()
		return err
	}
	t.mu.Unlock()

	for i, action := range t.commitActions {
		if err := safeAction(ctx, action, fmt.Sprintf("commit action %d", i)); err != nil {
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			log.Printf("Commit action failed: %v", err)

			rbErr := t.Rollback(ctx)
			if rbErr != nil {
				return fmt.Errorf("commit failed: %v; additionally rollback failed: %v", err, rbErr)
			}
			return fmt.Errorf("commit action failed: %v", err)
		}
	}

	for i, res := range t.resources {
		if err := safeResourceCommit(ctx, res, i); err != nil {
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			log.Printf("Resource commit failed: %v", err)
			rbErr := t.Rollback(ctx)
			if rbErr != nil {
				return fmt.Errorf("resource commit failed: %v; rollback also failed: %v", err, rbErr)
			}
			return fmt.Errorf("resource commit failed: %v", err)
		}
	}

	t.mu.Lock()
	t.state = StateCommitted

	t.rollbackActions = nil
	t.mu.Unlock()
	log.Println("Transaction committed successfully")
	return nil
}

func (t *Transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	if t.state != StateInProgress && t.state != StateFailed {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback transaction in state %s", t.state)
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

	log.Println("Rolling back transaction")

	for i := len(actions) - 1; i >= 0; i-- {
		if err := safeAction(ctx, actions[i], fmt.Sprintf("rollback action %d", i)); err != nil {
			errs = append(errs, err)
			log.Printf("Error during rollback action %d: %v", i, err)
		}
	}

	for i := len(resources) - 1; i >= 0; i-- {
		if err := safeResourceRollback(ctx, resources[i], i); err != nil {
			errs = append(errs, err)
			log.Printf("Error during resource rollback %d: %v", i, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("rollback encountered errors: %v", errs)
	}
	return nil
}

func (t *Transaction) Close() error {
	t.mu.Lock()
	currentState := t.state
	t.mu.Unlock()
	if currentState == StateCommitted || currentState == StateRolledBack {
		return nil
	}
	log.Println("Closing transaction: performing rollback")
	return t.Rollback(context.Background())
}

func (t *Transaction) CreateSavepoint() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return 0, fmt.Errorf("cannot create savepoint in state %s", t.state)
	}
	sp := len(t.rollbackActions)
	log.Printf("Created savepoint at index %d", sp)
	return sp, nil
}

func (t *Transaction) RollbackToSavepoint(ctx context.Context, sp int) error {
	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback to savepoint in state %s", t.state)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		t.mu.Unlock()
		return fmt.Errorf("invalid savepoint index: %d", sp)
	}
	actionsToRollback := t.rollbackActions[sp:]
	t.rollbackActions = t.rollbackActions[:sp]
	t.mu.Unlock()

	var errs []error
	log.Printf("Rolling back to savepoint at index %d", sp)
	for i := len(actionsToRollback) - 1; i >= 0; i-- {
		if err := safeAction(ctx, actionsToRollback[i], fmt.Sprintf("rollback savepoint action %d", i)); err != nil {
			errs = append(errs, err)
			log.Printf("Error during rollback action at savepoint: %v", err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("rollback to savepoint encountered errors: %v", errs)
	}
	return nil
}

func (t *Transaction) ReleaseSavepoint(sp int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot release savepoint in state %s", t.state)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		return fmt.Errorf("invalid savepoint index: %d", sp)
	}

	t.rollbackActions = t.rollbackActions[:sp]
	log.Printf("Released savepoint at index %d", sp)
	return nil
}

func (t *Transaction) GetState() TxState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		return err
	}
	defer func() {
		_ = tx.Close()
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err)
		}
		return err
	}
	return tx.Commit(ctx)
}

func safeAction(ctx context.Context, action func(ctx context.Context) error, description string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s: %v", description, r)
		}
	}()
	return action(ctx)
}

func safeResourceCommit(ctx context.Context, res TransactionalResource, index int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in resource commit %d: %v", index, r)
		}
	}()
	return res.Commit(ctx)
}

func safeResourceRollback(ctx context.Context, res TransactionalResource, index int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in resource rollback %d: %v", index, r)
		}
	}()
	return res.Rollback(ctx)
}
