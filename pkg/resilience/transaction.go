package resilience

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Transaction struct {
	mu              sync.Mutex
	rollbackActions []func(ctx context.Context) error
	committed       bool
}

func NewTransaction() *Transaction {
	return &Transaction{
		rollbackActions: make([]func(ctx context.Context) error, 0),
	}
}

func (t *Transaction) Begin(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return fmt.Errorf("transaction already committed")
	}
	t.rollbackActions = make([]func(ctx context.Context) error, 0)
	return nil
}

func (t *Transaction) RegisterRollback(fn func(ctx context.Context) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return fmt.Errorf("cannot register rollback, transaction already committed")
	}
	t.rollbackActions = append(t.rollbackActions, fn)
	log.Println("Registered rollback action")
	return nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return fmt.Errorf("transaction already committed")
	}
	t.committed = true
	t.rollbackActions = nil
	return nil
}

func (t *Transaction) Rollback(ctx context.Context) error {
	t.mu.Lock()
	if t.committed || len(t.rollbackActions) == 0 {
		t.mu.Unlock()
		return nil
	}
	actions := t.rollbackActions
	t.rollbackActions = nil
	t.mu.Unlock()
	var errs []error
	log.Println("Rolling back transaction")
	for i := len(actions) - 1; i >= 0; i-- {
		if err := actions[i](ctx); err != nil {
			errs = append(errs, err)
			log.Printf("Error during rollback action %d: %v", i, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("rollback encountered errors: %v", errs)
	}
	return nil
}

func (t *Transaction) Close() error {
	t.mu.Lock()
	if t.committed || len(t.rollbackActions) == 0 {
		t.mu.Unlock()
		return nil
	}
	actions := t.rollbackActions
	t.rollbackActions = nil
	t.mu.Unlock()
	var errs []error
	log.Println("Closing transaction: performing rollback")
	for i := len(actions) - 1; i >= 0; i-- {
		if err := actions[i](context.Background()); err != nil {
			errs = append(errs, err)
			log.Printf("Error during rollback action %d in Close: %v", i, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close rollback encountered errors: %v", errs)
	}
	return nil
}

func (t *Transaction) CreateSavepoint() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return 0, fmt.Errorf("cannot create savepoint: transaction already committed")
	}
	sp := len(t.rollbackActions)
	log.Printf("Created savepoint at index %d", sp)
	return sp, nil
}

func (t *Transaction) RollbackToSavepoint(ctx context.Context, sp int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return fmt.Errorf("cannot rollback to savepoint: transaction already committed")
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		return fmt.Errorf("invalid savepoint index: %d", sp)
	}
	var errs []error
	log.Printf("Rolling back to savepoint at index %d", sp)
	for i := len(t.rollbackActions) - 1; i >= sp; i-- {
		if err := t.rollbackActions[i](ctx); err != nil {
			errs = append(errs, err)
			log.Printf("Error during rollback action %d at savepoint: %v", i, err)
		}
	}
	t.rollbackActions = t.rollbackActions[:sp]
	if len(errs) > 0 {
		return fmt.Errorf("rollback to savepoint encountered errors: %v", errs)
	}
	return nil
}

func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		return err
	}
	defer tx.Close()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err)
		}
		return err
	}
	return tx.Commit(ctx)
}
