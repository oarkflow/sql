package etl

import (
	"context"
	"fmt"
	"sync"
)

type Transaction struct {
	mu              sync.Mutex
	rollbackActions []func() error
	committed       bool
}

func NewTransaction() *Transaction {
	return &Transaction{
		rollbackActions: make([]func() error, 0),
	}
}

func (t *Transaction) Begin(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committed = false
	t.rollbackActions = make([]func() error, 0)
	return nil
}

func (t *Transaction) RegisterRollback(fn func() error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.committed {
		t.rollbackActions = append(t.rollbackActions, fn)
	}
}

func (t *Transaction) Commit(_ context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committed = true
	t.rollbackActions = nil
	return nil
}

func (t *Transaction) Rollback(_ context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var err error
	for i := len(t.rollbackActions) - 1; i >= 0; i-- {
		if e := t.rollbackActions[i](); e != nil && err == nil {
			err = e
		}
	}
	t.rollbackActions = nil
	return err
}

func (t *Transaction) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.committed && len(t.rollbackActions) > 0 {
		return t.Rollback(context.Background())
	}
	return nil
}

func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err)
		}
		return err
	}
	return tx.Commit(ctx)
}
