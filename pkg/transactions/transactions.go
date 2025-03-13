package transactions

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
}

type defaultLogger struct{}

func (l *defaultLogger) Info(msg string, args ...interface{}) {
	log.Printf("[INFO] "+msg, args...)
}

func (l *defaultLogger) Error(msg string, args ...interface{}) {
	log.Printf("[ERROR] "+msg, args...)
}

func (l *defaultLogger) Debug(msg string, args ...interface{}) {
	log.Printf("[DEBUG] "+msg, args...)
}

type MetricsCollector interface {
	IncCommitCount()
	IncRollbackCount()
	RecordCommitDuration(d time.Duration)
	RecordRollbackDuration(d time.Duration)
	IncErrorCount()
}

type noopMetricsCollector struct{}

func (n *noopMetricsCollector) IncCommitCount()                        {}
func (n *noopMetricsCollector) IncRollbackCount()                      {}
func (n *noopMetricsCollector) RecordCommitDuration(d time.Duration)   {}
func (n *noopMetricsCollector) RecordRollbackDuration(d time.Duration) {}
func (n *noopMetricsCollector) IncErrorCount()                         {}

type TransactionError struct {
	TxID int64
	Err  error
}

func (te TransactionError) Error() string {
	return fmt.Sprintf("transaction %d: %v", te.TxID, te.Err)
}

type TransactionalResource interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type PreparableResource interface {
	TransactionalResource
	Prepare(ctx context.Context) error
}

type RetryPolicy struct {
	MaxRetries  int
	Delay       time.Duration
	ShouldRetry func(err error) bool
}

type TestHooks struct {
	SimulateCommitFailure   bool
	SimulateRollbackFailure bool
}

var txCounter int64

type Transaction struct {
	mu               sync.Mutex
	id               int64
	state            TxState
	rollbackActions  []func(ctx context.Context) error
	commitActions    []func(ctx context.Context) error
	cleanupActions   []func(ctx context.Context) error
	resources        []TransactionalResource
	cleanupCalled    bool
	retryPolicy      RetryPolicy
	parallelCommit   bool
	parallelRollback bool
	onError          func(txID int64, err error)
	isolationLevel   string
	timeout          time.Duration

	logger      Logger
	metrics     MetricsCollector
	testHooks   *TestHooks
	savepoints  map[string]int
	abortCalled bool
}

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

func NewTransaction() *Transaction {
	t := &Transaction{
		id:              atomic.AddInt64(&txCounter, 1),
		state:           StateInitialized,
		rollbackActions: make([]func(ctx context.Context) error, 0),
		commitActions:   make([]func(ctx context.Context) error, 0),
		cleanupActions:  make([]func(ctx context.Context) error, 0),
		resources:       make([]TransactionalResource, 0),
		retryPolicy: RetryPolicy{
			MaxRetries:  0,
			Delay:       0,
			ShouldRetry: func(err error) bool { return false },
		},
		logger:     &defaultLogger{},
		metrics:    &noopMetricsCollector{},
		savepoints: make(map[string]int),
	}
	return t
}

func (t *Transaction) SetLogger(l Logger) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = l
}

func (t *Transaction) SetMetricsCollector(m MetricsCollector) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = m
}

func (t *Transaction) SetTestHooks(th *TestHooks) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.testHooks = th
}

func (t *Transaction) SetRetryPolicy(rp RetryPolicy) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retryPolicy = rp
}

func (t *Transaction) SetParallelExecution(parallelCommit, parallelRollback bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parallelCommit = parallelCommit
	t.parallelRollback = parallelRollback
}

func (t *Transaction) SetOnError(f func(txID int64, err error)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onError = f
}

func (t *Transaction) SetIsolationLevel(level string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolationLevel = level
}

func (t *Transaction) SetTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timeout = d
}

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
	t.savepoints = make(map[string]int)
	t.abortCalled = false
	t.logger.Info("Transaction %d begun with isolation level '%s'", t.id, t.isolationLevel)
	if batch != nil {
		t.logger.Info("Transaction %d begun for batch %v", t.id, batch)
	}
	return nil
}

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
	t.logger.Info("Transaction %d: Registered rollback action", t.id)
	return nil
}

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
	t.logger.Info("Transaction %d: Registered commit action", t.id)
	return nil
}

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
	t.logger.Info("Transaction %d: Registered cleanup action", t.id)
	return nil
}

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
	t.logger.Info("Transaction %d: Registered transactional resource", t.id)
	return nil
}

func (t *Transaction) prepareResources(ctx context.Context) error {
	for i, res := range t.resources {
		if pr, ok := res.(PreparableResource); ok {
			desc := fmt.Sprintf("resource prepare %d for transaction %d", i, t.id)
			if err := retryAction(ctx, func(ctx context.Context) error {
				return safeAction(ctx, pr.Prepare, desc)
			}, desc, t.retryPolicy); err != nil {
				t.logger.Error("Transaction %d: Resource prepare failed: %v", t.id, err)
				return err
			}
		}
	}
	return nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	startTime := time.Now()

	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}

	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot commit transaction %d in state %s", t.id, t.state)
	}
	batch := ctx.Value("batch")
	if err := ctx.Err(); err != nil {
		t.mu.Unlock()
		t.logger.Error("Transaction %d: Context error during commit: %v", t.id, err)
		return err
	}
	t.mu.Unlock()

	if t.testHooks != nil && t.testHooks.SimulateCommitFailure {
		err := fmt.Errorf("simulated commit failure")
		t.logger.Error("Transaction %d: %v", t.id, err)
		t.metrics.IncErrorCount()
		_ = t.Rollback(ctx)
		return TransactionError{TxID: t.id, Err: err}
	}

	if err := t.prepareResources(ctx); err != nil {
		t.mu.Lock()
		t.state = StateFailed
		t.mu.Unlock()
		t.logger.Error("Transaction %d: Prepare phase failed: %v", t.id, err)
		t.metrics.IncErrorCount()
		_ = t.Rollback(ctx)
		return TransactionError{TxID: t.id, Err: err}
	}

	handleError := func(err error) {
		if t.onError != nil {
			t.onError(t.id, err)
		}
	}

	if t.parallelCommit {
		var wg sync.WaitGroup
		errCh := make(chan error, len(t.commitActions))
		for i, action := range t.commitActions {
			wg.Add(1)
			go func(i int, action func(ctx context.Context) error) {
				defer wg.Done()
				desc := fmt.Sprintf("commit action %d in transaction %d", i, t.id)
				if err := retryAction(ctx, func(ctx context.Context) error {
					return safeAction(ctx, action, desc)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
				}
			}(i, action)
		}
		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			var errList []error
			for err := range errCh {
				errList = append(errList, err)
			}
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			rbErr := t.Rollback(ctx)
			if rbErr != nil {
				handleError(rbErr)
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit errors: %v; rollback failed: %v", errList, rbErr)}
			}
			handleError(fmt.Errorf("commit errors: %v", errList))
			t.metrics.IncErrorCount()
			return TransactionError{TxID: t.id, Err: fmt.Errorf("commit errors: %v", errList)}
		}
	} else {
		for i, action := range t.commitActions {
			desc := fmt.Sprintf("commit action %d in transaction %d", i, t.id)
			if err := retryAction(ctx, func(ctx context.Context) error {
				return safeAction(ctx, action, desc)
			}, desc, t.retryPolicy); err != nil {
				t.mu.Lock()
				t.state = StateFailed
				t.mu.Unlock()
				t.logger.Error("Transaction %d: Commit action failed: %v", t.id, err)
				handleError(err)
				rbErr := t.Rollback(ctx)
				if rbErr != nil {
					return TransactionError{TxID: t.id, Err: fmt.Errorf("commit failed: %v; rollback failed: %v", err, rbErr)}
				}
				t.metrics.IncErrorCount()
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit action failed: %v", err)}
			}
		}
	}

	if t.parallelCommit {
		var wg sync.WaitGroup
		errCh := make(chan error, len(t.resources))
		for i, res := range t.resources {
			wg.Add(1)
			go func(i int, res TransactionalResource) {
				defer wg.Done()
				desc := fmt.Sprintf("resource commit %d for transaction %d", i, t.id)
				if err := retryAction(ctx, func(ctx context.Context) error {
					return safeResourceCommit(ctx, res, i, t.id)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
				}
			}(i, res)
		}
		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			var errList []error
			for err := range errCh {
				errList = append(errList, err)
			}
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			rbErr := t.Rollback(ctx)
			if rbErr != nil {
				handleError(rbErr)
				return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit errors: %v; rollback failed: %v", errList, rbErr)}
			}
			handleError(fmt.Errorf("resource commit errors: %v", errList))
			t.metrics.IncErrorCount()
			return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit errors: %v", errList)}
		}
	} else {
		for i, res := range t.resources {
			desc := fmt.Sprintf("resource commit %d for transaction %d", i, t.id)
			if err := retryAction(ctx, func(ctx context.Context) error {
				return safeResourceCommit(ctx, res, i, t.id)
			}, desc, t.retryPolicy); err != nil {
				t.mu.Lock()
				t.state = StateFailed
				t.mu.Unlock()
				t.logger.Error("Transaction %d: Resource commit failed: %v", t.id, err)
				handleError(err)
				rbErr := t.Rollback(ctx)
				if rbErr != nil {
					return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit failed: %v; rollback failed: %v", err, rbErr)}
				}
				t.metrics.IncErrorCount()
				return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit failed: %v", err)}
			}
		}
	}

	t.mu.Lock()
	t.state = StateCommitted
	t.rollbackActions = nil
	t.mu.Unlock()
	if batch != nil {
		t.logger.Info("Transaction %d committed successfully for batch %v", t.id, batch)
	} else {
		t.logger.Info("Transaction %d committed successfully", t.id)
	}
	if err := t.runCleanup(ctx); err != nil {
		handleError(err)
		t.metrics.IncErrorCount()
		return TransactionError{TxID: t.id, Err: fmt.Errorf("commit cleanup error: %v", err)}
	}
	t.metrics.RecordCommitDuration(time.Since(startTime))
	t.metrics.IncCommitCount()
	return nil
}

func (t *Transaction) AsyncCommit(ctx context.Context) <-chan error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- t.Commit(ctx)
	}()
	return resultCh
}

func (t *Transaction) AsyncRollback(ctx context.Context) <-chan error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- t.Rollback(ctx)
	}()
	return resultCh
}

func (t *Transaction) Rollback(ctx context.Context) error {
	startTime := time.Now()

	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}

	t.mu.Lock()
	if t.state != StateInProgress && t.state != StateFailed && !t.abortCalled {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback transaction %d in state %s", t.id, t.state)
	}

	if err := ctx.Err(); err != nil {
		t.mu.Unlock()
		t.logger.Error("Transaction %d: Context error during rollback: %v", t.id, err)
		return err
	}
	actions := t.rollbackActions
	resources := t.resources
	t.rollbackActions = nil
	t.resources = nil
	t.state = StateRolledBack
	t.mu.Unlock()

	if t.testHooks != nil && t.testHooks.SimulateRollbackFailure {
		err := fmt.Errorf("simulated rollback failure")
		t.logger.Error("Transaction %d: %v", t.id, err)
		t.metrics.IncErrorCount()
		return TransactionError{TxID: t.id, Err: err}
	}

	var errs []error
	t.logger.Info("Transaction %d: Rolling back", t.id)

	if t.parallelRollback {
		var wg sync.WaitGroup
		errCh := make(chan error, len(actions))
		for i := len(actions) - 1; i >= 0; i-- {
			wg.Add(1)
			go func(i int, action func(ctx context.Context) error) {
				defer wg.Done()
				desc := fmt.Sprintf("rollback action %d in transaction %d", i, t.id)
				if err := retryAction(ctx, func(ctx context.Context) error {
					return safeAction(ctx, action, desc)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
				}
			}(i, actions[i])
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			errs = append(errs, err)
			t.logger.Error("Transaction %d: Error during parallel rollback action: %v", t.id, err)
		}
	} else {
		for i := len(actions) - 1; i >= 0; i-- {
			desc := fmt.Sprintf("rollback action %d in transaction %d", i, t.id)
			if err := retryAction(ctx, func(ctx context.Context) error {
				return safeAction(ctx, actions[i], desc)
			}, desc, t.retryPolicy); err != nil {
				errs = append(errs, err)
				t.logger.Error("Transaction %d: Error during rollback action %d: %v", t.id, i, err)
			}
		}
	}

	if t.parallelRollback {
		var wg sync.WaitGroup
		errCh := make(chan error, len(resources))
		for i := len(resources) - 1; i >= 0; i-- {
			wg.Add(1)
			go func(i int, res TransactionalResource) {
				defer wg.Done()
				desc := fmt.Sprintf("resource rollback %d for transaction %d", i, t.id)
				if err := retryAction(ctx, func(ctx context.Context) error {
					return safeResourceRollback(ctx, res, i, t.id)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
				}
			}(i, resources[i])
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			errs = append(errs, err)
			t.logger.Error("Transaction %d: Error during parallel resource rollback: %v", t.id, err)
		}
	} else {
		for i := len(resources) - 1; i >= 0; i-- {
			desc := fmt.Sprintf("resource rollback %d for transaction %d", i, t.id)
			if err := retryAction(ctx, func(ctx context.Context) error {
				return safeResourceRollback(ctx, resources[i], i, t.id)
			}, desc, t.retryPolicy); err != nil {
				errs = append(errs, err)
				t.logger.Error("Transaction %d: Error during resource rollback %d: %v", t.id, i, err)
			}
		}
	}

	if cleanupErr := t.runCleanup(ctx); cleanupErr != nil {
		errs = append(errs, cleanupErr)
	}

	if len(errs) > 0 {
		t.metrics.IncErrorCount()
		return TransactionError{TxID: t.id, Err: fmt.Errorf("rollback encountered errors: %v", errs)}
	}
	t.metrics.RecordRollbackDuration(time.Since(startTime))
	t.metrics.IncRollbackCount()
	return nil
}

func (t *Transaction) Abort(ctx context.Context) error {
	t.mu.Lock()
	t.abortCalled = true
	t.mu.Unlock()
	t.logger.Info("Transaction %d: Aborting", t.id)
	return t.Rollback(ctx)
}

func (t *Transaction) Close() error {
	t.mu.Lock()
	currentState := t.state
	t.mu.Unlock()
	if currentState == StateCommitted || currentState == StateRolledBack {
		return nil
	}
	t.logger.Info("Transaction %d: Closing - performing rollback", t.id)
	return t.Rollback(context.Background())
}

func (t *Transaction) CreateSavepoint() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return 0, fmt.Errorf("cannot create savepoint in transaction %d state %s", t.id, t.state)
	}
	sp := len(t.rollbackActions)
	t.logger.Info("Transaction %d: Created savepoint at index %d", t.id, sp)
	return sp, nil
}

func (t *Transaction) CreateNamedSavepoint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot create named savepoint in transaction %d state %s", t.id, t.state)
	}
	if _, exists := t.savepoints[name]; exists {
		return fmt.Errorf("savepoint '%s' already exists in transaction %d", name, t.id)
	}
	t.savepoints[name] = len(t.rollbackActions)
	t.logger.Info("Transaction %d: Created named savepoint '%s' at index %d", t.id, name, t.savepoints[name])
	return nil
}

func (t *Transaction) RollbackToNamedSavepoint(ctx context.Context, name string) error {
	t.mu.Lock()
	sp, exists := t.savepoints[name]
	t.mu.Unlock()
	if !exists {
		return fmt.Errorf("savepoint '%s' does not exist in transaction %d", name, t.id)
	}
	return t.RollbackToSavepoint(ctx, sp)
}

func (t *Transaction) ReleaseNamedSavepoint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	sp, exists := t.savepoints[name]
	if !exists {
		return fmt.Errorf("savepoint '%s' does not exist in transaction %d", name, t.id)
	}

	if sp < 0 || sp > len(t.rollbackActions) {
		return fmt.Errorf("invalid savepoint index %d for transaction %d", sp, t.id)
	}
	t.rollbackActions = t.rollbackActions[:sp]
	delete(t.savepoints, name)
	t.logger.Info("Transaction %d: Released named savepoint '%s'", t.id, name)
	return nil
}

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
	t.logger.Info("Transaction %d: Rolling back to savepoint at index %d", t.id, sp)
	for i := len(actionsToRollback) - 1; i >= 0; i-- {
		desc := fmt.Sprintf("rollback savepoint action %d in transaction %d", i, t.id)
		if err := retryAction(ctx, func(ctx context.Context) error {
			return safeAction(ctx, actionsToRollback[i], desc)
		}, desc, t.retryPolicy); err != nil {
			errs = append(errs, err)
			t.logger.Error("Transaction %d: Error during rollback action at savepoint: %v", t.id, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("transaction %d rollback to savepoint encountered errors: %v", t.id, errs)
	}
	return nil
}

func (t *Transaction) GetState() TxState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *Transaction) GetID() int64 {
	return t.id
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
			return TransactionError{TxID: tx.id, Err: fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err)}
		}
		return err
	}
	return tx.Commit(ctx)
}

func (t *Transaction) runCleanup(ctx context.Context) error {
	t.mu.Lock()
	if t.cleanupCalled {
		t.mu.Unlock()
		return nil
	}
	actions := t.cleanupActions
	t.cleanupCalled = true
	t.mu.Unlock()

	var errs []error
	for i, action := range actions {
		desc := fmt.Sprintf("cleanup action %d in transaction %d", i, t.id)
		if err := safeAction(ctx, action, desc); err != nil {
			errs = append(errs, err)
			t.logger.Error("Transaction %d: Error during cleanup action %d: %v", t.id, i, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("cleanup encountered errors: %v", errs)
	}
	return nil
}

func retryAction(ctx context.Context, action func(ctx context.Context) error, description string, rp RetryPolicy) error {
	var err error
	for attempt := 0; attempt <= rp.MaxRetries; attempt++ {
		if err = action(ctx); err != nil {
			if rp.ShouldRetry != nil && rp.ShouldRetry(err) {
				log.Printf("%s failed on attempt %d: %v; retrying after %v", description, attempt, err, rp.Delay)
				select {
				case <-time.After(rp.Delay):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return err
		}
		return nil
	}
	return err
}

func safeAction(ctx context.Context, action func(ctx context.Context) error, description string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s: %v", description, r)
		}
	}()
	return action(ctx)
}

func safeResourceCommit(ctx context.Context, res TransactionalResource, index int, txID int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in resource commit %d for transaction %d: %v", index, txID, r)
		}
	}()
	return res.Commit(ctx)
}

func safeResourceRollback(ctx context.Context, res TransactionalResource, index int, txID int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in resource rollback %d for transaction %d: %v", index, txID, r)
		}
	}()
	return res.Rollback(ctx)
}

type NestedTransaction struct {
	parent    *Transaction
	savepoint int
	active    bool
}

func (t *Transaction) BeginNested(ctx context.Context) (*NestedTransaction, error) {
	sp, err := t.CreateSavepoint()
	if err != nil {
		return nil, err
	}
	nt := &NestedTransaction{
		parent:    t,
		savepoint: sp,
		active:    true,
	}
	t.logger.Info("Transaction %d: Nested transaction started at savepoint %d", t.id, sp)
	return nt, nil
}

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
	t.logger.Info("Transaction %d: Released savepoint at index %d", t.id, sp)
	return nil
}

func (nt *NestedTransaction) Commit(ctx context.Context) error {
	if !nt.active {
		return fmt.Errorf("nested transaction already closed")
	}
	if err := nt.parent.ReleaseSavepoint(nt.savepoint); err != nil {
		return err
	}
	nt.active = false
	nt.parent.logger.Info("Transaction %d: Nested transaction committed (released savepoint %d)", nt.parent.id, nt.savepoint)
	return nil
}

func (nt *NestedTransaction) Rollback(ctx context.Context) error {
	if !nt.active {
		return fmt.Errorf("nested transaction already closed")
	}
	if err := nt.parent.RollbackToSavepoint(ctx, nt.savepoint); err != nil {
		return err
	}
	nt.active = false
	nt.parent.logger.Info("Transaction %d: Nested transaction rolled back to savepoint %d", nt.parent.id, nt.savepoint)
	return nil
}
