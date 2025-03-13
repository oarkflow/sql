package transactions

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
	WithFields(fields map[string]interface{}) Logger
}

type DefaultLogger struct {
	fields map[string]interface{}
}

func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	msg = l.prependFields(msg)
	log.Printf("[INFO] "+msg, args...)
}

func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	msg = l.prependFields(msg)
	log.Printf("[ERROR] "+msg, args...)
}

func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	msg = l.prependFields(msg)
	log.Printf("[DEBUG] "+msg, args...)
}

func (l *DefaultLogger) WithFields(fields map[string]interface{}) Logger {
	newFields := make(map[string]interface{})
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}
	return &DefaultLogger{fields: newFields}
}

func (l *DefaultLogger) prependFields(msg string) string {
	if len(l.fields) == 0 {
		return msg
	}
	fieldStr := "[FIELDS: "
	first := true
	for k, v := range l.fields {
		if !first {
			fieldStr += ", "
		}
		fieldStr += fmt.Sprintf("%s=%v", k, v)
		first = false
	}
	fieldStr += "] "
	return fieldStr + msg
}

type MetricsCollector interface {
	IncCommitCount()
	IncRollbackCount()
	RecordCommitDuration(d time.Duration)
	RecordRollbackDuration(d time.Duration)
	IncErrorCount()
}

type NoopMetricsCollector struct{}

func (n *NoopMetricsCollector) IncCommitCount()                        {}
func (n *NoopMetricsCollector) IncRollbackCount()                      {}
func (n *NoopMetricsCollector) RecordCommitDuration(_ time.Duration)   {}
func (n *NoopMetricsCollector) RecordRollbackDuration(_ time.Duration) {}
func (n *NoopMetricsCollector) IncErrorCount()                         {}

type TransactionError struct {
	TxID       int64
	Err        error
	Action     string
	StackTrace string
}

func (te TransactionError) Error() string {
	return fmt.Sprintf("transaction %d [%s]: %v\nStackTrace:\n%s", te.TxID, te.Action, te.Err, te.StackTrace)
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
	MaxRetries      int
	Delay           time.Duration
	ShouldRetry     func(err error) bool
	BackoffStrategy func(attempt int) time.Duration
}

type TestHooks struct {
	SimulateCommitFailure   bool
	SimulateRollbackFailure bool
	SimulatePrepareFailure  bool
}

type DistributedCoordinator interface {
	BeginDistributed(tx *Transaction) error
	CommitDistributed(tx *Transaction) error
	RollbackDistributed(tx *Transaction) error
}

type LifecycleHooks struct {
	OnBegin          func(txID int64, ctx context.Context)
	OnBeforeCommit   func(txID int64, ctx context.Context)
	OnAfterCommit    func(txID int64, ctx context.Context)
	OnBeforeRollback func(txID int64, ctx context.Context)
	OnAfterRollback  func(txID int64, ctx context.Context)
	OnClose          func(txID int64, ctx context.Context)
}

type TransactionOptions struct {
	IsolationLevel         string
	Timeout                time.Duration
	PrepareTimeout         time.Duration
	CommitTimeout          time.Duration
	RollbackTimeout        time.Duration
	ParallelCommit         bool
	ParallelRollback       bool
	RetryPolicy            RetryPolicy
	LifecycleHooks         *LifecycleHooks
	DistributedCoordinator DistributedCoordinator
	Logger                 Logger
	Metrics                MetricsCollector
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

type AsyncResult struct {
	TxID  int64
	Err   error
	State TxState
}

type Transaction struct {
	mu                     sync.RWMutex
	id                     int64
	state                  TxState
	rollbackActions        []func(ctx context.Context) error
	commitActions          []func(ctx context.Context) error
	cleanupActions         []func(ctx context.Context) error
	resources              []TransactionalResource
	cleanupCalled          bool
	retryPolicy            RetryPolicy
	parallelCommit         bool
	parallelRollback       bool
	onError                func(txID int64, err error)
	isolationLevel         string
	timeout                time.Duration
	prepareTimeout         time.Duration
	commitTimeout          time.Duration
	rollbackTimeout        time.Duration
	lifecycleHooks         *LifecycleHooks
	distributedCoordinator DistributedCoordinator
	logger                 Logger
	metrics                MetricsCollector
	testHooks              *TestHooks
	savepoints             map[string]int
	abortCalled            bool
}

var txCounter int64

func NewTransaction() *Transaction {
	return NewTransactionWithOptions(TransactionOptions{
		IsolationLevel: "default",
		Timeout:        0,
		RetryPolicy: RetryPolicy{
			MaxRetries:  0,
			Delay:       0,
			ShouldRetry: func(err error) bool { return false },
		},
		Logger:  &DefaultLogger{},
		Metrics: &NoopMetricsCollector{},
	})
}

func NewTransactionWithOptions(opts TransactionOptions) *Transaction {
	tx := &Transaction{
		id:                     atomic.AddInt64(&txCounter, 1),
		state:                  StateInitialized,
		rollbackActions:        make([]func(ctx context.Context) error, 0),
		commitActions:          make([]func(ctx context.Context) error, 0),
		cleanupActions:         make([]func(ctx context.Context) error, 0),
		resources:              make([]TransactionalResource, 0),
		retryPolicy:            opts.RetryPolicy,
		isolationLevel:         opts.IsolationLevel,
		timeout:                opts.Timeout,
		prepareTimeout:         opts.PrepareTimeout,
		commitTimeout:          opts.CommitTimeout,
		rollbackTimeout:        opts.RollbackTimeout,
		parallelCommit:         opts.ParallelCommit,
		parallelRollback:       opts.ParallelRollback,
		logger:                 opts.Logger,
		metrics:                opts.Metrics,
		lifecycleHooks:         opts.LifecycleHooks,
		distributedCoordinator: opts.DistributedCoordinator,
		savepoints:             make(map[string]int),
	}
	return tx
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

func (t *Transaction) SetPrepareTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prepareTimeout = d
}

func (t *Transaction) SetCommitTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commitTimeout = d
}

func (t *Transaction) SetRollbackTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rollbackTimeout = d
}

func (t *Transaction) SetLifecycleHooks(hooks *LifecycleHooks) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lifecycleHooks = hooks
}

func (t *Transaction) SetDistributedCoordinator(dc DistributedCoordinator) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.distributedCoordinator = dc
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

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnBegin != nil {
		t.lifecycleHooks.OnBegin(t.id, ctx)
	}

	t.logger.Info("Transaction %d begun with isolation level '%s'", t.id, t.isolationLevel)
	if batch != nil {
		t.logger.Info("Transaction %d begun for batch %v", t.id, batch)
	}

	if t.distributedCoordinator != nil {
		if err := t.distributedCoordinator.BeginDistributed(t); err != nil {
			return err
		}
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

	var prepareCtx context.Context
	var cancel context.CancelFunc
	t.mu.RLock()
	pt := t.prepareTimeout
	t.mu.RUnlock()
	if pt > 0 {
		prepareCtx, cancel = context.WithTimeout(ctx, pt)
		defer cancel()
	} else {
		prepareCtx = ctx
	}

	t.mu.RLock()
	if t.testHooks != nil && t.testHooks.SimulatePrepareFailure {
		t.mu.RUnlock()
		return errors.New("simulated prepare failure")
	}
	t.mu.RUnlock()

	for i, res := range t.resources {
		if pr, ok := res.(PreparableResource); ok {
			desc := fmt.Sprintf("resource prepare %d for transaction %d", i, t.id)
			if err := retryAction(prepareCtx, func(ctx context.Context) error {
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

	var commitCtx context.Context
	var cancel context.CancelFunc
	t.mu.RLock()
	ct := t.commitTimeout
	t.mu.RUnlock()
	if ct > 0 {
		commitCtx, cancel = context.WithTimeout(ctx, ct)
		defer cancel()
	} else {
		commitCtx = ctx
	}

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommit != nil {
		t.lifecycleHooks.OnBeforeCommit(t.id, commitCtx)
	}

	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot commit transaction %d in state %s", t.id, t.state)
	}
	batch := commitCtx.Value("batch")
	if err := commitCtx.Err(); err != nil {
		t.mu.Unlock()
		t.logger.Error("Transaction %d: Context error during commit: %v", t.id, err)
		return err
	}
	t.mu.Unlock()

	if t.testHooks != nil && t.testHooks.SimulateCommitFailure {
		err := fmt.Errorf("simulated commit failure")
		t.logger.Error("Transaction %d: %v", t.id, err)
		t.metrics.IncErrorCount()
		_ = t.Rollback(commitCtx)
		return TransactionError{TxID: t.id, Err: err, Action: "commit", StackTrace: string(debug.Stack())}
	}

	if err := t.prepareResources(commitCtx); err != nil {
		t.mu.Lock()
		t.state = StateFailed
		t.mu.Unlock()
		t.logger.Error("Transaction %d: Prepare phase failed: %v", t.id, err)
		t.metrics.IncErrorCount()
		_ = t.Rollback(commitCtx)
		return TransactionError{TxID: t.id, Err: err, Action: "prepare", StackTrace: string(debug.Stack())}
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
				if err := retryAction(commitCtx, func(ctx context.Context) error {
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
			rbErr := t.Rollback(commitCtx)
			if rbErr != nil {
				handleError(rbErr)
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit errors: %v; rollback failed: %v", errList, rbErr), Action: "commit", StackTrace: string(debug.Stack())}
			}
			handleError(fmt.Errorf("commit errors: %v", errList))
			t.metrics.IncErrorCount()
			return TransactionError{TxID: t.id, Err: fmt.Errorf("commit errors: %v", errList), Action: "commit", StackTrace: string(debug.Stack())}
		}
	} else {
		for i, action := range t.commitActions {
			desc := fmt.Sprintf("commit action %d in transaction %d", i, t.id)
			if err := retryAction(commitCtx, func(ctx context.Context) error {
				return safeAction(ctx, action, desc)
			}, desc, t.retryPolicy); err != nil {
				t.mu.Lock()
				t.state = StateFailed
				t.mu.Unlock()
				t.logger.Error("Transaction %d: Commit action failed: %v", t.id, err)
				handleError(err)
				rbErr := t.Rollback(commitCtx)
				if rbErr != nil {
					return TransactionError{TxID: t.id, Err: fmt.Errorf("commit failed: %v; rollback failed: %v", err, rbErr), Action: "commit", StackTrace: string(debug.Stack())}
				}
				t.metrics.IncErrorCount()
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit action failed: %v", err), Action: "commit", StackTrace: string(debug.Stack())}
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
				if err := retryAction(commitCtx, func(ctx context.Context) error {
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
			rbErr := t.Rollback(commitCtx)
			if rbErr != nil {
				handleError(rbErr)
				return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit errors: %v; rollback failed: %v", errList, rbErr), Action: "commit", StackTrace: string(debug.Stack())}
			}
			handleError(fmt.Errorf("resource commit errors: %v", errList))
			t.metrics.IncErrorCount()
			return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit errors: %v", errList), Action: "commit", StackTrace: string(debug.Stack())}
		}
	} else {
		for i, res := range t.resources {
			desc := fmt.Sprintf("resource commit %d for transaction %d", i, t.id)
			if err := retryAction(commitCtx, func(ctx context.Context) error {
				return safeResourceCommit(ctx, res, i, t.id)
			}, desc, t.retryPolicy); err != nil {
				t.mu.Lock()
				t.state = StateFailed
				t.mu.Unlock()
				t.logger.Error("Transaction %d: Resource commit failed: %v", t.id, err)
				handleError(err)
				rbErr := t.Rollback(commitCtx)
				if rbErr != nil {
					return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit failed: %v; rollback failed: %v", err, rbErr), Action: "commit", StackTrace: string(debug.Stack())}
				}
				t.metrics.IncErrorCount()
				return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit failed: %v", err), Action: "commit", StackTrace: string(debug.Stack())}
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

	if t.distributedCoordinator != nil {
		if err := t.distributedCoordinator.CommitDistributed(t); err != nil {
			return err
		}
	}
	if err := t.runCleanup(commitCtx); err != nil {
		handleError(err)
		t.metrics.IncErrorCount()
		return TransactionError{TxID: t.id, Err: fmt.Errorf("commit cleanup error: %v", err), Action: "cleanup", StackTrace: string(debug.Stack())}
	}
	t.metrics.RecordCommitDuration(time.Since(startTime))
	t.metrics.IncCommitCount()

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommit != nil {
		t.lifecycleHooks.OnAfterCommit(t.id, commitCtx)
	}
	return nil
}

func (t *Transaction) AsyncCommitWithResult(ctx context.Context) <-chan AsyncResult {
	resultCh := make(chan AsyncResult, 1)
	go func() {
		err := t.Commit(ctx)
		t.mu.RLock()
		state := t.state
		t.mu.RUnlock()
		resultCh <- AsyncResult{TxID: t.id, Err: err, State: state}
	}()
	return resultCh
}

func (t *Transaction) AsyncCommit(ctx context.Context) <-chan error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- t.Commit(ctx)
	}()
	return resultCh
}

func (t *Transaction) Rollback(ctx context.Context) error {
	startTime := time.Now()

	var rbCtx context.Context
	var cancel context.CancelFunc
	t.mu.RLock()
	rt := t.rollbackTimeout
	t.mu.RUnlock()
	if rt > 0 {
		rbCtx, cancel = context.WithTimeout(ctx, rt)
		defer cancel()
	} else {
		rbCtx = ctx
	}

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeRollback != nil {
		t.lifecycleHooks.OnBeforeRollback(t.id, rbCtx)
	}

	t.mu.Lock()
	if t.state != StateInProgress && t.state != StateFailed && !t.abortCalled {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback transaction %d in state %s", t.id, t.state)
	}
	if err := rbCtx.Err(); err != nil {
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
		return TransactionError{TxID: t.id, Err: err, Action: "rollback", StackTrace: string(debug.Stack())}
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
				if err := retryAction(rbCtx, func(ctx context.Context) error {
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
			if err := retryAction(rbCtx, func(ctx context.Context) error {
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
				if err := retryAction(rbCtx, func(ctx context.Context) error {
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
			if err := retryAction(rbCtx, func(ctx context.Context) error {
				return safeResourceRollback(ctx, resources[i], i, t.id)
			}, desc, t.retryPolicy); err != nil {
				errs = append(errs, err)
				t.logger.Error("Transaction %d: Error during resource rollback %d: %v", t.id, i, err)
			}
		}
	}

	if cleanupErr := t.runCleanup(rbCtx); cleanupErr != nil {
		errs = append(errs, cleanupErr)
	}

	if len(errs) > 0 {
		t.metrics.IncErrorCount()
		return TransactionError{TxID: t.id, Err: fmt.Errorf("rollback encountered errors: %v", errs), Action: "rollback", StackTrace: string(debug.Stack())}
	}
	t.metrics.RecordRollbackDuration(time.Since(startTime))
	t.metrics.IncRollbackCount()

	if t.distributedCoordinator != nil {
		if err := t.distributedCoordinator.RollbackDistributed(t); err != nil {
			return err
		}
	}

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterRollback != nil {
		t.lifecycleHooks.OnAfterRollback(t.id, rbCtx)
	}
	return nil
}

func (t *Transaction) AsyncRollbackWithResult(ctx context.Context) <-chan AsyncResult {
	resultCh := make(chan AsyncResult, 1)
	go func() {
		err := t.Rollback(ctx)
		t.mu.RLock()
		state := t.state
		t.mu.RUnlock()
		resultCh <- AsyncResult{TxID: t.id, Err: err, State: state}
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

func (t *Transaction) Abort(ctx context.Context) error {
	t.mu.Lock()
	t.abortCalled = true
	t.mu.Unlock()
	t.logger.Info("Transaction %d: Aborting", t.id)
	return t.Rollback(ctx)
}

func (t *Transaction) Close() error {
	t.mu.RLock()
	currentState := t.state
	t.mu.RUnlock()

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnClose != nil {
		t.lifecycleHooks.OnClose(t.id, context.Background())
	}
	if currentState == StateCommitted || currentState == StateRolledBack {
		return nil
	}
	t.logger.Info("Transaction %d: Closing - performing rollback", t.id)
	return t.Rollback(context.Background())
}

func (t *Transaction) CreateSavepoint(_ context.Context) (int, error) {
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
	t.mu.RLock()
	sp, exists := t.savepoints[name]
	t.mu.RUnlock()
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
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

func (t *Transaction) GetID() int64 {
	return t.id
}

type NestedTransaction struct {
	parent    *Transaction
	savepoint int
	active    bool
}

func (t *Transaction) BeginNested(ctx context.Context) (*NestedTransaction, error) {
	sp, err := t.CreateSavepoint(ctx)
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

func (nt *NestedTransaction) BeginNested(ctx context.Context) (*NestedTransaction, error) {
	return nt.parent.BeginNested(ctx)
}

func (t *Transaction) ReleaseSavepoint(_ context.Context, sp int) error {
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
	if err := nt.parent.ReleaseSavepoint(ctx, nt.savepoint); err != nil {
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
			return TransactionError{TxID: tx.id, Err: fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err), Action: "rollback", StackTrace: string(debug.Stack())}
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

				var delay time.Duration
				if rp.BackoffStrategy != nil {
					delay = rp.BackoffStrategy(attempt)
				} else {
					delay = rp.Delay
				}
				log.Printf("%s failed on attempt %d: %v; retrying after %v", description, attempt, err, delay)
				select {
				case <-time.After(delay):
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
