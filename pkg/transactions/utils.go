package transactions

import (
	"context"
	"fmt"
	"log"
	"time"
)

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
