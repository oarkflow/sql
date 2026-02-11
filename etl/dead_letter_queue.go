package etl

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/oarkflow/sql/pkg/utils"
)

// DeadLetterRecord represents a record that failed processing
type DeadLetterRecord struct {
	ID            string         `json:"id"`
	Record        utils.Record   `json:"record"`
	FailureReason string         `json:"failure_reason"`
	FailureTime   time.Time      `json:"failure_time"`
	RetryCount    int            `json:"retry_count"`
	MaxRetries    int            `json:"max_retries"`
	NodeName      string         `json:"node_name"`
	WorkerID      int            `json:"worker_id"`
	Metadata      map[string]any `json:"metadata"`
	NextRetry     time.Time      `json:"next_retry"`
}

// DeadLetterQueue manages failed records with retry capabilities
type DeadLetterQueue struct {
	queueFile      string
	maxSize        int
	maxRetries     int
	baseDelay      time.Duration
	maxDelay       time.Duration
	queue          []DeadLetterRecord
	mutex          sync.RWMutex
	stateManager   *StateManager
	processing     bool
	stopProcessing chan struct{}
}

// NewDeadLetterQueue creates a new dead letter queue
func NewDeadLetterQueue(queueFile string, maxSize, maxRetries int, baseDelay, maxDelay time.Duration) *DeadLetterQueue {
	dlq := &DeadLetterQueue{
		queueFile:      queueFile,
		maxSize:        maxSize,
		maxRetries:     maxRetries,
		baseDelay:      baseDelay,
		maxDelay:       maxDelay,
		queue:          make([]DeadLetterRecord, 0),
		stopProcessing: make(chan struct{}),
	}

	// Load existing dead letter records
	dlq.loadQueue()

	return dlq
}

// SetStateManager sets the state manager for coordination
func (dlq *DeadLetterQueue) SetStateManager(sm *StateManager) {
	dlq.stateManager = sm
}

// AddRecord adds a failed record to the dead letter queue
func (dlq *DeadLetterQueue) AddRecord(record utils.Record, reason, nodeName string, workerID int, metadata map[string]any) error {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	// Generate unique ID for the record
	id := fmt.Sprintf("%s_%d_%d", nodeName, workerID, time.Now().UnixNano())

	dlr := DeadLetterRecord{
		ID:            id,
		Record:        record,
		FailureReason: reason,
		FailureTime:   time.Now(),
		RetryCount:    0,
		MaxRetries:    dlq.maxRetries,
		NodeName:      nodeName,
		WorkerID:      workerID,
		Metadata:      metadata,
		NextRetry:     time.Now().Add(dlq.calculateRetryDelay(0)),
	}

	// Add to queue if there's space
	if len(dlq.queue) < dlq.maxSize {
		dlq.queue = append(dlq.queue, dlr)
		log.Printf("[DLQ] Added record %s from %s worker %d (reason: %s)", id, nodeName, workerID, reason)

		// Notify state manager
		if dlq.stateManager != nil {
			dlq.stateManager.AddError(nodeName, workerID, reason, 1, "ERROR")
		}

		return dlq.saveQueue()
	}

	// Queue is full, log the issue
	log.Printf("[DLQ] Queue full, dropping record from %s worker %d (reason: %s)", nodeName, workerID, reason)
	return fmt.Errorf("dead letter queue is full")
}

// calculateRetryDelay calculates the delay before next retry using exponential backoff
func (dlq *DeadLetterQueue) calculateRetryDelay(retryCount int) time.Duration {
	if retryCount >= dlq.maxRetries {
		return 0 // No more retries
	}

	// Exponential backoff with jitter
	delay := time.Duration(retryCount+1) * dlq.baseDelay
	if delay > dlq.maxDelay {
		delay = dlq.maxDelay
	}

	// Add jitter (±25%)
	jitterRange := int64(float64(delay) * 0.25)
	jitter := (time.Now().UnixNano()%2*2 - 1) * jitterRange
	return delay + time.Duration(jitter)
}

// GetRetryableRecords returns records that are ready for retry
func (dlq *DeadLetterQueue) GetRetryableRecords() []DeadLetterRecord {
	dlq.mutex.RLock()
	defer dlq.mutex.RUnlock()

	now := time.Now()
	var retryable []DeadLetterRecord

	for _, record := range dlq.queue {
		if record.RetryCount < record.MaxRetries && now.After(record.NextRetry) {
			retryable = append(retryable, record)
		}
	}

	return retryable
}

// ProcessRetries processes retryable records
func (dlq *DeadLetterQueue) ProcessRetries(ctx context.Context, processor func(DeadLetterRecord) error) {
	if dlq.processing {
		return
	}

	dlq.processing = true
	defer func() { dlq.processing = false }()

	ticker := time.NewTicker(30 * time.Second) // Check for retries every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dlq.processRetryBatch(ctx, processor)
		case <-dlq.stopProcessing:
			return
		case <-ctx.Done():
			return
		}
	}
}

// processRetryBatch processes a batch of retryable records
func (dlq *DeadLetterQueue) processRetryBatch(ctx context.Context, processor func(DeadLetterRecord) error) {
	retryable := dlq.GetRetryableRecords()
	if len(retryable) == 0 {
		return
	}

	log.Printf("[DLQ] Processing %d retryable records", len(retryable))

	for _, record := range retryable {
		select {
		case <-ctx.Done():
			return
		default:
			if err := processor(record); err != nil {
				dlq.markRetryFailed(record.ID, err.Error())
			} else {
				dlq.removeRecord(record.ID)
				log.Printf("[DLQ] Successfully retried record %s", record.ID)
			}
		}
	}
}

// markRetryFailed marks a retry as failed and schedules next retry
func (dlq *DeadLetterQueue) markRetryFailed(id, reason string) {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	for i := range dlq.queue {
		if dlq.queue[i].ID == id {
			dlq.queue[i].RetryCount++
			dlq.queue[i].FailureReason = reason
			dlq.queue[i].NextRetry = time.Now().Add(dlq.calculateRetryDelay(dlq.queue[i].RetryCount))

			if dlq.queue[i].RetryCount >= dlq.queue[i].MaxRetries {
				log.Printf("[DLQ] Record %s exceeded max retries (%d), giving up", id, dlq.queue[i].MaxRetries)
				// Move to permanent failure state
				dlq.queue[i].NextRetry = time.Time{} // Never retry again
			}

			dlq.saveQueue()
			break
		}
	}
}

// removeRecord removes a successfully processed record from the queue
func (dlq *DeadLetterQueue) removeRecord(id string) {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	for i, record := range dlq.queue {
		if record.ID == id {
			dlq.queue = append(dlq.queue[:i], dlq.queue[i+1:]...)
			dlq.saveQueue()
			break
		}
	}
}

// GetQueueStats returns statistics about the dead letter queue
func (dlq *DeadLetterQueue) GetQueueStats() map[string]any {
	dlq.mutex.RLock()
	defer dlq.mutex.RUnlock()

	now := time.Now()
	retryable := 0
	permanentFailures := 0
	totalRetries := 0

	for _, record := range dlq.queue {
		totalRetries += record.RetryCount
		if record.RetryCount >= record.MaxRetries {
			permanentFailures++
		} else if now.After(record.NextRetry) {
			retryable++
		}
	}

	return map[string]any{
		"total_records":       len(dlq.queue),
		"retryable_records":   retryable,
		"permanent_failures":  permanentFailures,
		"total_retries":       totalRetries,
		"max_size":            dlq.maxSize,
		"utilization_percent": float64(len(dlq.queue)) / float64(dlq.maxSize) * 100,
	}
}

// GetAllRecords returns all records in the queue regardless of status
func (dlq *DeadLetterQueue) GetAllRecords() []DeadLetterRecord {
	dlq.mutex.RLock()
	defer dlq.mutex.RUnlock()
	// Return a copy to avoid race conditions
	records := make([]DeadLetterRecord, len(dlq.queue))
	copy(records, dlq.queue)
	return records
}

// GetFailedRecords returns records that have permanently failed
func (dlq *DeadLetterQueue) GetFailedRecords() []DeadLetterRecord {
	dlq.mutex.RLock()
	defer dlq.mutex.RUnlock()

	var failed []DeadLetterRecord
	for _, record := range dlq.queue {
		if record.RetryCount >= record.MaxRetries {
			failed = append(failed, record)
		}
	}

	return failed
}

// ClearQueue clears all records from the dead letter queue
func (dlq *DeadLetterQueue) ClearQueue() error {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	dlq.queue = dlq.queue[:0]
	return dlq.saveQueue()
}

// Stop stops the retry processing
func (dlq *DeadLetterQueue) Stop() {
	close(dlq.stopProcessing)
}

// saveQueue saves the queue to file
func (dlq *DeadLetterQueue) saveQueue() error {
	data, err := json.MarshalIndent(dlq.queue, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal queue: %w", err)
	}

	tempFile := dlq.queueFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp queue file: %w", err)
	}

	return os.Rename(tempFile, dlq.queueFile)
}

// loadQueue loads the queue from file
func (dlq *DeadLetterQueue) loadQueue() error {
	data, err := os.ReadFile(dlq.queueFile)
	if err != nil {
		if os.IsNotExist(err) {
			// No existing queue file
			return nil
		}
		return fmt.Errorf("failed to read queue file: %w", err)
	}

	var queue []DeadLetterRecord
	if err := json.Unmarshal(data, &queue); err != nil {
		return fmt.Errorf("failed to unmarshal queue: %w", err)
	}

	dlq.queue = queue
	log.Printf("[DLQ] Loaded %d records from dead letter queue", len(queue))
	return nil
}

// Cleanup removes old records beyond a certain age
func (dlq *DeadLetterQueue) Cleanup(maxAge time.Duration) int {
	dlq.mutex.Lock()
	defer dlq.mutex.Unlock()

	cutoff := time.Now().Add(-maxAge)
	originalSize := len(dlq.queue)
	newQueue := make([]DeadLetterRecord, 0)

	for _, record := range dlq.queue {
		// Keep records that are still retryable or recently failed
		if record.NextRetry.IsZero() || record.FailureTime.After(cutoff) {
			newQueue = append(newQueue, record)
		}
	}

	cleaned := originalSize - len(newQueue)
	dlq.queue = newQueue

	if cleaned > 0 {
		log.Printf("[DLQ] Cleaned up %d old records", cleaned)
		dlq.saveQueue()
	}

	return cleaned
}
