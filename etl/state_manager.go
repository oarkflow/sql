package etl

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/sql/pkg/utils"
)

// ETLState represents the complete state of an ETL process
type ETLState struct {
	ETLID            string                 `json:"etl_id"`
	Status           string                 `json:"status"`
	StartTime        time.Time              `json:"start_time"`
	LastUpdateTime   time.Time              `json:"last_update_time"`
	LastCheckpoint   string                 `json:"last_checkpoint"`
	ProcessedRecords int64                  `json:"processed_records"`
	FailedRecords    int64                  `json:"failed_records"`
	WorkerStates     map[string]WorkerState `json:"worker_states"`
	NodeProgress     map[string]NodeState   `json:"node_progress"`
	PendingRecords   []utils.Record         `json:"pending_records"`
	ErrorDetails     []ErrorDetail          `json:"error_details"`
	StateVersion     int64                  `json:"state_version"`
	Metadata         map[string]any         `json:"metadata"`
}

// WorkerState represents the state of a specific worker
type WorkerState struct {
	WorkerID      int       `json:"worker_id"`
	NodeName      string    `json:"node_name"`
	LastProcessed string    `json:"last_processed"`
	Processed     int64     `json:"processed"`
	Failed        int64     `json:"failed"`
	LastActivity  time.Time `json:"last_activity"`
	Status        string    `json:"status"`
}

// NodeState represents the state of a processing node
type NodeState struct {
	NodeName         string    `json:"node_name"`
	RecordsProcessed int64     `json:"records_processed"`
	RecordsFailed    int64     `json:"records_failed"`
	LastCheckpoint   string    `json:"last_checkpoint"`
	LastUpdate       time.Time `json:"last_update"`
	Status           string    `json:"status"`
}

// ErrorDetail represents detailed error information
type ErrorDetail struct {
	Timestamp   time.Time `json:"timestamp"`
	NodeName    string    `json:"node_name"`
	WorkerID    int       `json:"worker_id"`
	Error       string    `json:"error"`
	RecordCount int       `json:"record_count"`
	Severity    string    `json:"severity"`
}

// StateManager manages ETL state persistence and recovery
type StateManager struct {
	etlID            string
	stateFile        string
	state            *ETLState
	mutex            sync.RWMutex
	autoSave         bool
	autoSaveInterval time.Duration
	lastSave         time.Time
	stopAutoSave     chan struct{}
}

// NewStateManager creates a new state manager
func NewStateManager(etlID, stateFile string) *StateManager {
	sm := &StateManager{
		etlID:     etlID,
		stateFile: stateFile,
		state: &ETLState{
			ETLID:            etlID,
			Status:           "INITIALIZED",
			StartTime:        time.Now(),
			LastUpdateTime:   time.Now(),
			ProcessedRecords: 0,
			FailedRecords:    0,
			WorkerStates:     make(map[string]WorkerState),
			NodeProgress:     make(map[string]NodeState),
			PendingRecords:   make([]utils.Record, 0),
			ErrorDetails:     make([]ErrorDetail, 0),
			StateVersion:     0,
			Metadata:         make(map[string]any),
		},
		autoSave:         true,
		autoSaveInterval: 30 * time.Second, // Auto-save every 30 seconds
		stopAutoSave:     make(chan struct{}),
	}

	// Try to load existing state
	sm.loadState()

	return sm
}

// StartAutoSave starts the automatic state saving goroutine
func (sm *StateManager) StartAutoSave() {
	if !sm.autoSave {
		return
	}

	go func() {
		ticker := time.NewTicker(sm.autoSaveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := sm.SaveState(); err != nil {
					log.Printf("[StateManager %s] Auto-save error: %v", sm.etlID, err)
				}
			case <-sm.stopAutoSave:
				return
			}
		}
	}()
}

// StopAutoSave stops the automatic state saving
func (sm *StateManager) StopAutoSave() {
	close(sm.stopAutoSave)
}

// SaveState saves the current state to file
func (sm *StateManager) SaveState() error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.state.LastUpdateTime = time.Now()
	sm.state.StateVersion++

	data, err := json.MarshalIndent(sm.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write to temporary file first, then rename for atomicity
	tempFile := sm.stateFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	if err := os.Rename(tempFile, sm.stateFile); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}

	sm.lastSave = time.Now()
	log.Printf("[StateManager %s] State saved (version: %d)", sm.etlID, sm.state.StateVersion)
	return nil
}

// LoadState loads state from file
func (sm *StateManager) LoadState() error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	return sm.loadState()
}

// loadState loads state from file (internal version without locking)
func (sm *StateManager) loadState() error {
	data, err := os.ReadFile(sm.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			// No existing state file, start fresh
			return nil
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	var state ETLState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// Validate that this state belongs to our ETL
	if state.ETLID != sm.etlID {
		return fmt.Errorf("state file belongs to different ETL: %s", state.ETLID)
	}

	sm.state = &state
	log.Printf("[StateManager %s] State loaded (version: %d, status: %s)",
		sm.etlID, state.StateVersion, state.Status)
	return nil
}

// GetState returns a copy of the current state
func (sm *StateManager) GetState() *ETLState {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	// Return a deep copy
	data, _ := json.Marshal(sm.state)
	var state ETLState
	json.Unmarshal(data, &state)
	return &state
}

// UpdateStatus updates the ETL status
func (sm *StateManager) UpdateStatus(status string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.state.Status = status
	sm.state.LastUpdateTime = time.Now()
}

// UpdateCheckpoint updates the last checkpoint
func (sm *StateManager) UpdateCheckpoint(checkpoint string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if checkpoint > sm.state.LastCheckpoint {
		sm.state.LastCheckpoint = checkpoint
		sm.state.LastUpdateTime = time.Now()
	}
}

// AddProcessedRecords adds to the processed records count
func (sm *StateManager) AddProcessedRecords(count int64) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	atomic.AddInt64(&sm.state.ProcessedRecords, count)
	sm.state.LastUpdateTime = time.Now()
}

// AddFailedRecords adds to the failed records count
func (sm *StateManager) AddFailedRecords(count int64) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	atomic.AddInt64(&sm.state.FailedRecords, count)
	sm.state.LastUpdateTime = time.Now()
}

// UpdateWorkerState updates the state of a specific worker
func (sm *StateManager) UpdateWorkerState(nodeName string, workerID int, processed, failed int64, lastProcessed string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	key := fmt.Sprintf("%s:%d", nodeName, workerID)
	sm.state.WorkerStates[key] = WorkerState{
		WorkerID:      workerID,
		NodeName:      nodeName,
		LastProcessed: lastProcessed,
		Processed:     processed,
		Failed:        failed,
		LastActivity:  time.Now(),
		Status:        "ACTIVE",
	}
	sm.state.LastUpdateTime = time.Now()
}

// UpdateNodeState updates the state of a processing node
func (sm *StateManager) UpdateNodeState(nodeName string, recordsProcessed, recordsFailed int64, checkpoint string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.state.NodeProgress[nodeName] = NodeState{
		NodeName:         nodeName,
		RecordsProcessed: recordsProcessed,
		RecordsFailed:    recordsFailed,
		LastCheckpoint:   checkpoint,
		LastUpdate:       time.Now(),
		Status:           "ACTIVE",
	}
	sm.state.LastUpdateTime = time.Now()
}

// AddError adds an error detail
func (sm *StateManager) AddError(nodeName string, workerID int, errorMsg string, recordCount int, severity string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	detail := ErrorDetail{
		Timestamp:   time.Now(),
		NodeName:    nodeName,
		WorkerID:    workerID,
		Error:       errorMsg,
		RecordCount: recordCount,
		Severity:    severity,
	}

	sm.state.ErrorDetails = append(sm.state.ErrorDetails, detail)

	// Keep only the last 1000 errors to prevent memory bloat
	if len(sm.state.ErrorDetails) > 1000 {
		sm.state.ErrorDetails = sm.state.ErrorDetails[len(sm.state.ErrorDetails)-1000:]
	}

	sm.state.LastUpdateTime = time.Now()
}

// AddPendingRecords adds records to the pending queue
func (sm *StateManager) AddPendingRecords(records []utils.Record) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.state.PendingRecords = append(sm.state.PendingRecords, records...)

	// Limit pending records to prevent memory issues
	maxPending := 10000
	if len(sm.state.PendingRecords) > maxPending {
		sm.state.PendingRecords = sm.state.PendingRecords[len(sm.state.PendingRecords)-maxPending:]
	}

	sm.state.LastUpdateTime = time.Now()
}

// GetPendingRecords returns and clears the pending records
func (sm *StateManager) GetPendingRecords() []utils.Record {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	records := make([]utils.Record, len(sm.state.PendingRecords))
	copy(records, sm.state.PendingRecords)
	sm.state.PendingRecords = sm.state.PendingRecords[:0] // Clear the slice

	return records
}

// CanResume checks if the ETL can be resumed from the current state
func (sm *StateManager) CanResume() bool {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	return sm.state.Status == "FAILED" ||
		sm.state.Status == "STOPPED" ||
		sm.state.Status == "PAUSED" ||
		(sm.state.Status == "COMPLETED" && sm.state.FailedRecords > 0)
}

// GetResumeInfo returns information about what can be resumed
func (sm *StateManager) GetResumeInfo() map[string]any {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	info := map[string]any{
		"can_resume":      sm.CanResume(),
		"last_checkpoint": sm.state.LastCheckpoint,
		"processed":       sm.state.ProcessedRecords,
		"failed":          sm.state.FailedRecords,
		"pending_records": len(sm.state.PendingRecords),
		"error_count":     len(sm.state.ErrorDetails),
		"worker_states":   len(sm.state.WorkerStates),
		"node_states":     len(sm.state.NodeProgress),
	}

	return info
}

// ResetState resets the state for a fresh start
func (sm *StateManager) ResetState() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.state = &ETLState{
		ETLID:            sm.etlID,
		Status:           "INITIALIZED",
		StartTime:        time.Now(),
		LastUpdateTime:   time.Now(),
		ProcessedRecords: 0,
		FailedRecords:    0,
		WorkerStates:     make(map[string]WorkerState),
		NodeProgress:     make(map[string]NodeState),
		PendingRecords:   make([]utils.Record, 0),
		ErrorDetails:     make([]ErrorDetail, 0),
		StateVersion:     0,
		Metadata:         make(map[string]any),
	}
}

// SetMetadata sets metadata for the ETL state
func (sm *StateManager) SetMetadata(key string, value any) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.state.Metadata[key] = value
	sm.state.LastUpdateTime = time.Now()
}

// GetMetadata gets metadata from the ETL state
func (sm *StateManager) GetMetadata(key string) (any, bool) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	value, exists := sm.state.Metadata[key]
	return value, exists
}
