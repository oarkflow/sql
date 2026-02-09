package etl

import (
	"time"

	"github.com/oarkflow/sql/pkg/utils"
)

// NewStateManagerWithStore creates a manager directly with a store
func NewStateManagerWithStore(etlID string, store StateStore) *StateManager {
	sm := &StateManager{
		etlID:            etlID,
		store:            store,
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
		autoSaveInterval: 30 * time.Second,
		stopAutoSave:     make(chan struct{}),
	}

	sm.loadState()
	if sm.autoSave {
		go sm.StartAutoSave()
	}
	return sm
}
