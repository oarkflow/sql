package checkpoint

import (
	"context"
	"os"
	"sync"
)

type FileCheckpointStore struct {
	fileName string
	mu       sync.Mutex
}

func NewFileCheckpointStore(fileName string) *FileCheckpointStore {
	return &FileCheckpointStore{fileName: fileName}
}

func (cs *FileCheckpointStore) SaveCheckpoint(_ context.Context, cp string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return os.WriteFile(cs.fileName, []byte(cp), 0644)
}

func (cs *FileCheckpointStore) GetCheckpoint(context.Context) (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	data, err := os.ReadFile(cs.fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}
