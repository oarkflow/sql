package checkpoints

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type FileCheckpointStore struct {
	filename   string
	mu         sync.Mutex
	checkpoint string
}

func NewFileCheckpointStore(filename string) *FileCheckpointStore {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	ext := filepath.Ext(filename)
	baseName := filename[:len(filename)-len(ext)]
	newFilename := fmt.Sprintf("%s-%s%s", baseName, timestamp, ext)
	store := &FileCheckpointStore{filename: newFilename}
	data, err := os.ReadFile(filename)
	if err == nil {
		store.checkpoint = string(data)
	}
	return store
}

func (f *FileCheckpointStore) GetCheckpoint(_ context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoint, nil
}

func (f *FileCheckpointStore) SaveCheckpoint(_ context.Context, cp string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cp <= f.checkpoint {
		return nil
	}
	if err := os.WriteFile(f.filename, []byte(cp), 0644); err != nil {
		return fmt.Errorf("failed to write checkpoints: %w", err)
	}
	f.checkpoint = cp
	return nil
}
