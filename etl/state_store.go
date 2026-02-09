package etl

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrStateNotFound = errors.New("state not found")
)

// StateStore defines the interface for persisting ETL state
// allowing for different backends (File, Redis, Database, S3)
type StateStore interface {
	Save(ctx context.Context, id string, data []byte) error
	Load(ctx context.Context, id string) ([]byte, error)
	Delete(ctx context.Context, id string) error
	Close() error
}

// FileStateStore implements StateStore using the local filesystem
type FileStateStore struct {
	baseDir string
	mu      sync.Mutex
}

func NewFileStateStore(baseDir string) (*FileStateStore, error) {
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			return nil, err
		}
	}
	return &FileStateStore{baseDir: baseDir}, nil
}

func (s *FileStateStore) Save(ctx context.Context, id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.baseDir, id+".json")
	return os.WriteFile(path, data, 0644)
}

func (s *FileStateStore) Load(ctx context.Context, id string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.baseDir, id+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrStateNotFound
		}
		return nil, err
	}
	return data, nil
}

func (s *FileStateStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.baseDir, id+".json")
	return os.Remove(path)
}

func (s *FileStateStore) Close() error {
	return nil
}

// MemoryStateStore implements StateStore using memory (for testing/single instance)
type MemoryStateStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		data: make(map[string][]byte),
	}
}

func (s *MemoryStateStore) Save(ctx context.Context, id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Copy data to avoid reference issues
	dst := make([]byte, len(data))
	copy(dst, data)
	s.data[id] = dst
	return nil
}

func (s *MemoryStateStore) Load(ctx context.Context, id string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.data[id]
	if !ok {
		return nil, ErrStateNotFound
	}
	// Copy data to avoid external mutation affecting store
	dst := make([]byte, len(data))
	copy(dst, data)
	return dst, nil
}

func (s *MemoryStateStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, id)
	return nil
}

func (s *MemoryStateStore) Close() error {
	return nil
}
