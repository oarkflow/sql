package etl

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/oarkflow/sql/pkg/utils"
)

// IdempotencyKey represents a key for tracking processed records
type IdempotencyKey struct {
	Key       string    `json:"key"`
	Hash      string    `json:"hash"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	Count     int64     `json:"count"`
	Status    string    `json:"status"`
}

// IdempotencyManager manages record processing idempotency
type IdempotencyManager struct {
	cache         *ristretto.Cache
	persistFile   string
	keys          map[string]*IdempotencyKey
	mutex         sync.RWMutex
	ttl           time.Duration
	maxKeys       int
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// NewIdempotencyManager creates a new idempotency manager
func NewIdempotencyManager(persistFile string, ttl time.Duration, maxKeys int) *IdempotencyManager {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(maxKeys * 10),
		MaxCost:     int64(maxKeys),
		BufferItems: 64,
	})
	if err != nil {
		log.Printf("[Idempotency] Failed to create cache: %v", err)
		return nil
	}

	im := &IdempotencyManager{
		cache:       cache,
		persistFile: persistFile,
		keys:        make(map[string]*IdempotencyKey),
		ttl:         ttl,
		maxKeys:     maxKeys,
		stopCleanup: make(chan struct{}),
	}

	// Load existing keys
	im.loadKeys()

	// Start cleanup routine
	im.startCleanup()

	return im
}

// GenerateKey generates an idempotency key for a record
func (im *IdempotencyManager) GenerateKey(record utils.Record, fields []string) string {
	if len(fields) == 0 {
		// Use all fields for key generation
		return im.hashRecord(record)
	}

	// Use specified fields for key generation
	keyData := make(utils.Record)
	for _, field := range fields {
		if value, exists := record[field]; exists {
			keyData[field] = value
		}
	}

	return im.hashRecord(keyData)
}

// hashRecord creates a hash of the record for idempotency checking
func (im *IdempotencyManager) hashRecord(record utils.Record) string {
	hash := md5.New()
	for key, value := range record {
		fmt.Fprintf(hash, "%s=%v|", key, value)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// IsProcessed checks if a record has already been processed
func (im *IdempotencyManager) IsProcessed(key string) (bool, error) {
	// Check cache first
	if processed, found := im.cache.Get(key); found {
		return processed.(bool), nil
	}

	// Check persistent storage
	im.mutex.RLock()
	idempKey, exists := im.keys[key]
	im.mutex.RUnlock()

	if !exists {
		return false, nil
	}

	// Check if key has expired
	if time.Since(idempKey.LastSeen) > im.ttl {
		im.mutex.Lock()
		delete(im.keys, key)
		im.mutex.Unlock()
		return false, nil
	}

	processed := idempKey.Status == "COMPLETED"
	im.cache.Set(key, processed, 1)
	return processed, nil
}

// MarkProcessing marks a record as being processed
func (im *IdempotencyManager) MarkProcessing(key string) error {
	now := time.Now()

	im.mutex.Lock()
	defer im.mutex.Unlock()

	idempKey, exists := im.keys[key]
	if !exists {
		idempKey = &IdempotencyKey{
			Key:       key,
			FirstSeen: now,
			Status:    "PROCESSING",
		}
		im.keys[key] = idempKey
	}

	idempKey.LastSeen = now
	idempKey.Count++
	idempKey.Status = "PROCESSING"

	// Add to cache
	im.cache.Set(key, false, 1) // Not processed yet, but being processed

	// Check if we need to evict old keys
	if len(im.keys) > im.maxKeys {
		im.evictOldKeys()
	}

	return im.saveKeys()
}

// MarkCompleted marks a record as successfully processed
func (im *IdempotencyManager) MarkCompleted(key string) error {
	now := time.Now()

	im.mutex.Lock()
	defer im.mutex.Unlock()

	idempKey, exists := im.keys[key]
	if !exists {
		idempKey = &IdempotencyKey{
			Key:       key,
			FirstSeen: now,
			Status:    "COMPLETED",
		}
		im.keys[key] = idempKey
	} else {
		idempKey.LastSeen = now
		idempKey.Status = "COMPLETED"
	}

	// Add to cache
	im.cache.Set(key, true, 1)

	return im.saveKeys()
}

// MarkFailed marks a record as failed (can be retried)
func (im *IdempotencyManager) MarkFailed(key string) error {
	now := time.Now()

	im.mutex.Lock()
	defer im.mutex.Unlock()

	idempKey, exists := im.keys[key]
	if !exists {
		idempKey = &IdempotencyKey{
			Key:       key,
			FirstSeen: now,
			Status:    "FAILED",
		}
		im.keys[key] = idempKey
	} else {
		idempKey.LastSeen = now
		idempKey.Status = "FAILED"
	}

	// Add to cache
	im.cache.Set(key, false, 1)

	return im.saveKeys()
}

// GetKeyInfo returns information about a specific key
func (im *IdempotencyManager) GetKeyInfo(key string) (*IdempotencyKey, bool) {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	idempKey, exists := im.keys[key]
	if !exists {
		return nil, false
	}

	// Check if key has expired
	if time.Since(idempKey.LastSeen) > im.ttl {
		return nil, false
	}

	// Return a copy
	keyCopy := *idempKey
	return &keyCopy, true
}

// GetStats returns statistics about the idempotency manager
func (im *IdempotencyManager) GetStats() map[string]any {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	processing := 0
	completed := 0
	failed := 0

	for _, key := range im.keys {
		switch key.Status {
		case "PROCESSING":
			processing++
		case "COMPLETED":
			completed++
		case "FAILED":
			failed++
		}
	}

	return map[string]any{
		"total_keys":  len(im.keys),
		"processing":  processing,
		"completed":   completed,
		"failed":      failed,
		"cache_size":  im.cache.MaxCost,
		"max_keys":    im.maxKeys,
		"ttl_seconds": int(im.ttl.Seconds()),
	}
}

// CleanupExpired removes expired keys
func (im *IdempotencyManager) CleanupExpired() int {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	now := time.Now()
	expired := 0
	for key, idempKey := range im.keys {
		if now.Sub(idempKey.LastSeen) > im.ttl {
			delete(im.keys, key)
			im.cache.Del(key)
			expired++
		}
	}

	if expired > 0 {
		log.Printf("[Idempotency] Cleaned up %d expired keys", expired)
		im.saveKeys()
	}

	return expired
}

// evictOldKeys removes old keys when we exceed max capacity
func (im *IdempotencyManager) evictOldKeys() {
	// Simple LRU eviction - remove oldest keys first
	type keyTime struct {
		key  string
		time time.Time
	}

	var keysToEvict []keyTime
	for key, idempKey := range im.keys {
		keysToEvict = append(keysToEvict, keyTime{key: key, time: idempKey.LastSeen})
	}

	// Sort by time (oldest first) - simple bubble sort for small datasets
	for i := 0; i < len(keysToEvict); i++ {
		for j := i + 1; j < len(keysToEvict); j++ {
			if keysToEvict[i].time.After(keysToEvict[j].time) {
				keysToEvict[i], keysToEvict[j] = keysToEvict[j], keysToEvict[i]
			}
		}
	}

	// Evict oldest keys
	evictCount := len(im.keys) - im.maxKeys + 100 // Evict extra to prevent frequent evictions
	if evictCount > 0 {
		for i := 0; i < evictCount && i < len(keysToEvict); i++ {
			key := keysToEvict[i].key
			delete(im.keys, key)
			im.cache.Del(key)
		}
		log.Printf("[Idempotency] Evicted %d old keys", evictCount)
	}
}

// startCleanup starts the cleanup routine
func (im *IdempotencyManager) startCleanup() {
	im.cleanupTicker = time.NewTicker(5 * time.Minute) // Cleanup every 5 minutes

	go func() {
		for {
			select {
			case <-im.cleanupTicker.C:
				im.CleanupExpired()
			case <-im.stopCleanup:
				im.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// Stop stops the cleanup routine
func (im *IdempotencyManager) Stop() {
	close(im.stopCleanup)
}

// saveKeys saves keys to persistent storage
func (im *IdempotencyManager) saveKeys() error {
	// Implementation would save to file or database
	// For now, we'll just keep in memory
	return nil
}

// loadKeys loads keys from persistent storage
func (im *IdempotencyManager) loadKeys() error {
	// Implementation would load from file or database
	// For now, we'll start with empty keys
	return nil
}

// ProcessWithIdempotency processes a record with idempotency checking
func (im *IdempotencyManager) ProcessWithIdempotency(ctx context.Context, record utils.Record, keyFields []string, processor func(utils.Record) error) error {
	key := im.GenerateKey(record, keyFields)

	// Check if already processed
	processed, err := im.IsProcessed(key)
	if err != nil {
		return fmt.Errorf("failed to check processing status: %w", err)
	}

	if processed {
		log.Printf("[Idempotency] Record already processed: %s", key[:8])
		return nil // Already processed, skip
	}

	// Mark as processing
	if err := im.MarkProcessing(key); err != nil {
		return fmt.Errorf("failed to mark as processing: %w", err)
	}

	// Process the record
	if err := processor(record); err != nil {
		// Mark as failed
		im.MarkFailed(key)
		return fmt.Errorf("processing failed: %w", err)
	}

	// Mark as completed
	return im.MarkCompleted(key)
}
