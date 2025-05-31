package fileutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"unicode"

	"github.com/gofrs/flock"
)

// JSONAppender appends elements of type T to a JSON array stored in a file.
type JSONAppender[T any] struct {
	filePath       string
	file           *os.File
	fileLock       *flock.Flock
	mu             sync.Mutex // protects file operations
	tailBufferSize int
	syncOnAppend   bool
	appendMode     bool

	// Batch buffering fields.
	batchSize int        // predefined batch size
	pending   []T        // pending elements waiting to be flushed
	batchMu   sync.Mutex // protects the pending slice
}

// NewJSONAppender creates a new JSONAppender for elements of type T.
// The batchSize parameter defines how many items are accumulated before a flush.
func NewJSONAppender[T any](filePath string, appendMode bool, batchSize int) (*JSONAppender[T], error) {
	if batchSize <= 0 {
		return nil, errors.New("batchSize must be greater than zero")
	}
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ja := &JSONAppender[T]{
		filePath:       filePath,
		file:           f,
		fileLock:       flock.New(filePath + ".lock"),
		tailBufferSize: 1024,
		syncOnAppend:   true,
		appendMode:     appendMode,
		batchSize:      batchSize,
		pending:        make([]T, 0, batchSize),
	}
	if err := ja.validateOrInitialize(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return ja, nil
}

// validateOrInitialize checks that the file contains a valid JSON array, or initializes it.
func (ja *JSONAppender[T]) validateOrInitialize() error {
	fi, err := ja.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		content := []byte("[\n]\n")
		if err := ja.file.Truncate(0); err != nil {
			return err
		}
		if _, err := ja.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := ja.file.Write(content); err != nil {
			return err
		}
		if ja.syncOnAppend {
			return ja.file.Sync()
		}
		return nil
	}
	head := make([]byte, 10)
	if _, err := ja.file.ReadAt(head, 0); err != nil && err != io.EOF {
		return err
	}
	if !bytes.Contains(head, []byte("[")) {
		return errors.New("invalid JSON file: missing opening bracket")
	}
	size := fi.Size()
	tailSize := int64(10)
	if size < tailSize {
		tailSize = size
	}
	tailBuf := make([]byte, tailSize)
	if _, err := ja.file.ReadAt(tailBuf, size-tailSize); err != nil && err != io.EOF {
		return err
	}
	if !bytes.Contains(tailBuf, []byte("]")) {
		return errors.New("invalid JSON file: missing closing bracket")
	}
	return nil
}

// Append appends a single element of type T, accumulating in a batch.
func (ja *JSONAppender[T]) Append(element T) error {
	ja.batchMu.Lock()
	ja.pending = append(ja.pending, element)
	if len(ja.pending) >= ja.batchSize {
		batch := make([]T, len(ja.pending))
		copy(batch, ja.pending)
		ja.pending = nil
		ja.batchMu.Unlock()
		if err := ja.AppendBatch(batch); err != nil {
			ja.batchMu.Lock()
			ja.pending = append(batch, ja.pending...)
			ja.batchMu.Unlock()
			return err
		}
		return nil
	}
	ja.batchMu.Unlock()
	return nil
}

// AppendBatch appends a batch of elements while ensuring the file remains a valid JSON array.
func (ja *JSONAppender[T]) AppendBatch(elements []T) error {
	ja.mu.Lock()
	defer ja.mu.Unlock()
	if err := ja.fileLock.Lock(); err != nil {
		return err
	}
	defer func() { _ = ja.fileLock.Unlock() }()

	fi, err := ja.file.Stat()
	if err != nil {
		return err
	}
	// If file is empty, write a new JSON array.
	if fi.Size() == 0 {
		content := []byte("[\n  ")
		for i, element := range elements {
			elementBytes, err := json.Marshal(element)
			if err != nil {
				return err
			}
			if i > 0 {
				content = append(content, []byte(",\n  ")...)
			}
			content = append(content, elementBytes...)
		}
		content = append(content, []byte("\n]\n")...)
		if err := ja.file.Truncate(0); err != nil {
			return err
		}
		if _, err := ja.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := ja.file.Write(content); err != nil {
			return err
		}
		if ja.syncOnAppend {
			return ja.file.Sync()
		}
		return nil
	}

	// Read the tail of the file to locate the last closing bracket.
	tailSize := int64(ja.tailBufferSize)
	if fi.Size() < tailSize {
		tailSize = fi.Size()
	}
	offset := fi.Size() - tailSize
	buf := make([]byte, tailSize)
	if _, err := ja.file.ReadAt(buf, offset); err != nil && err != io.EOF {
		return err
	}
	lastBracketIndex := bytes.LastIndexByte(buf, ']')
	if lastBracketIndex == -1 {
		return errors.New("invalid JSON file: missing closing bracket")
	}
	// Walk backwards from the last bracket to skip any whitespace.
	pos := lastBracketIndex - 1
	for pos >= 0 {
		if !unicode.IsSpace(rune(buf[pos])) {
			break
		}
		pos--
	}
	if pos < 0 {
		return errors.New("invalid JSON file: unable to find content before closing bracket")
	}
	lastNonSpace := buf[pos]
	newTruncationPos := offset + int64(pos) + 1
	if err := ja.file.Truncate(newTruncationPos); err != nil {
		return err
	}
	if _, err := ja.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	// Determine whether to prepend a comma.
	var prefix []byte
	if lastNonSpace == '[' {
		prefix = []byte("\n  ")
	} else {
		prefix = []byte(",\n  ")
	}
	dataToWrite := prefix
	for i, element := range elements {
		elementBytes, err := json.Marshal(element)
		if err != nil {
			return err
		}
		if i > 0 {
			dataToWrite = append(dataToWrite, []byte(",\n  ")...)
		}
		dataToWrite = append(dataToWrite, elementBytes...)
	}
	dataToWrite = append(dataToWrite, []byte("\n]\n")...)
	if _, err := ja.file.Write(dataToWrite); err != nil {
		return err
	}
	if ja.syncOnAppend {
		return ja.file.Sync()
	}
	return nil
}

// DeleteRecord reads all records, removes those for which predicate returns true,
// and rewrites the file.
func (ja *JSONAppender[T]) DeleteRecord(predicate func(T) bool) error {
	// Flush any pending batch.
	ja.batchMu.Lock()
	pendingBatch := make([]T, len(ja.pending))
	copy(pendingBatch, ja.pending)
	ja.pending = nil
	ja.batchMu.Unlock()
	if len(pendingBatch) > 0 {
		if err := ja.AppendBatch(pendingBatch); err != nil {
			return err
		}
	}

	ja.mu.Lock()
	defer ja.mu.Unlock()
	if err := ja.fileLock.Lock(); err != nil {
		return err
	}
	defer ja.fileLock.Unlock()

	// Read entire file.
	data, err := os.ReadFile(ja.filePath)
	if err != nil {
		return err
	}
	var records []T
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	// Filter out records matching predicate.
	filtered := make([]T, 0, len(records))
	for _, r := range records {
		if !predicate(r) {
			filtered = append(filtered, r)
		}
	}
	// Rewrite file.
	f, err := os.Create(ja.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(filtered); err != nil {
		return err
	}
	if ja.syncOnAppend {
		return f.Sync()
	}
	return nil
}

// SearchRecords returns all records for which predicate returns true.
func (ja *JSONAppender[T]) SearchRecords(predicate func(T) bool) ([]T, error) {
	// Flush pending batch.
	ja.batchMu.Lock()
	pendingBatch := make([]T, len(ja.pending))
	copy(pendingBatch, ja.pending)
	ja.pending = nil
	ja.batchMu.Unlock()
	if len(pendingBatch) > 0 {
		if err := ja.AppendBatch(pendingBatch); err != nil {
			return nil, err
		}
	}

	ja.mu.Lock()
	defer ja.mu.Unlock()
	data, err := os.ReadFile(ja.filePath)
	if err != nil {
		return nil, err
	}
	var records []T
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	var result []T
	for _, r := range records {
		if predicate(r) {
			result = append(result, r)
		}
	}
	return result, nil
}

// ForEach iterates over each record in the file (after flushing pending records)
// and calls the provided callback.
func (ja *JSONAppender[T]) ForEach(callback func(T)) error {
	// Flush pending records.
	ja.batchMu.Lock()
	pendingBatch := make([]T, len(ja.pending))
	copy(pendingBatch, ja.pending)
	ja.pending = nil
	ja.batchMu.Unlock()
	if len(pendingBatch) > 0 {
		if err := ja.AppendBatch(pendingBatch); err != nil {
			return err
		}
	}

	ja.mu.Lock()
	defer ja.mu.Unlock()
	data, err := os.ReadFile(ja.filePath)
	if err != nil {
		return err
	}
	var records []T
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	for _, r := range records {
		callback(r)
	}
	return nil
}

// Close flushes pending records and closes the underlying file.
func (ja *JSONAppender[T]) Close() error {
	ja.batchMu.Lock()
	batch := make([]T, len(ja.pending))
	copy(batch, ja.pending)
	ja.pending = nil
	ja.batchMu.Unlock()
	if len(batch) > 0 {
		if err := ja.AppendBatch(batch); err != nil {
			return err
		}
	}
	ja.mu.Lock()
	defer func() {
		ja.mu.Unlock()
		_ = os.Remove(ja.fileLock.Path())
	}()
	return ja.file.Close()
}
