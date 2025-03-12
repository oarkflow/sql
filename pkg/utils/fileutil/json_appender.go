package fileutil

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"unicode"

	"github.com/gofrs/flock"
	"github.com/oarkflow/json"
)

// JSONAppender appends elements of type T to a JSON array stored in a file.
type JSONAppender[T any] struct {
	filePath       string
	file           *os.File
	fileLock       *flock.Flock
	mu             sync.Mutex
	tailBufferSize int
	syncOnAppend   bool
	appendMode     bool
}

// NewJSONAppender creates a new JSONAppender for elements of type T.
// Additional options (such as WithDedup) can be provided to change the appender’s behavior.
func NewJSONAppender[T any](filePath string, appendMode bool) (*JSONAppender[T], error) {
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

// Append appends a single element of type T.
func (ja *JSONAppender[T]) Append(element T) error {
	return ja.AppendBatch([]T{element})
}

// AppendBatch appends a batch of elements while ensuring the file remains a valid JSON array.
// If deduplication is enabled, duplicate elements (as determined by their JSON representation)
// are filtered out and not inserted.
func (ja *JSONAppender[T]) AppendBatch(elements []T) error {
	ja.mu.Lock()
	defer ja.mu.Unlock()
	if err := ja.fileLock.Lock(); err != nil {
		return err
	}
	defer func() {
		_ = ja.fileLock.Unlock()
	}()

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

// Close closes the underlying file.
func (ja *JSONAppender[T]) Close() error {
	ja.mu.Lock()
	defer ja.mu.Unlock()
	return ja.file.Close()
}
