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

type JSONAppender struct {
	filePath       string
	file           *os.File
	fileLock       *flock.Flock
	mu             sync.Mutex
	tailBufferSize int
	syncOnAppend   bool
}

func NewJSONAppender(filePath string) (*JSONAppender, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ja := &JSONAppender{
		filePath:       filePath,
		file:           f,
		fileLock:       flock.New(filePath + ".lock"),
		tailBufferSize: 1024,
		syncOnAppend:   true,
	}
	if err := ja.validateOrInitialize(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return ja, nil
}

func (ja *JSONAppender) validateOrInitialize() error {
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

func (ja *JSONAppender) Append(element any) error {

	return ja.AppendBatch([]any{element})
}

func (ja *JSONAppender) AppendBatch(elements ...any) error {
	if len(elements) == 0 {
		return nil
	}

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

	if fi.Size() == 0 {
		var buf bytes.Buffer
		buf.WriteString("[\n  ")
		for i, element := range elements {
			b, err := json.Marshal(element)
			if err != nil {
				return err
			}
			if i > 0 {
				buf.WriteString(",\n  ")
			}
			buf.Write(b)
		}
		buf.WriteString("\n]\n")
		if err := ja.file.Truncate(0); err != nil {
			return err
		}
		if _, err := ja.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := ja.file.Write(buf.Bytes()); err != nil {
			return err
		}
		if ja.syncOnAppend {
			return ja.file.Sync()
		}
		return nil
	}

	tailSize := int64(ja.tailBufferSize)
	if fi.Size() < tailSize {
		tailSize = fi.Size()
	}
	offset := fi.Size() - tailSize
	bufTail := make([]byte, tailSize)
	if _, err := ja.file.ReadAt(bufTail, offset); err != nil && err != io.EOF {
		return err
	}
	lastBracketIndex := bytes.LastIndexByte(bufTail, ']')
	if lastBracketIndex == -1 {
		return errors.New("invalid JSON file: missing closing bracket")
	}

	pos := lastBracketIndex - 1
	for pos >= 0 {
		if !unicode.IsSpace(rune(bufTail[pos])) {
			break
		}
		pos--
	}
	if pos < 0 {
		return errors.New("invalid JSON file: unable to find content before closing bracket")
	}
	lastNonSpace := bufTail[pos]

	newTruncationPos := offset + int64(pos) + 1
	if err := ja.file.Truncate(newTruncationPos); err != nil {
		return err
	}
	if _, err := ja.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	var batchData bytes.Buffer

	if lastNonSpace != '[' {
		batchData.WriteString(",\n  ")
	} else {
		batchData.WriteString("\n  ")
	}
	for i, element := range elements {
		b, err := json.Marshal(element)
		if err != nil {
			return err
		}
		if i > 0 {
			batchData.WriteString(",\n  ")
		}
		batchData.Write(b)
	}

	batchData.WriteString("\n]\n")
	if _, err := ja.file.Write(batchData.Bytes()); err != nil {
		return err
	}
	if ja.syncOnAppend {
		return ja.file.Sync()
	}
	return nil
}

func (ja *JSONAppender) Close() error {
	ja.mu.Lock()
	defer ja.mu.Unlock()
	return ja.file.Close()
}
