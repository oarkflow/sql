package hl7adapter

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/oarkflow/log"

	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

// FileSourceOption customizes HL7 file source behaviour.
type FileSourceOption func(*FileSource)

// WithBlankLineSplit toggles whether blank lines delimit messages.
func WithBlankLineSplit(enabled bool) FileSourceOption {
	return func(fs *FileSource) {
		fs.splitOnBlankLine = enabled
	}
}

// FileSource streams HL7 messages from a file, emitting full messages per record.
type FileSource struct {
	path             string
	splitOnBlankLine bool
}

// NewFileSource builds a FileSource with optional behaviour tweaks.
func NewFileSource(path string, opts ...FileSourceOption) *FileSource {
	fs := &FileSource{
		path:             path,
		splitOnBlankLine: true,
	}
	for _, opt := range opts {
		opt(fs)
	}
	return fs
}

// Setup validates the source file exists.
func (fs *FileSource) Setup(_ context.Context) error {
	if fs.path == "" {
		return fmt.Errorf("hl7 file source: path is empty")
	}
	_, err := os.Stat(fs.path)
	return err
}

// Extract streams HL7 messages as utils.Record objects.
func (fs *FileSource) Extract(ctx context.Context, _ ...contracts.Option) (<-chan utils.Record, error) {
	file, err := os.Open(fs.path)
	if err != nil {
		return nil, err
	}

	out := make(chan utils.Record)
	go func() {
		defer close(out)
		defer file.Close()

		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 128*1024)
		scanner.Buffer(buf, 4*1024*1024)
		var builder strings.Builder

		flush := func() {
			if builder.Len() == 0 {
				return
			}
			message := builder.String()
			builder.Reset()
			select {
			case <-ctx.Done():
				return
			case out <- utils.Record{
				"raw_message": message,
				"source_path": fs.path,
			}:
			}
		}

		for scanner.Scan() {
			line := scanner.Text()
			trimmed := strings.TrimSpace(line)
			if fs.splitOnBlankLine && trimmed == "" {
				flush()
				continue
			}
			if strings.HasPrefix(line, "MSH") && builder.Len() > 0 {
				flush()
			}
			if builder.Len() > 0 {
				builder.WriteString("\r")
			}
			builder.WriteString(line)
		}
		flush()

		if err := scanner.Err(); err != nil {
			log.Printf("hl7 file source scan error: %v", err)
		}
	}()

	return out, nil
}

// Close implements contracts.Source.
func (fs *FileSource) Close() error {
	return nil
}
