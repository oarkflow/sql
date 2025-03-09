package source

import (
	"context"
	"log"

	"github.com/oarkflow/sql/utils"
	"github.com/oarkflow/sql/utils/fileutil"
)

type FileSource struct {
	Filename string
}

func (fs *FileSource) Close() error {
	return nil
}

func (fs *FileSource) Setup(_ context.Context) error {
	return nil
}

func NewFileSource(filename string) *FileSource {
	return &FileSource{Filename: filename}
}

func (fs *FileSource) Extract(_ context.Context) (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		_, err := fileutil.ProcessFile(fs.Filename, func(record utils.Record) {
			out <- record
		})
		if err != nil {
			log.Printf("File extraction error: %v", err)
		}
	}()
	return out, nil
}
