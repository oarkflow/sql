package source

import (
	"log"

	"github.com/chand1012/sq/utils"
)

type FileSource struct {
	Filename string
}

func (fs *FileSource) Close() error {
	return nil
}

func (fs *FileSource) Setup() error {
	return nil
}

func NewFileSource(filename string) *FileSource {
	return &FileSource{Filename: filename}
}

func (fs *FileSource) Extract() (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		_, err := utils.ProcessFile(fs.Filename, func(record utils.Record) {
			out <- record
		})
		if err != nil {
			log.Printf("File extraction error: %v", err)
		}
	}()
	return out, nil
}
