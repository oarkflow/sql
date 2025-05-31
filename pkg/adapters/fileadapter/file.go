package fileadapter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/oarkflow/log"

	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
	"github.com/oarkflow/sql/pkg/utils/fileutil"
)

type Adapter struct {
	mode            string
	Filename        string
	extension       string
	appendMode      bool
	file            *os.File
	jsonFirstRecord bool
	csvHeader       []string
	appender        contracts.Appender[utils.Record]
	headerWritten   bool
}

func New(fileName, mode string, appendMode bool) *Adapter {
	extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
	return &Adapter{
		Filename:        fileName,
		extension:       extension,
		appendMode:      appendMode,
		mode:            mode,
		jsonFirstRecord: true,
	}
}

func (fl *Adapter) Setup(_ context.Context) error {
	if fl.mode != "loader" {
		_, err := os.Stat(fl.Filename)
		return err
	}
	appender, err := fileutil.NewAppender[utils.Record](fl.Filename, fl.extension, fl.appendMode)
	if err != nil {
		return err
	}
	fl.appender = appender
	return nil
}

func (fl *Adapter) StoreBatch(_ context.Context, records []utils.Record) error {
	switch fl.extension {
	case "csv", "json":
		err := fl.appender.AppendBatch(records)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported file extension: %s", fl.extension)
	}
	return nil
}

func (fl *Adapter) Close() error {
	if fl.appender != nil {
		err := fl.appender.Close()
		if err != nil {
			return err
		}
	}
	if fl.file != nil {
		return fl.file.Close()
	}
	return nil
}

func (fl *Adapter) LoadData(_ ...contracts.Option) ([]utils.Record, error) {
	ch, err := fl.Extract(context.Background())
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (fl *Adapter) Extract(_ context.Context, _ ...contracts.Option) (<-chan utils.Record, error) {
	out := make(chan utils.Record)
	go func() {
		defer close(out)
		_, err := fileutil.ProcessFile(fl.Filename, func(record utils.Record) {
			out <- record
		})
		if err != nil {
			log.Printf("File extraction error: %v", err)
		}
	}()
	return out, nil
}
