package mapper

import (
	"time"

	"github.com/chand1012/sq/utils"
)

type AddTimestampMapper struct{}

func (m *AddTimestampMapper) Map(rec utils.Record) (utils.Record, error) {
	rec["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	return rec, nil
}
