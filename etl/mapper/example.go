package mapper

import (
	"github.com/chand1012/sq/utils"
)

type ExampleMapper struct{}

func (m *ExampleMapper) Map(rec utils.Record) (utils.Record, error) {
	if val, exists := rec["oldName"]; exists {
		rec["newName"] = val
		delete(rec, "oldName")
	}
	return rec, nil
}
