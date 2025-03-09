package utils

import (
	"fmt"
	"strings"

	"github.com/oarkflow/convert"
)

type Record = map[string]any

func ApplyAliasToRecord(row Record, alias string) Record {
	newRow := make(Record)
	for k, v := range row {
		newRow[alias+"."+k] = v
	}
	return newRow
}

func CompareValues(a, b any) int {
	af, errA := convert.ToFloat64(a)
	bf, errB := convert.ToFloat64(b)
	if errA && errB {
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		} else {
			return 0
		}
	}
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	return strings.Compare(as, bs)
}

func DeepEqual(a, b Record) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func Union(left, right []Record) []Record {
	m := map[string]Record{}
	for _, row := range left {
		key := fmt.Sprintf("%v", row)
		m[key] = row
	}
	for _, row := range right {
		key := fmt.Sprintf("%v", row)
		m[key] = row
	}
	var result []Record
	for _, row := range m {
		result = append(result, row)
	}
	return result
}

func Intersect(left, right []Record) []Record {
	m := map[string]Record{}
	for _, row := range left {
		key := fmt.Sprintf("%v", row)
		m[key] = row
	}
	var result []Record
	for _, row := range right {
		key := fmt.Sprintf("%v", row)
		if _, ok := m[key]; ok {
			result = append(result, row)
		}
	}
	return result
}

func Except(left, right []Record) []Record {
	m := map[string]bool{}
	for _, row := range right {
		key := fmt.Sprintf("%v", row)
		m[key] = true
	}
	var result []Record
	for _, row := range left {
		key := fmt.Sprintf("%v", row)
		if !m[key] {
			result = append(result, row)
		}
	}
	return result
}

func MergeRows(left, right Record, rightAlias string) Record {
	merged := make(Record)
	if left != nil {
		for k, v := range left {
			merged[k] = v
		}
	}
	if right != nil {
		for k, v := range right {
			if rightAlias != "" {
				merged[rightAlias+"."+k] = v
			} else {
				merged[k] = v
			}
		}
	}
	return merged
}

func RecordKey(left, right Record) string {
	return fmt.Sprintf("%v|%v", left, right)
}
