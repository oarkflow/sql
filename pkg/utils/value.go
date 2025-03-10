package utils

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/oarkflow/dipper"
	"github.com/oarkflow/expr"
)

func NormalizeRecord(rec Record, schema map[string]string) (Record, error) {
	for field, targetType := range schema {
		if val, ok := rec[field]; ok {
			normalized, err := normalizeValue(val, targetType)
			if err != nil {
				return nil, fmt.Errorf("error normalizing field %s: %v", field, err)
			}
			rec[field] = normalized
		}
	}
	return rec, nil
}

func normalizeValue(val any, targetType string) (any, error) {
	switch targetType {
	case "int":
		switch v := val.(type) {
		case int:
			return v, nil
		case int64:
			return int(v), nil
		case float64:
			return int(v), nil
		case string:
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			return i, nil
		case bool:
			if v {
				return 1, nil
			}
			return 0, nil
		default:
			return nil, fmt.Errorf("unsupported type for int conversion: %T", val)
		}
	case "bool":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}
			return b, nil
		case int:
			return v != 0, nil
		case float64:
			return v != 0, nil
		default:
			return nil, fmt.Errorf("unsupported type for bool conversion: %T", val)
		}
	case "float":
		switch v := val.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
		default:
			return nil, fmt.Errorf("unsupported type for float conversion: %T", val)
		}
	case "string":
		return fmt.Sprintf("%v", val), nil
	default:
		return nil, fmt.Errorf("unknown target type: %s", targetType)
	}
}

func IsEmpty(s interface{}) bool {
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsZero() {
			return false
		}
	}
	return true
}

func GetDataType(v any) string {
	switch v.(type) {
	case int, int32, int64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "bool"
	case string:
		return "string"
	default:
		return "unknown"
	}
}

func GetValue(c context.Context, v string, data map[string]any) (key string, val any) {
	key, val = getVal(c, v, data)
	if val == nil {
		if strings.Contains(v, "+") {
			vPartsG := strings.Split(v, "+")
			var value []string
			for _, v := range vPartsG {
				key, val = getVal(c, strings.TrimSpace(v), data)
				if val == nil {
					continue
				}
				value = append(value, val.(string))
			}
			val = strings.Join(value, "")
		} else {
			key, val = getVal(c, v, data)
		}
	}

	return
}

func getVal(c context.Context, v string, data map[string]any) (key string, val any) {
	var param, query, consts map[string]any
	var enums map[string]map[string]any
	headerData := make(map[string]any)
	header := c.Value("header")
	switch header := header.(type) {
	case map[string]any:
		if p, exists := header["param"]; exists && p != nil {
			param = p.(map[string]any)
		}
		if p, exists := header["query"]; exists && p != nil {
			query = p.(map[string]any)
		}
		if p, exists := header["consts"]; exists && p != nil {
			consts = p.(map[string]any)
		}
		if p, exists := header["enums"]; exists && p != nil {
			enums = p.(map[string]map[string]any)
		}
		params := []string{"param", "query", "consts", "enums", "scopes"}
		// add other data in header, other than param, query, consts, enums to data
		for k, v := range header {
			if !slices.Contains(params, k) {
				headerData[k] = v
			}
		}
	}
	v = strings.TrimPrefix(v, "header.")
	vParts := strings.Split(v, ".")
	switch vParts[0] {
	case "body":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range data {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := data[v]; ok {
				val = vd
				key = v
			}
		}
	case "param":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range param {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := param[v]; ok {
				val = vd
				key = v
			}
		}
	case "query":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range query {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := query[v]; ok {
				val = vd
				key = v
			}
		}
	case "eval":
		// connect string except the first one if more than two parts exist
		var v string
		if len(vParts) > 2 {
			v = strings.Join(vParts[1:], ".")
		} else {
			v = vParts[1]
		}
		// remove '{{' and '}}'
		v = v[2 : len(v)-2]

		// parse the expression
		p, err := expr.Parse(v)
		if err != nil {
			return "", nil
		}
		// evaluate the expression
		val, err := p.Eval(data)
		if err != nil {
			val, err := p.Eval(headerData)
			if err == nil {
				return v, val
			}
			return "", nil
		} else {
			return v, val
		}
	case "eval_raw", "gorm_eval":
		// connect string except the first one if more than two parts exist
		var v string
		if len(vParts) > 2 {
			v = strings.Join(vParts[1:], ".")
		} else {
			v = vParts[1]
		}
		// remove '{{' and '}}'
		v = v[2 : len(v)-2]

		// parse the expression
		p, err := expr.Parse(v)
		if err != nil {
			return "", nil
		}
		dt := map[string]any{
			"header": header,
		}
		for k, vt := range data {
			dt[k] = vt
		}
		// evaluate the expression
		val, err := p.Eval(dt)
		if err != nil {
			val, err := p.Eval(headerData)
			if err == nil {
				return v, val
			}
			return "", nil
		} else {
			return v, val
		}
	case "consts":
		constG := vParts[1]
		if constVal, ok := consts[constG]; ok {
			val = constVal
			key = v
		}
	case "enums":
		enumG := vParts[1]
		if enumGVal, ok := enums[enumG]; ok {
			if enumVal, ok := enumGVal[vParts[2]]; ok {
				val = enumVal
				key = v
			}
		}
	default:
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range data {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			vd, err := dipper.Get(data, v)
			if err == nil {
				val = vd
				key = v
			} else {
				vd, err := dipper.Get(headerData, v)
				if err == nil {
					val = vd
					key = v
				}
			}
		}
	}
	return
}
