package utils

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	convert "github.com/oarkflow/convert/v2"
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
	if val == nil {
		return nil, nil
	}
	if idx := strings.Index(targetType, "("); idx != -1 {
		targetType = strings.TrimSpace(targetType[:idx])
	} else {
		targetType = strings.TrimSpace(targetType)
	}

	switch strings.ToLower(targetType) {
	// Integer types
	case "int", "integer":
		return convert.ToInt(val)
	case "int8":
		return convert.ToInt8(val)
	case "int16":
		return convert.ToInt16(val)
	case "int32":
		return convert.ToInt32(val)
	case "int64":
		return convert.ToInt64(val)
	case "uint":
		return convert.ToUint(val)
	case "uint8", "byte":
		return convert.ToUint8(val)
	case "uint16":
		return convert.ToUint16(val)
	case "uint32":
		return convert.ToUint32(val)
	case "uint64":
		return convert.ToUint64(val)
	case "rune":
		return convert.ToInt32(val) // rune is int32

	// Boolean types
	case "bool", "boolean":
		return convert.ToBool(val)

	// Float types
	case "float":
		return convert.ToFloat32(val)
	case "float32":
		return convert.ToFloat32(val)
	case "float64":
		return convert.ToFloat64(val)

	// String types
	case "string", "text", "varchar", "char":
		return convert.ToString(val)

	// Time types
	case "datetime", "date", "date-time", "timestamp", "time", "timestamp with time zone", "time with time zone":
		return convert.ToTime(val)

	// SQL-specific types (map to appropriate Go types)
	case "smallint":
		return convert.ToInt16(val)
	case "mediumint":
		return convert.ToInt32(val)
	case "bigint":
		return convert.ToInt64(val)
	case "tinyint":
		return convert.ToInt8(val)
	case "decimal", "numeric":
		return convert.ToFloat64(val)
	case "real":
		return convert.ToFloat32(val)
	case "double", "double precision":
		return convert.ToFloat64(val)
	case "json", "jsonb":
		return convert.ToString(val) // JSON as string
	case "uuid":
		return convert.ToString(val)
	case "blob", "bytea", "tinyblob", "mediumblob", "longblob":
		return val, nil // Keep as is for binary data
	case "year":
		return convert.ToInt(val)
	case "serial":
		return convert.ToInt(val)
	case "bigserial":
		return convert.ToInt64(val)
	case "smallserial":
		return convert.ToInt16(val)
	case "enum":
		return convert.ToString(val)
	case "set":
		return convert.ToString(val) // SET as comma-separated string
	case "interval":
		return convert.ToString(val) // Interval as string
	case "bit":
		return convert.ToString(val) // Bit as string
	case "varbit":
		return convert.ToString(val) // Variable bit as string
	case "macaddr":
		return convert.ToString(val)
	case "inet":
		return convert.ToString(val)
	case "cidr":
		return convert.ToString(val)
	case "point":
		return convert.ToString(val)
	case "line":
		return convert.ToString(val)
	case "lseg":
		return convert.ToString(val)
	case "box":
		return convert.ToString(val)
	case "path":
		return convert.ToString(val)
	case "polygon":
		return convert.ToString(val)
	case "circle":
		return convert.ToString(val)
	case "tsvector":
		return convert.ToString(val)
	case "tsquery":
		return convert.ToString(val)
	case "int4range":
		return convert.ToString(val)
	case "int8range":
		return convert.ToString(val)
	case "numrange":
		return convert.ToString(val)
	case "tsrange":
		return convert.ToString(val)
	case "tstzrange":
		return convert.ToString(val)
	case "daterange":
		return convert.ToString(val)
	case "geometry":
		return convert.ToString(val)
	case "geography":
		return convert.ToString(val)

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
