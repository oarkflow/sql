package interpreter

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

// Exec executes the given SPL script content with the provided data.
func Exec(script string, data map[string]interface{}) (Object, error) {
	env := NewGlobalEnvironment([]string{})

	injectData(env, data)

	l := NewLexer(script)
	p := NewParser(l)
	program := p.ParseProgram()

	if len(p.Errors()) != 0 {
		return nil, fmt.Errorf("parser errors: %v", p.Errors())
	}

	result := Eval(program, env)

	// Check for runtime errors returned as objects
	if result != nil {
		if result.Type() == STRING_OBJ {
			if strings.HasPrefix(result.(*String).Value, "ERROR:") {
				// Optional: return as error?
			}
		}
	}

	return result, nil
}

// ExecFile executes the SPL script from a file with the provided data.
func ExecFile(filename string, data map[string]interface{}) (Object, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return Exec(string(content), data)
}

func injectData(env *Environment, data map[string]interface{}) {
	for k, v := range data {
		obj := toObject(v)
		env.Set(k, obj)
	}
}

// toObject converts a Go value to an SPL Object
func toObject(val interface{}) Object {
	if val == nil {
		return NULL
	}

	v := reflect.ValueOf(val)

	switch v.Kind() {
	case reflect.Bool:
		return nativeBoolToBooleanObject(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &Integer{Value: v.Int()}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &Integer{Value: int64(v.Uint())}
	case reflect.Float32, reflect.Float64:
		return &Float{Value: v.Float()}
	case reflect.String:
		return &String{Value: v.String()}
	case reflect.Slice, reflect.Array:
		elements := make([]Object, v.Len())
		for i := 0; i < v.Len(); i++ {
			elements[i] = toObject(v.Index(i).Interface())
		}
		return &Array{Elements: elements}
	case reflect.Map:
		// Only support map[string]interface{} or similar for now ideally
		// But we can iterate keys
		pairs := make(map[HashKey]HashPair)
		iter := v.MapRange()
		for iter.Next() {
			key := toObject(iter.Key().Interface())
			// Key must be hashable
			hashKey, ok := key.(Hashable)
			if !ok {
				continue // Skip unhashable keys
			}
			val := toObject(iter.Value().Interface()) // Recursion
			pairs[hashKey.HashKey()] = HashPair{Key: key, Value: val}
		}
		return &Hash{Pairs: pairs}
	case reflect.Struct:
		// Convert struct to Hash
		pairs := make(map[HashKey]HashPair)
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			// Use json tag if available? For simplicity use Name.
			fieldName := field.Name
			key := &String{Value: fieldName}
			val := toObject(v.Field(i).Interface())
			pairs[key.HashKey()] = HashPair{Key: key, Value: val}
		}
		return &Hash{Pairs: pairs}
	default:
		return &String{Value: fmt.Sprintf("%v", val)}
	}
}
