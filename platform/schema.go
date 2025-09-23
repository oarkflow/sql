package platform

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InMemorySchemaRegistry implements SchemaRegistry
type InMemorySchemaRegistry struct {
	schemas map[string]CanonicalModel
	mu      sync.RWMutex
}

func NewInMemorySchemaRegistry() *InMemorySchemaRegistry {
	return &InMemorySchemaRegistry{
		schemas: make(map[string]CanonicalModel),
	}
}

func (r *InMemorySchemaRegistry) Register(schema CanonicalModel) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if schema.SchemaName == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	r.schemas[schema.SchemaName] = schema
	log.Printf("Registered schema: %s", schema.SchemaName)
	return nil
}

func (r *InMemorySchemaRegistry) Get(schemaName string) (CanonicalModel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schema, exists := r.schemas[schemaName]
	if !exists {
		return CanonicalModel{}, fmt.Errorf("schema not found: %s", schemaName)
	}

	return schema, nil
}

func (r *InMemorySchemaRegistry) Validate(record Record, schemaName string) error {
	schema, err := r.Get(schemaName)
	if err != nil {
		return err
	}

	for fieldName, fieldSchema := range schema.Fields {
		value, exists := record[fieldName]

		// Check required fields
		if fieldSchema.Required && (!exists || value == nil) {
			return fmt.Errorf("required field missing: %s", fieldName)
		}

		if exists && value != nil {
			// Type validation
			if err := r.validateFieldType(value, fieldSchema.Type); err != nil {
				return fmt.Errorf("field %s validation error: %v", fieldName, err)
			}

			// Custom validation rules
			if fieldSchema.Validation != "" {
				if err := r.validateCustomRule(value, fieldSchema.Validation); err != nil {
					return fmt.Errorf("field %s custom validation error: %v", fieldName, err)
				}
			}
		}
	}

	return nil
}

func (r *InMemorySchemaRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.schemas))
	for name := range r.schemas {
		names = append(names, name)
	}
	return names
}

func (r *InMemorySchemaRegistry) validateFieldType(value interface{}, expectedType string) error {
	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "int", "integer":
		switch v := value.(type) {
		case int, int32, int64:
			// OK
		case float64:
			if v != float64(int64(v)) {
				return fmt.Errorf("expected integer, got float with decimal")
			}
		case string:
			if _, err := strconv.Atoi(v); err != nil {
				return fmt.Errorf("expected integer, got invalid string: %s", v)
			}
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
	case "float", "number":
		switch value.(type) {
		case float32, float64, int, int32, int64:
			// OK
		case string:
			if _, err := strconv.ParseFloat(value.(string), 64); err != nil {
				return fmt.Errorf("expected number, got invalid string: %s", value)
			}
		default:
			return fmt.Errorf("expected number, got %T", value)
		}
	case "boolean", "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case "date", "datetime":
		switch v := value.(type) {
		case time.Time:
			// OK
		case string:
			if _, err := time.Parse(time.RFC3339, v); err != nil {
				return fmt.Errorf("expected datetime, got invalid string: %s", v)
			}
		default:
			return fmt.Errorf("expected datetime, got %T", value)
		}
	default:
		// Unknown type - accept as string
	}

	return nil
}

func (r *InMemorySchemaRegistry) validateCustomRule(value interface{}, rule string) error {
	// Simple validation rules
	// In production, this could use a more sophisticated validation engine

	strValue := fmt.Sprintf("%v", value)

	switch rule {
	case "email":
		if !strings.Contains(strValue, "@") {
			return fmt.Errorf("invalid email format")
		}
	case "not_empty":
		if strings.TrimSpace(strValue) == "" {
			return fmt.Errorf("value cannot be empty")
		}
	case "positive":
		if num, err := strconv.ParseFloat(strValue, 64); err == nil && num <= 0 {
			return fmt.Errorf("value must be positive")
		}
	}

	return nil
}

// FieldNormalizer normalizes field values
type FieldNormalizer struct{}

func NewFieldNormalizer() *FieldNormalizer {
	return &FieldNormalizer{}
}

func (n *FieldNormalizer) Normalize(value interface{}, targetType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch targetType {
	case "string":
		return n.toString(value), nil
	case "int", "integer":
		return n.toInt(value)
	case "float", "number":
		return n.toFloat(value)
	case "boolean", "bool":
		return n.toBool(value)
	case "date", "datetime":
		return n.toDateTime(value)
	default:
		return value, nil
	}
}

func (n *FieldNormalizer) toString(value interface{}) string {
	return fmt.Sprintf("%v", value)
}

func (n *FieldNormalizer) toInt(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if v == float64(int64(v)) {
			return int64(v), nil
		}
		return 0, fmt.Errorf("cannot convert float with decimal to int: %v", v)
	case string:
		return strconv.ParseInt(v, 10, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

func (n *FieldNormalizer) toFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}

func (n *FieldNormalizer) toBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int32, int64:
		return reflect.ValueOf(v).Int() != 0, nil
	case float64:
		return v != 0.0, nil
	case string:
		lower := strings.ToLower(v)
		return lower == "true" || lower == "1" || lower == "yes" || lower == "y", nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func (n *FieldNormalizer) toDateTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try common date formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02",
			"01/02/2006",
			"2006-01-02 15:04:05",
			"01/02/2006 15:04:05",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}

		// Try parsing as unix timestamp
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Unix(ts, 0), nil
		}

		return time.Time{}, fmt.Errorf("cannot parse date string: %s", v)
	case int64:
		return time.Unix(v, 0), nil
	case float64:
		return time.Unix(int64(v), 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to datetime", value)
	}
}

// RecordNormalizer normalizes entire records
type RecordNormalizer struct {
	registry   SchemaRegistry
	normalizer *FieldNormalizer
}

func NewRecordNormalizer(registry SchemaRegistry) *RecordNormalizer {
	return &RecordNormalizer{
		registry:   registry,
		normalizer: NewFieldNormalizer(),
	}
}

func (n *RecordNormalizer) Normalize(record Record, schemaName string) (Record, error) {
	schema, err := n.registry.Get(schemaName)
	if err != nil {
		return nil, err
	}

	normalized := make(Record)

	for fieldName, fieldSchema := range schema.Fields {
		originalValue, exists := record[fieldName]

		var normalizedValue interface{}

		if exists && originalValue != nil {
			// Normalize the field value
			normalizedValue, err = n.normalizer.Normalize(originalValue, fieldSchema.Type)
			if err != nil {
				log.Printf("Warning: failed to normalize field %s: %v", fieldName, err)
				normalizedValue = originalValue // Keep original value
			}
		} else if fieldSchema.Default != nil {
			// Use default value
			normalizedValue = fieldSchema.Default
		}

		if normalizedValue != nil {
			normalized[fieldName] = normalizedValue
		}
	}

	return normalized, nil
}

// SchemaMapper maps incoming fields to canonical schema fields
type SchemaMapper struct {
	mappings map[string]string // incoming_field -> canonical_field
}

func NewSchemaMapper(mappings map[string]string) *SchemaMapper {
	return &SchemaMapper{
		mappings: mappings,
	}
}

func (m *SchemaMapper) Map(record Record) Record {
	mapped := make(Record)

	for incomingField, canonicalField := range m.mappings {
		if value, exists := record[incomingField]; exists {
			mapped[canonicalField] = value
		}
	}

	// Include unmapped fields as-is
	for field, value := range record {
		if _, isMapped := m.mappings[field]; !isMapped {
			mapped[field] = value
		}
	}

	return mapped
}

// JSONSchemaValidator validates against JSON Schema
type JSONSchemaValidator struct{}

func NewJSONSchemaValidator() *JSONSchemaValidator {
	return &JSONSchemaValidator{}
}

func (v *JSONSchemaValidator) Validate(record Record, schemaJSON string) error {
	// Basic JSON schema validation
	// In production, use a proper JSON schema validator like gojsonschema

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return fmt.Errorf("invalid schema JSON: %v", err)
	}

	// Simple validation - check required fields
	if required, ok := schema["required"].([]interface{}); ok {
		for _, req := range required {
			if reqField, ok := req.(string); ok {
				if _, exists := record[reqField]; !exists {
					return fmt.Errorf("required field missing: %s", reqField)
				}
			}
		}
	}

	// Check properties
	if properties, ok := schema["properties"].(map[string]interface{}); ok {
		for fieldName, fieldDef := range properties {
			if fieldDefMap, ok := fieldDef.(map[string]interface{}); ok {
				if fieldType, ok := fieldDefMap["type"].(string); ok {
					if value, exists := record[fieldName]; exists {
						if err := v.validateType(value, fieldType); err != nil {
							return fmt.Errorf("field %s: %v", fieldName, err)
						}
					}
				}
			}
		}
	}

	return nil
}

func (v *JSONSchemaValidator) validateType(value interface{}, expectedType string) error {
	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "number":
		switch value.(type) {
		case float64, int, int32, int64:
			// OK
		default:
			return fmt.Errorf("expected number, got %T", value)
		}
	case "integer":
		switch value.(type) {
		case int, int32, int64:
			// OK
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case "object":
		if _, ok := value.(map[string]interface{}); !ok {
			return fmt.Errorf("expected object, got %T", value)
		}
	case "array":
		if _, ok := value.([]interface{}); !ok {
			return fmt.Errorf("expected array, got %T", value)
		}
	}

	return nil
}
