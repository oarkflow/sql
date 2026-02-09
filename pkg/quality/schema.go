package quality

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/oarkflow/sql/pkg/utils"
)

type SchemaDefinition struct {
	Version   int               `json:"version"`
	Fields    map[string]string `json:"fields"` // name -> type
	Hash      string            `json:"hash"`
	CreatedAt string            `json:"created_at"`
}

type SchemaRegistry struct {
	currentSchema *SchemaDefinition
	history       []*SchemaDefinition
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		history: make([]*SchemaDefinition, 0),
	}
}

func InferSchema(record utils.Record) *SchemaDefinition {
	fields := make(map[string]string)
	var keys []string

	for k, v := range record {
		keys = append(keys, k)
		fields[k] = fmt.Sprintf("%T", v)
	}
	sort.Strings(keys)

	// Create signature for hash
	var sig strings.Builder
	for _, k := range keys {
		sig.WriteString(k)
		sig.WriteString(":")
		sig.WriteString(fields[k])
		sig.WriteString(";")
	}

	hash := sha256.Sum256([]byte(sig.String()))

	return &SchemaDefinition{
		Fields: fields,
		Hash:   hex.EncodeToString(hash[:]),
	}
}

func (r *SchemaRegistry) Register(schema *SchemaDefinition) {
	if r.currentSchema == nil {
		schema.Version = 1
		r.currentSchema = schema
		r.history = append(r.history, schema)
		return
	}

	if r.currentSchema.Hash != schema.Hash {
		// New version
		schema.Version = r.currentSchema.Version + 1
		r.currentSchema = schema
		r.history = append(r.history, schema)
	}
}

func (r *SchemaRegistry) DetectDrift(record utils.Record) (bool, *SchemaDefinition) {
	incoming := InferSchema(record)

	if r.currentSchema == nil {
		return true, incoming // First schema is technically "new", handled by caller
	}

	if incoming.Hash != r.currentSchema.Hash {
		return true, incoming
	}

	return false, nil
}
