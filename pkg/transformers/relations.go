package transformers

import (
	"context"
	"strings"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/utils"
)

// RelationsTransformer implements MultiTransformer to create extra records
// for each relation defined in TableMapping.
type RelationsTransformer struct {
	Relations []config.TableMapping
}

// NewRelationsTransformer creates a new instance.
func NewRelationsTransformer(relations []config.TableMapping) *RelationsTransformer {
	return &RelationsTransformer{Relations: relations}
}

func (rt *RelationsTransformer) Name() string {
	return "RelationsTransformer"
}

// Transform returns the base record.
// (Not used when MultiTransformer interface is implemented.)
func (rt *RelationsTransformer) Transform(ctx context.Context, rec utils.Record) (utils.Record, error) {
	return rec, nil
}

// TransformMany creates extra records for each relation based on the base record.
func (rt *RelationsTransformer) TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error) {
	output := []utils.Record{rec} // base record remains unchanged
	for _, rel := range rt.Relations {
		newRec := make(utils.Record)
		// start with the base mapping record
		for k, v := range rec {
			newRec[k] = v
		}
		// override/add new fields based on relation.Mapping
		for col, s := range rel.Mapping {
			if strings.HasPrefix(s, "eval.{{") && strings.HasSuffix(s, "}}") {
				inner := s[7 : len(s)-2]
				// For simplicity, if the inner expression is wrapped in quotes remove them
				inner = strings.Trim(inner, "'\"")
				newRec[col] = inner
			} else {
				newRec[col] = s
			}
		}
		// merge any extra values, if provided, into the relation record
		for k, v := range rel.ExtraValues {
			newRec[k] = v
		}
		output = append(output, newRec)
	}
	return output, nil
}
