package transformers

import (
	"context"
	
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
	panic("Implement Transform")
}

// TransformMany creates extra records for each relation based on the base record.
func (rt *RelationsTransformer) TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error) {
	panic("Implement TransformMany")
}
