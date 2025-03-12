package contract

import (
	"context"

	"github.com/oarkflow/etl/pkg/config"
	"github.com/oarkflow/etl/pkg/utils"
)

type SourceOption struct {
	Table string
	Query string
}

// Option defines a function type for configuring a SQLAdapter.
type Option func(*SourceOption)

// WithTable sets the table name for the adapter.
func WithTable(table string) Option {
	return func(a *SourceOption) {
		a.Table = table
	}
}

// WithQuery sets a custom query for the adapter.
func WithQuery(query string) Option {
	return func(a *SourceOption) {
		a.Query = query
	}
}

type Source interface {
	Setup(ctx context.Context) error
	Extract(ctx context.Context, opts ...Option) (<-chan utils.Record, error)
	Close() error
}

type Loader interface {
	Setup(ctx context.Context) error
	StoreBatch(ctx context.Context, batch []utils.Record) error
	Close() error
}

type Mapper interface {
	Name() string
	Map(ctx context.Context, rec utils.Record) (utils.Record, error)
}

type Transformer interface {
	Name() string
	Transform(ctx context.Context, rec utils.Record) (utils.Record, error)
}

type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, checkpoint string) error
	GetCheckpoint(context.Context) (string, error)
}

type Transactional interface {
	Begin(context.Context) error
	Commit(context.Context) error
	Rollback(context.Context) error
}

type Validator interface {
	Validate(ctx context.Context, rec utils.Record) error
}

type MultiTransformer interface {
	TransformMany(ctx context.Context, rec utils.Record) ([]utils.Record, error)
}

type LookupLoader interface {
	Setup(ctx context.Context) error
	LoadData(opts ...Option) ([]utils.Record, error)
	Close() error
}

type Node interface {
	Process(ctx context.Context, in <-chan utils.Record, tableCfg config.TableMapping) (<-chan utils.Record, error)
}

type Flushable interface {
	Flush(ctx context.Context) ([]utils.Record, error)
}
