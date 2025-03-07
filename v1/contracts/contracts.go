package contracts

import (
	"context"
)

type Record = map[string]any

type Source interface {
	Setup(ctx context.Context) error
	Extract(ctx context.Context) (<-chan Record, error)
	Close() error
}

type Loader interface {
	Setup(ctx context.Context) error
	LoadBatch(ctx context.Context, batch []Record) error
	Close() error
}

type Mapper interface {
	Name() string
	Map(ctx context.Context, rec Record) (Record, error)
}

type Transformer interface {
	Transform(ctx context.Context, rec Record) (Record, error)
}

type Validator interface {
	Validate(ctx context.Context, rec Record) error
}
