package contract

import (
	"context"

	"github.com/oarkflow/sql/utils"
)

type Source interface {
	Setup(ctx context.Context) error
	Extract(ctx context.Context) (<-chan utils.Record, error)
	Close() error
}

type Loader interface {
	Setup(ctx context.Context) error
	LoadBatch(ctx context.Context, batch []utils.Record) error
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
