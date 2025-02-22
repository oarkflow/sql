package contracts

import (
	"context"
	"io"

	"github.com/oarkflow/sql/utils"
)

type Connector interface {
	Setup(context.Context) error
}

type Source interface {
	Extract(context.Context) (<-chan utils.Record, error)
	io.Closer
	Connector
}

type Mapper interface {
	Map(context.Context, utils.Record) (utils.Record, error)
	Name() string
}

type Transformer interface {
	Transform(context.Context, utils.Record) (utils.Record, error)
}

type Loader interface {
	LoadBatch(context.Context, []utils.Record) error
	Connector
	io.Closer
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
