package contracts

import (
	"io"

	"github.com/oarkflow/sql/utils"
)

type Connector interface {
	Setup() error
}

type Source interface {
	Extract() (<-chan utils.Record, error)
	io.Closer
	Connector
}

type Mapper interface {
	Map(utils.Record) (utils.Record, error)
}

type Transformer interface {
	Transform(utils.Record) (utils.Record, error)
}

type Loader interface {
	LoadBatch([]utils.Record) error
	Connector
	io.Closer
}

type CheckpointStore interface {
	SaveCheckpoint(checkpoint string) error
	GetCheckpoint() (string, error)
}

type Transactional interface {
	Begin() error
	Commit() error
	Rollback() error
}

type Validator interface {
	Validate(rec utils.Record) error
}
