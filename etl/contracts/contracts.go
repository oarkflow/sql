package contracts

import (
	"github.com/oarkflow/sql/utils"
)

type Connector interface {
	Setup() error
}

type Closer interface {
	Close() error
}

type Source interface {
	Extract() (<-chan utils.Record, error)
	Closer
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
	Closer
	Connector
}
