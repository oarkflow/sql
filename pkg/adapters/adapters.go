package adapters

import (
	"fmt"
	"strings"

	"github.com/oarkflow/sql/pkg/adapters/fileadapter"
	"github.com/oarkflow/sql/pkg/adapters/mqadapter"
	"github.com/oarkflow/sql/pkg/adapters/nosqladapter"
	"github.com/oarkflow/sql/pkg/adapters/restadapter"
	"github.com/oarkflow/sql/pkg/adapters/sqladapter"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
)

func NewLookupLoader(lkup config.DataConfig) (contracts.LookupLoader, error) {
	switch strings.ToLower(lkup.Type) {
	case "postgresql", "mysql", "sqlite":
		db, err := config.OpenDB(lkup)
		if err != nil {
			return nil, fmt.Errorf("error connecting to lookup DB: %v", err)
		}
		return sqladapter.NewSource(db, "", lkup.Source), nil
	case "csv", "json":
		return fileadapter.New(lkup.File, "source", false), nil
	case "nosql":
		return nosqladapter.New(lkup), nil
	case "rest":
		return restadapter.New(lkup), nil
	case "mq":
		return mqadapter.New(lkup), nil
	default:
		return nil, fmt.Errorf("unsupported lookup type: %s", lkup.Type)
	}
}
