package utils

import (
	"slices"
)

var sqlTypes = []string{"mysql", "mariadb", "postgresql", "sqlite", "sqlite3", "postgres", "mssql"}

func IsSQLType(typ string) bool {
	return slices.Contains(sqlTypes, typ)
}
