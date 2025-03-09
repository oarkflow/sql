package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/etl/utils/sqlutil"
)

func main() {
	dataTypes := []interface{}{
		true,
		42,
		3.14,
		"Hello, World",
		[]byte{0x01, 0x02},
		time.Now(),
	}
	drivers := []string{"MySQL", "Postgres", "SQLite"}
	for _, data := range dataTypes {
		var goType string
		switch data.(type) {
		case bool:
			goType = "bool"
		case int:
			goType = "int"
		case int8:
			goType = "int8"
		case int16:
			goType = "int16"
		case int32:
			goType = "int32"
		case int64:
			goType = "int64"
		case uint:
			goType = "uint"
		case uint8:
			goType = "uint8"
		case uint16:
			goType = "uint16"
		case uint32:
			goType = "uint32"
		case uint64:
			goType = "uint64"
		case float32:
			goType = "float32"
		case float64:
			goType = "float64"
		case string:
			goType = "string"
		case []byte:
			goType = "[]byte"
		case time.Time:
			goType = "time.Time"
		default:
			goType = "interface{}"
		}
		fmt.Printf("Go type: %s\n", goType)
		for _, driver := range drivers {
			fmt.Printf("%s: %s\n", driver, sqlutil.GetDataType(goType, driver))
		}

		fmt.Println()
	}
}
