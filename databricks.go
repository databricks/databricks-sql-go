package dbsql

import (
	"database/sql"
	"io"
	"io/ioutil"
)

func init() {
	sql.Register("databricks", &Driver{})
}

// Options for driver connection
type Options struct {
	Host     string
	Port     string
	Token    string
	HTTPPath string
	MaxRows  int64
	Timeout  int

	LogOut io.Writer
}

var (
	// DefaultOptions for the driver
	DefaultOptions = Options{Port: "443", MaxRows: 10000, LogOut: ioutil.Discard}
)
