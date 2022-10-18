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
	Host           string
	Port           string
	Token          string
	HTTPPath       string
	MaxRows        int64
	Timeout        int
	UserAgentEntry string
	RunAsync       bool

	LogOut io.Writer
}

const (
	// Constants for Go driver
	DriverName    = "godatabrickssqlconnector"
	DriverVersion = "0.9.0"
)

var (
	// DefaultOptions for the driver
	DefaultOptions = Options{Port: "443", MaxRows: 10000, RunAsync: true, LogOut: ioutil.Discard}
)
