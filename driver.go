package dbsql

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/hive"
)

var (
	// ErrNotSupported means this operation is not supported by the driver
	ErrNotSupported = errors.New("databricks: not supported")
)

type Driver struct{}

// Open creates new connection to Databricks SQL
func (d *Driver) Open(uri string) (driver.Conn, error) {
	opts, err := parseURI(uri)
	if err != nil {
		return nil, err
	}

	log.Printf("opts: %v", opts)

	conn, err := connect(opts)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func parseURI(uri string) (*Options, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "databricks" {
		return nil, fmt.Errorf("scheme %s not recognized", u.Scheme)
	}

	opts := DefaultOptions

	if u.User != nil {
		token, ok := u.User.Password()
		if ok {
			opts.Token = token
		}
	} else {
		return nil, fmt.Errorf("Token is required.")
	}

	if !strings.Contains(u.Host, ":") {
		u.Host = fmt.Sprintf("%s:%s", u.Host, DefaultOptions.Port)
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}

	opts.Host = host
	opts.Port = port
	opts.HTTPPath = u.Path

	if u.Path == "" {
		return nil, fmt.Errorf("HTTP Path is required.")
	}

	query := u.Query()

	maxRows, ok := query["maxRows"]

	if ok {
		opts.MaxRows, err = strconv.ParseInt(maxRows[0], 10, 64)

		if err != nil {
			return nil, fmt.Errorf("maxRows value wrongly formatted: %w", err)
		}
	}

	timeout, ok := query["timeout"]

	if ok {
		opts.Timeout, err = strconv.Atoi(timeout[0])

		if err != nil {
			return nil, fmt.Errorf("timeout value wrongly formatted: %w", err)
		}
	}

	return &opts, nil
}

// OpenConnector parses name and return connector with fixed options
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {

	opts, err := parseURI(name)
	if err != nil {
		return nil, err
	}

	return &connector{opts: opts}, nil
}

type connector struct {
	d    *Driver
	opts *Options
}

// NewConnector creates connector with specified options
func NewConnector(opts *Options) driver.Connector {
	return &connector{opts: opts}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return connect(c.opts)
}

func (c *connector) Driver() driver.Driver {
	return c.d
}

func connect(opts *Options) (*Conn, error) {
	var socket thrift.TTransport
	var err error
	var transport thrift.TTransport
	logger := log.New(opts.LogOut, "databricks: ", log.LstdFlags)
	timeout := time.Duration(opts.Timeout * int(time.Millisecond))
	httpClient := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{},
		},
	}

	if err != nil {
		return nil, err
	}

	httpOptions := thrift.THttpClientOptions{Client: httpClient}
	endpointUrl := fmt.Sprintf("https://%s:%s@%s:%s"+opts.HTTPPath, "token", url.QueryEscape(opts.Token), opts.Host, opts.Port)
	transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(endpointUrl, httpOptions).GetTransport(socket)
	if err != nil {
		return nil, err
	}

	httpTransport, ok := transport.(*thrift.THttpClient)
	if ok {
		// TODO: currently masking as a python connector until additional user agents are white listed.
		httpTransport.SetHeader("User-Agent", "godatabrickssqlconnector/0.9.0 (go-dbsql)")
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	tclient := thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport))

	client := hive.NewClient(tclient, logger, &hive.Options{MaxRows: opts.MaxRows})

	return &Conn{client: client, t: transport, log: logger}, nil
}
