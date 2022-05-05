package hive

import (
	"context"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/arikfr/go-dbsql/cli_service"
)

// Client represents Hive Client
type Client struct {
	client *cli_service.TCLIServiceClient
	opts   *Options
	log    *log.Logger
}

// Options for Hive Client
type Options struct {
	MaxRows      int64
	MemLimit     string
	QueryTimeout int
}

// NewClient creates Hive Client
func NewClient(client thrift.TClient, log *log.Logger, opts *Options) *Client {
	return &Client{
		client: cli_service.NewTCLIServiceClient(client),
		log:    log,
		opts:   opts,
	}
}

// OpenSession creates new hive session
func (c *Client) OpenSession(ctx context.Context) (*Session, error) {

	cfg := map[string]string{
		// "MEM_LIMIT":       c.opts.MemLimit,
		// "QUERY_TIMEOUT_S": strconv.Itoa(c.opts.QueryTimeout),
	}

	req := cli_service.TOpenSessionReq{
		ClientProtocol: cli_service.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V6,
		Configuration:  cfg,
	}

	resp, err := c.client.OpenSession(ctx, &req)
	if err != nil {
		return nil, err
	}
	if err := checkStatus(resp); err != nil {
		return nil, err
	}

	c.log.Printf("open session: %s", guid(resp.SessionHandle.GetSessionId().GUID))
	c.log.Printf("session config: %v", resp.Configuration)
	return &Session{h: resp.SessionHandle, hive: c}, nil
}
