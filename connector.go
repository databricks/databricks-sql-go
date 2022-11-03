package dbsql

import (
	"compress/zlib"
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/utils"
)

// TODO this is the thrift connector. We could have many implementations
type connector struct {
	cfg *config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {

	tclient, err := initThriftClient(c.cfg)
	if err != nil {
		return nil, fmt.Errorf("databricks: error initializing thrift client. %w", err)
	}

	req := cli_service.TOpenSessionReq{
		ClientProtocol: c.cfg.Thrift.ProtocolVersion,
		Configuration:  make(map[string]string),
	}

	session, err := tclient.OpenSession(ctx, &req)
	if err != nil {
		return nil, err
	}

	fmt.Printf("open session: %s\n", utils.Guid(session.SessionHandle.GetSessionId().GUID))
	fmt.Printf("session config: %v\n", session.Configuration)
	conn := &conn{
		cfg:     c.cfg,
		client:  tclient,
		session: session,
	}
	return conn, nil
}

func (c *connector) Driver() driver.Driver {
	return &databricksDriver{}
}

func buildEndpointURL(c *config) string {
	var endpointUrl string
	if c.Host == "localhost" {
		endpointUrl = fmt.Sprintf("http://%s:%d", c.Host, c.Port)
	} else {
		endpointUrl = fmt.Sprintf("https://%s:%s@%s:%d%s", "token", url.QueryEscape(c.AccessToken), c.Host, c.Port, c.HTTPPath)

	}
	return endpointUrl
}

func initThriftClient(cfg *config) (*cli_service.TCLIServiceClient, error) {
	endpoint := buildEndpointURL(cfg)
	tcfg := &thrift.TConfiguration{
		TLSConfig: cfg.TLSConfig,
	}

	var protocolFactory thrift.TProtocolFactory
	switch cfg.Thrift.Protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactoryConf(tcfg)
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactoryConf(tcfg)
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
	case "binary":
		protocolFactory = thrift.NewTBinaryProtocolFactoryConf(tcfg)
	case "header":
		protocolFactory = thrift.NewTHeaderProtocolFactoryConf(tcfg)
	default:
		return nil, fmt.Errorf("invalid protocol specified %s", cfg.Thrift.Protocol)
	}
	if cfg.Thrift.DebugClientProtocol {
		protocolFactory = thrift.NewTDebugProtocolFactoryWithLogger(protocolFactory, "client:", thrift.StdLogger(nil))
	}

	var tTrans thrift.TTransport
	var err error

	switch cfg.Thrift.Transport {
	case "http":
		tr := &http.Transport{
			TLSClientConfig: cfg.TLSConfig,
		}
		httpclient := &http.Client{
			Transport: tr,
			// Timeout:   time.Duration(cfg.TimeoutSeconds * int(time.Second)), // Needed?
		}
		tTrans, err = thrift.NewTHttpClientWithOptions(endpoint, thrift.THttpClientOptions{Client: httpclient})
		fmt.Println(endpoint)
		httpTransport, ok := tTrans.(*thrift.THttpClient)
		if ok {
			var userAgent string
			if cfg.UserAgentEntry != "" {
				userAgent = fmt.Sprintf("%s/%s", DriverName, DriverVersion)
			} else {
				userAgent = fmt.Sprintf("%s/%s (%s)", DriverName, DriverVersion, cfg.UserAgentEntry)
			}
			httpTransport.SetHeader("User-Agent", userAgent)
		}
	case "framed":
		tTrans = thrift.NewTFramedTransportConf(tTrans, tcfg)
	case "buffered":
		tTrans = thrift.NewTBufferedTransport(tTrans, 8192)
	case "zlib":
		tTrans, err = thrift.NewTZlibTransport(tTrans, zlib.BestCompression)
	default:
		return nil, fmt.Errorf("invalid transport specified `%s`", cfg.Thrift.Transport)
	}
	if err != nil {
		return nil, err
	}
	if err = tTrans.Open(); err != nil {
		return nil, err
	}
	iprot := protocolFactory.GetProtocol(tTrans)
	oprot := protocolFactory.GetProtocol(tTrans)
	tclient := cli_service.NewTCLIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	return tclient, nil
}

var _ driver.Connector = (*connector)(nil)
