package client

import (
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
)

type ThriftServiceClient struct {
	*cli_service.TCLIServiceClient
}

func (tsc *ThriftServiceClient) FetchResults(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
	return tsc.TCLIServiceClient.FetchResults(ctx, req)
}

func (tsc *ThriftServiceClient) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
	resp, err := tsc.TCLIServiceClient.ExecuteStatement(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, CheckStatus(resp)
}

// log.Debug().Msg(fmt.Sprint(c.transport.response.StatusCode))
// log.Debug().Msg(c.transport.response.Header.Get("X-Databricks-Org-Id"))
// log.Debug().Msg(c.transport.response.Header.Get("x-databricks-error-or-redirect-message"))
// log.Debug().Msg(c.transport.response.Header.Get("x-thriftserver-error-message"))
// log.Debug().Msg(c.transport.response.Header.Get("x-databricks-reason-phrase"))

// This is a wrapper of the http transport so we can have access to response code and headers
// It is important to know the code and headers to know if we need to retry or not
type Transport struct {
	*http.Transport
	response *http.Response
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.Transport.RoundTrip(req)
	t.response = resp
	return resp, err
}

func InitThriftClient(cfg *config.Config) (*ThriftServiceClient, error) {
	endpoint := cfg.ToEndpointURL()
	tcfg := &thrift.TConfiguration{
		TLSConfig: cfg.TLSConfig,
	}

	var protocolFactory thrift.TProtocolFactory
	switch cfg.ThriftProtocol {
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
		return nil, fmt.Errorf("invalid protocol specified %s", cfg.ThriftProtocol)
	}
	if cfg.ThriftDebugClientProtocol {
		protocolFactory = thrift.NewTDebugProtocolFactoryWithLogger(protocolFactory, "client:", thrift.StdLogger(nil))
	}

	var tTrans thrift.TTransport
	var tr *Transport
	var err error

	switch cfg.ThriftTransport {
	case "http":
		tr = &Transport{
			Transport: &http.Transport{
				TLSClientConfig: cfg.TLSConfig,
			},
		}
		httpclient := &http.Client{
			Transport: tr,
			// Timeout:   time.Duration(cfg.TimeoutSeconds * int(time.Second)), // Needed?
		}
		tTrans, err = thrift.NewTHttpClientWithOptions(endpoint, thrift.THttpClientOptions{Client: httpclient})
		httpTransport := tTrans.(*thrift.THttpClient)
		var userAgent string
		if cfg.UserAgentEntry != "" {
			userAgent = fmt.Sprintf("%s/%s", cfg.DriverName, cfg.DriverVersion)
		} else {
			userAgent = fmt.Sprintf("%s/%s (%s)", cfg.DriverName, cfg.DriverVersion, cfg.UserAgentEntry)
		}
		httpTransport.SetHeader("User-Agent", userAgent)

	case "framed":
		tTrans = thrift.NewTFramedTransportConf(tTrans, tcfg)
	case "buffered":
		tTrans = thrift.NewTBufferedTransport(tTrans, 8192)
	case "zlib":
		tTrans, err = thrift.NewTZlibTransport(tTrans, zlib.BestCompression)
	default:
		return nil, fmt.Errorf("invalid transport specified `%s`", cfg.ThriftTransport)
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
	tsClient := &ThriftServiceClient{tclient}
	return tsClient, nil
}

// RPCResponse respresents thrift rpc response
type RPCResponse interface {
	GetStatus() *cli_service.TStatus
}

func CheckStatus(resp interface{}) error {
	rpcresp, ok := resp.(RPCResponse)
	if ok {
		status := rpcresp.GetStatus()
		if status.StatusCode == cli_service.TStatusCode_ERROR_STATUS {
			return errors.New(status.GetErrorMessage())
		}
		if status.StatusCode == cli_service.TStatusCode_INVALID_HANDLE_STATUS {
			return errors.New("thrift: invalid handle")
		}

		// SUCCESS, SUCCESS_WITH_INFO, STILL_EXECUTING are ok
		return nil
	}

	return errors.New("thrift: invalid response")
}
