package client

import (
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

// this is used to generate test data. Developer should change this manually
var RecordResults bool
var resultIndex int

type ThriftServiceClient struct {
	*cli_service.TCLIServiceClient
	transport *Transport
}

func (tsc *ThriftServiceClient) OpenSession(ctx context.Context, req *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error) {
	msg, start := logger.Track("OpenSession")
	resp, err := tsc.TCLIServiceClient.OpenSession(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "open session request error")
	}
	log := logger.WithContext(SprintGuid(resp.SessionHandle.SessionId.GUID), driverctx.CorrelationIdFromContext(ctx), "")
	defer log.Duration(msg, start)
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("OpenSession%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) CloseSession(ctx context.Context, req *cli_service.TCloseSessionReq) (*cli_service.TCloseSessionResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), "")
	defer log.Duration(logger.Track("CloseSession"))
	resp, err := tsc.TCLIServiceClient.CloseSession(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "close session request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("CloseSession%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) FetchResults(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.OperationHandle.OperationId.GUID))
	defer log.Duration(logger.Track("FetchResults"))
	resp, err := tsc.TCLIServiceClient.FetchResults(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "fetch results request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("FetchResults%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) GetResultSetMetadata(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.OperationHandle.OperationId.GUID))
	defer log.Duration(logger.Track("GetResultSetMetadata"))
	resp, err := tsc.TCLIServiceClient.GetResultSetMetadata(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "get result set metadata request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("ExecuteStatement%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
	msg, start := logger.Track("ExecuteStatement")
	resp, err := tsc.TCLIServiceClient.ExecuteStatement(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "execute statement request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("ExecuteStatement%d.json", resultIndex), j, 0600)
		// f, _ := os.ReadFile(fmt.Sprintf("ExecuteStatement%d.json", resultIndex))
		// var resp2 cli_service.TExecuteStatementResp
		// json.Unmarshal(f, &resp2)
		resultIndex++
	}
	if resp != nil && resp.OperationHandle != nil {
		log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(resp.OperationHandle.OperationId.GUID))
		defer log.Duration(msg, start)
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) GetOperationStatus(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.OperationHandle.OperationId.GUID))
	defer log.Duration(logger.Track("GetOperationStatus"))
	resp, err := tsc.TCLIServiceClient.GetOperationStatus(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "get operation status request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("GetOperationStatus%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) CloseOperation(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.OperationHandle.OperationId.GUID))
	defer log.Duration(logger.Track("CloseOperation"))
	resp, err := tsc.TCLIServiceClient.CloseOperation(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "close operation request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("CloseOperation%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return resp, CheckStatus(resp)
}

func (tsc *ThriftServiceClient) CancelOperation(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.OperationHandle.OperationId.GUID))
	defer log.Duration(logger.Track("CancelOperation"))
	resp, err := tsc.TCLIServiceClient.CancelOperation(ctx, req)
	if err != nil {
		return resp, errors.Wrap(err, "cancel operation request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("CancelOperation%d.json", resultIndex), j, 0600)
		resultIndex++
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
		return nil, errors.Errorf("invalid protocol specified %s", cfg.ThriftProtocol)
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
			Timeout:   cfg.ClientTimeout,
		}
		tTrans, err = thrift.NewTHttpClientWithOptions(endpoint, thrift.THttpClientOptions{Client: httpclient})
		if err != nil {
			return nil, err
		}

		httpTransport := tTrans.(*thrift.THttpClient)
		userAgent := fmt.Sprintf("%s/%s", cfg.DriverName, cfg.DriverVersion)
		if cfg.UserAgentEntry != "" {
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
		return nil, errors.Errorf("invalid transport specified `%s`", cfg.ThriftTransport)
	}
	if err != nil {
		return nil, err
	}
	if err = tTrans.Open(); err != nil {
		return nil, errors.Wrapf(err, "failed to open http transport for endpoint %s", endpoint)
	}
	iprot := protocolFactory.GetProtocol(tTrans)
	oprot := protocolFactory.GetProtocol(tTrans)
	tclient := cli_service.NewTCLIServiceClient(thrift.NewTStandardClient(iprot, oprot))
	tsClient := &ThriftServiceClient{tclient, tr}
	return tsClient, nil
}

// ThriftResponse respresents thrift rpc response
type ThriftResponse interface {
	GetStatus() *cli_service.TStatus
}

func CheckStatus(resp interface{}) error {
	rpcresp, ok := resp.(ThriftResponse)
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

func SprintGuid(bts []byte) string {
	if len(bts) == 16 {
		return fmt.Sprintf("%x-%x-%x-%x-%x", bts[0:4], bts[4:6], bts[6:8], bts[8:10], bts[10:16])
	}
	logger.Warn().Msgf("GUID not valid: %x", bts)
	return fmt.Sprintf("%x", bts)
}
