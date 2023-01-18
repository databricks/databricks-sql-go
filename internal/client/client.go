package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
)

// RecordResults is used to generate test data. Developer should change this manually
var RecordResults bool
var resultIndex int

type ThriftServiceClient struct {
	*cli_service.TCLIServiceClient
}

// OpenSession is a wrapper around the thrift operation OpenSession
// If RecordResults is true, the results will be marshalled to JSON format and written to OpenSession<index>.json
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

// CloseSession is a wrapper around the thrift operation CloseSession
// If RecordResults is true, the results will be marshalled to JSON format and written to CloseSession<index>.json
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

// FetchResults is a wrapper around the thrift operation FetchResults
// If RecordResults is true, the results will be marshalled to JSON format and written to FetchResults<index>.json
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

// GetResultSetMetadata is a wrapper around the thrift operation GetResultSetMetadata
// If RecordResults is true, the results will be marshalled to JSON format and written to GetResultSetMetadata<index>.json
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

// ExecuteStatement is a wrapper around the thrift operation ExecuteStatement
// If RecordResults is true, the results will be marshalled to JSON format and written to ExecuteStatement<index>.json
func (tsc *ThriftServiceClient) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
	msg, start := logger.Track("ExecuteStatement")
	resp, err := tsc.TCLIServiceClient.ExecuteStatement(context.Background(), req)
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

// GetOperationStatus is a wrapper around the thrift operation GetOperationStatus
// If RecordResults is true, the results will be marshalled to JSON format and written to GetOperationStatus<index>.json
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

// CloseOperation is a wrapper around the thrift operation CloseOperation
// If RecordResults is true, the results will be marshalled to JSON format and written to CloseOperation<index>.json
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

// CancelOperation is a wrapper around the thrift operation CancelOperation
// If RecordResults is true, the results will be marshalled to JSON format and written to CancelOperation<index>.json
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

// InitThriftClient is a wrapper of the http transport, so we can have access to response code and headers.
// It is important to know the code and headers to know if we need to retry or not
func InitThriftClient(cfg *config.Config, httpclient *http.Client) (*ThriftServiceClient, error) {
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
	var err error

	switch cfg.ThriftTransport {
	case "http":
		if httpclient == nil {
			if cfg.Authenticator == nil {
				return nil, errors.New("databricks: no authentication method set")
			}
			httpclient = RetryableClient(cfg)
		}

		tTrans, err = thrift.NewTHttpClientWithOptions(endpoint, thrift.THttpClientOptions{Client: httpclient})

		thriftHttpClient := tTrans.(*thrift.THttpClient)
		userAgent := fmt.Sprintf("%s/%s", cfg.DriverName, cfg.DriverVersion)
		if cfg.UserAgentEntry != "" {
			userAgent = fmt.Sprintf("%s/%s (%s)", cfg.DriverName, cfg.DriverVersion, cfg.UserAgentEntry)
		}
		thriftHttpClient.SetHeader("User-Agent", userAgent)

	default:
		return nil, errors.Errorf("unsupported transport `%s`", cfg.ThriftTransport)
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
	tsClient := &ThriftServiceClient{tclient}
	return tsClient, nil
}

// ThriftResponse represents the thrift rpc response
type ThriftResponse interface {
	GetStatus() *cli_service.TStatus
}

// CheckStatus checks the status code after a thrift operation.
// Returns nil if the operation is successful or still executing, otherwise returns an error.
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

// SprintGuid is a convenience function to format a byte array into GUID.
func SprintGuid(bts []byte) string {
	if len(bts) == 16 {
		return fmt.Sprintf("%x-%x-%x-%x-%x", bts[0:4], bts[4:6], bts[6:8], bts[8:10], bts[10:16])
	}
	logger.Warn().Msgf("GUID not valid: %x", bts)
	return fmt.Sprintf("%x", bts)
}

type Transport struct {
	Base  *http.Transport
	Authr auth.Authenticator
	trace bool
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.trace {
		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { log.Printf("conn was reused: %t", info.Reused) },
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	}

	defer logger.Duration(logger.Track("RoundTrip"))
	// this is inspired by oauth2.Transport
	reqBodyClosed := false
	if req.Body != nil {
		defer func() {
			if !reqBodyClosed {
				req.Body.Close()
			}
		}()
	}

	req2 := cloneRequest(req) // per RoundTripper contract

	err := t.Authr.Authenticate(req2)

	if err != nil {
		return nil, err
	}

	// req.Body is assumed to be closed by the base RoundTripper.
	reqBodyClosed = true
	resp, err := t.Base.RoundTrip(req2)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func RetryableClient(cfg *config.Config) *http.Client {
	httpclient := PooledClient(cfg)
	retryableClient := &retryablehttp.Client{
		HTTPClient:   httpclient,
		Logger:       &leveledLogger{},
		RetryWaitMin: cfg.RetryWaitMin,
		RetryWaitMax: cfg.RetryWaitMax,
		RetryMax:     cfg.RetryMax,
		ErrorHandler: errorHandler,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      retryablehttp.DefaultBackoff,
	}
	return retryableClient.StandardClient()
}

func PooledTransport() *http.Transport {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   10, // this client is only used for one host
		MaxConnsPerHost:       100,
	}
	return transport
}

func PooledClient(cfg *config.Config) *http.Client {
	if cfg.Authenticator == nil {
		return nil
	}
	tr := &Transport{
		Base:  PooledTransport(),
		Authr: cfg.Authenticator,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   cfg.ClientTimeout,
	}
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request) *http.Request {
	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *r
	// deep copy of the Header
	r2.Header = make(http.Header, len(r.Header))
	for k, s := range r.Header {
		r2.Header[k] = append([]string(nil), s...)
	}
	return r2
}

type leveledLogger struct {
}

func (l *leveledLogger) Error(msg string, keysAndValues ...interface{}) {
	logger.Error().Msg(msg)
}
func (l *leveledLogger) Info(msg string, keysAndValues ...interface{}) {
	logger.Info().Msg(msg)
}
func (l *leveledLogger) Debug(msg string, keysAndValues ...interface{}) {
	logger.Debug().Msg(msg)
}
func (l *leveledLogger) Warn(msg string, keysAndValues ...interface{}) {
	logger.Warn().Msg(msg)
}

func errorHandler(resp *http.Response, err error, numTries int) (*http.Response, error) {
	var werr error
	if err == nil {
		err = errors.New(fmt.Sprintf("request error after %d attempt(s)", numTries))
	}
	if resp != nil && resp.Header != nil {

		// TODO @mattdeekay: convert these to specific error types

		orgid := resp.Header.Get("X-Databricks-Org-Id")
		reason := resp.Header.Get("X-Databricks-Reason-Phrase")
		terrmsg := resp.Header.Get("X-Thriftserver-Error-Message")
		errmsg := resp.Header.Get("x-databricks-error-or-redirect-message")

		werr = errors.Wrapf(err, fmt.Sprintf("orgId: %s, reason: %s, thriftErr: %s, err: %s", orgid, reason, terrmsg, errmsg))
	} else {
		werr = err
	}

	return resp, werr
}
