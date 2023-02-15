package client

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
)

// RecordResults is used to generate test data. Developer should change this manually
var RecordResults bool
var resultIndex int

type DatabricksClient interface {
	OpenSession(context.Context, *OpenSessionReq) (*OpenSessionResp, error)
	CloseSession(context.Context, *CloseSessionReq) (*CloseSessionResp, error)
	FetchResults(context.Context, *FetchResultsReq) (*FetchResultsResp, error)
	GetResultsMetadata(context.Context, *GetResultsMetadataReq) (*GetResultsMetadataResp, error)
	ExecuteStatement(context.Context, *ExecuteStatementReq) (*ExecuteStatementResp, error)
	GetExecutionStatus(context.Context, *GetExecutionStatusReq) (*GetExecutionStatusResp, error)
	CloseExecution(context.Context, *CloseExecutionReq) (*CloseExecutionResp, error)
	CancelExecution(context.Context, *CancelExecutionReq) (*CancelExecutionResp, error)
}

type OpenSessionReq struct {
	Schema  string
	Catalog string
}

type OpenSessionResp struct {
	SessionHandle Handle // for now
	Status        *RequestStatus
}
type CloseSessionReq struct {
	SessionHandle Handle // for now
}
type CloseSessionResp struct {
	Status *RequestStatus
}

type Handle interface {
	Id() string
}

type ExecuteStatementReq struct {
	SessionHandle Handle // for now
	Statement     string
}

type ExecuteStatementResp struct {
	Status          *RequestStatus
	ExecutionHandle Handle // todo
	ExecutionStatus
	ExecutionResult
	IsClosed bool
}

type ExecutionStatus struct {
	SqlState        string
	ExecutionState  string
	NumModifiedRows int64
	Error           *ExecutionError
}
type FetchResultsReq struct {
	ExecutionHandle Handle // todo
	MaxRows         int64
	Orientation     cli_service.TFetchOrientation
	ChunkIndex      int
	RowOffset       int64
}
type FetchResultsResp struct {
	Status *RequestStatus
	ExecutionResult
}

type GetResultsMetadataReq struct {
	ExecutionHandle Handle // todo
}

type GetResultsMetadataResp struct {
	Status *RequestStatus
	Schema *ResultSchema // think about multi-statement
}

type GetExecutionStatusReq struct {
	ExecutionHandle Handle // todo
}

type GetExecutionStatusResp struct {
	Status *RequestStatus
	ExecutionStatus
}

type CloseExecutionReq struct {
	ExecutionHandle Handle // todo
}

type CloseExecutionResp struct {
	Status *RequestStatus
}

type CancelExecutionReq struct {
	ExecutionHandle Handle // todo
}

type CancelExecutionResp struct {
	Status *RequestStatus
}

type RequestStatus struct {
	StatusCode string
	Error      *RequestError
}

type ExecutionResult struct {
	Schema *ResultSchema
	Result *ResultData // think about multi-statement
}

type ExecutionError struct {
	ErrorCode string
	Message   string
}

type RequestError struct {
	Message string
}

type ResultSchema struct {
	Columns          []*ColumnInfo
	ArrowSchemaBytes []byte
}

type ColumnInfo struct {
	Name             string
	Position         int
	Type             ColumnTypeId
	TypeIntervalType string
	TypeName         string
	TypePrecision    int
	TypeScale        int
	TypeText         string
}

type ColumnTypeId int

const (
	BOOLEAN_TYPE             ColumnTypeId = 0
	TINYINT_TYPE             ColumnTypeId = 1
	SMALLINT_TYPE            ColumnTypeId = 2
	INT_TYPE                 ColumnTypeId = 3
	BIGINT_TYPE              ColumnTypeId = 4
	FLOAT_TYPE               ColumnTypeId = 5
	DOUBLE_TYPE              ColumnTypeId = 6
	STRING_TYPE              ColumnTypeId = 7
	TIMESTAMP_TYPE           ColumnTypeId = 8
	BINARY_TYPE              ColumnTypeId = 9
	ARRAY_TYPE               ColumnTypeId = 10
	MAP_TYPE                 ColumnTypeId = 11
	STRUCT_TYPE              ColumnTypeId = 12
	UNION_TYPE               ColumnTypeId = 13
	USER_DEFINED_TYPE        ColumnTypeId = 14
	DECIMAL_TYPE             ColumnTypeId = 15
	NULL_TYPE                ColumnTypeId = 16
	DATE_TYPE                ColumnTypeId = 17
	VARCHAR_TYPE             ColumnTypeId = 18
	CHAR_TYPE                ColumnTypeId = 19
	INTERVAL_YEAR_MONTH_TYPE ColumnTypeId = 20
	INTERVAL_DAY_TIME_TYPE   ColumnTypeId = 21
	UNKNOWN_TYPE             ColumnTypeId = 22
)

func (p ColumnTypeId) String() string {
	switch p {
	case BOOLEAN_TYPE:
		return "BOOLEAN"
	case TINYINT_TYPE:
		return "TINYINT"
	case SMALLINT_TYPE:
		return "SMALLINT"
	case INT_TYPE:
		return "INT"
	case BIGINT_TYPE:
		return "BIGINT"
	case FLOAT_TYPE:
		return "FLOAT"
	case DOUBLE_TYPE:
		return "DOUBLE"
	case STRING_TYPE:
		return "STRING"
	case TIMESTAMP_TYPE:
		return "TIMESTAMP"
	case BINARY_TYPE:
		return "BINARY"
	case ARRAY_TYPE:
		return "ARRAY"
	case MAP_TYPE:
		return "MAP"
	case STRUCT_TYPE:
		return "STRUCT"
	case UNION_TYPE:
		return "UNION"
	case USER_DEFINED_TYPE:
		return "USER_DEFINED"
	case DECIMAL_TYPE:
		return "DECIMAL"
	case NULL_TYPE:
		return "NULL"
	case DATE_TYPE:
		return "DATE"
	case VARCHAR_TYPE:
		return "VARCHAR"
	case CHAR_TYPE:
		return "CHAR"
	case INTERVAL_YEAR_MONTH_TYPE:
		return "INTERVAL_YEAR_MONTH"
	case INTERVAL_DAY_TIME_TYPE:
		return "INTERVAL_DAY_TIME"
	}
	return "<UNSET>"
}

func columnTypeFromString(s string) ColumnTypeId {
	for i := 0; i <= 22; i++ {
		ti := ColumnTypeId(i)
		if ti.String() == s {
			return ti
		}
	}

	return UNKNOWN_TYPE
}

type ResultData struct {
	HasMoreRows    bool
	StartRowOffset int64
	Rows           []*cli_service.TRow
	Columns        []*cli_service.TColumn
	ArrowBatches   []*cli_service.TSparkArrowBatch
	ArrowSchema    []byte
	ResultLinks    []*ResultLink
}

type ResultLink struct {
	FileLink       string
	ExpiryTime     int64
	StartRowOffset int64
	RowCount       int64
	BytesNum       int64
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
