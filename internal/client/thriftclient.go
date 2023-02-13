package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

type ThriftServiceClient struct {
	*cli_service.TCLIServiceClient
	cfg *config.Config
}

// OpenSession is a wrapper around the thrift operation OpenSession
// If RecordResults is true, the results will be marshalled to JSON format and written to OpenSession<index>.json
func (tsc *ThriftServiceClient) OpenSession(ctx context.Context, req *OpenSessionReq) (*OpenSessionResp, error) {
	msg, start := logger.Track("OpenSession")
	var catalogName *cli_service.TIdentifier
	var schemaName *cli_service.TIdentifier
	if tsc.cfg.Catalog != "" {
		catalogName = cli_service.TIdentifierPtr(cli_service.TIdentifier(tsc.cfg.Catalog))
	}
	if tsc.cfg.Schema != "" {
		schemaName = cli_service.TIdentifierPtr(cli_service.TIdentifier(tsc.cfg.Schema))
	}
	protocolVersion := int64(tsc.cfg.ThriftProtocolVersion)
	resp, err := tsc.TCLIServiceClient.OpenSession(ctx, &cli_service.TOpenSessionReq{
		ClientProtocolI64: &protocolVersion,
		Configuration:     make(map[string]string),
		InitialNamespace: &cli_service.TNamespace{
			CatalogName: catalogName,
			SchemaName:  schemaName,
		},
		CanUseMultipleCatalogs: &tsc.cfg.CanUseMultipleCatalogs,
	})
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
	return &OpenSessionResp{
		SessionHandle: resp.SessionHandle,
		Status:        toRequestStatus(resp.Status),
	}, CheckStatus(resp)
}

// CloseSession is a wrapper around the thrift operation CloseSession
// If RecordResults is true, the results will be marshalled to JSON format and written to CloseSession<index>.json
func (tsc *ThriftServiceClient) CloseSession(ctx context.Context, req *CloseSessionReq) (*CloseSessionResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), "")
	defer log.Duration(logger.Track("CloseSession"))
	resp, err := tsc.TCLIServiceClient.CloseSession(ctx, &cli_service.TCloseSessionReq{
		SessionHandle: req.SessionHandle,
	})

	if err != nil {
		return nil, errors.Wrap(err, "close session request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("CloseSession%d.json", resultIndex), j, 0600)
		resultIndex++
	}

	return &CloseSessionResp{
		Status: toRequestStatus(resp.Status),
	}, CheckStatus(resp)
}

// FetchResults is a wrapper around the thrift operation FetchResults
// If RecordResults is true, the results will be marshalled to JSON format and written to FetchResults<index>.json
func (tsc *ThriftServiceClient) FetchResults(ctx context.Context, req *FetchResultsReq) (*FetchResultsResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.ExecutionHandle.OperationId.GUID))
	defer log.Duration(logger.Track("FetchResults"))
	resp, err := tsc.TCLIServiceClient.FetchResults(ctx, &cli_service.TFetchResultsReq{
		OperationHandle: req.ExecutionHandle,
		MaxRows:         req.MaxRows,
		Orientation:     req.Orientation,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch results request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("FetchResults%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return &FetchResultsResp{
		Status: toRequestStatus(resp.Status),
		Schema: toSchema(resp.ResultSetMetadata.Schema),
		Result: toResult(resp.Results, *resp.HasMoreRows),
	}, CheckStatus(resp)
}

// GetResultSetMetadata is a wrapper around the thrift operation GetResultSetMetadata
// If RecordResults is true, the results will be marshalled to JSON format and written to GetResultSetMetadata<index>.json
func (tsc *ThriftServiceClient) GetResultsMetadata(ctx context.Context, req *GetResultsMetadataReq) (*GetResultsMetadataResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.ExecutionHandle.OperationId.GUID))
	defer log.Duration(logger.Track("GetResultSetMetadata"))
	resp, err := tsc.TCLIServiceClient.GetResultSetMetadata(ctx, &cli_service.TGetResultSetMetadataReq{
		OperationHandle: req.ExecutionHandle,
	})
	if err != nil {
		return nil, errors.Wrap(err, "get result set metadata request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("ExecuteStatement%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return &GetResultsMetadataResp{
		Status: toRequestStatus(resp.Status),
		Schema: toSchema(resp.Schema),
	}, CheckStatus(resp)
}

// ExecuteStatement is a wrapper around the thrift operation ExecuteStatement
// If RecordResults is true, the results will be marshalled to JSON format and written to ExecuteStatement<index>.json
func (tsc *ThriftServiceClient) ExecuteStatement(ctx context.Context, req *ExecuteStatementReq) (*ExecuteStatementResp, error) {
	msg, start := logger.Track("ExecuteStatement")
	resp, err := tsc.TCLIServiceClient.ExecuteStatement(context.Background(), &cli_service.TExecuteStatementReq{
		SessionHandle: req.SessionHandle,
		Statement:     req.Statement,
		RunAsync:      tsc.cfg.RunAsync,
		QueryTimeout:  int64(tsc.cfg.QueryTimeout / time.Second),
		GetDirectResults: &cli_service.TSparkGetDirectResults{
			MaxRows: int64(tsc.cfg.MaxRows),
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "execute statement request error")
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
	var sqlState, executionState string
	var numModifiedRows int64
	var execErr *ExecutionError
	var result *ResultData
	var schema *ResultSchema

	if resp.DirectResults != nil {
		if resp.DirectResults.OperationStatus.SqlState != nil {
			sqlState = *resp.DirectResults.OperationStatus.SqlState
		}
		if resp.DirectResults.OperationStatus.NumModifiedRows != nil {
			numModifiedRows = *resp.DirectResults.OperationStatus.NumModifiedRows
		}
		executionState = toExecutionState(resp.DirectResults.OperationStatus.OperationState)
		if resp.DirectResults.OperationStatus.Status.StatusCode == cli_service.TStatusCode_ERROR_STATUS {
			execErr = &ExecutionError{
				Message: *resp.DirectResults.OperationStatus.ErrorMessage,
			}
		}
		if resp.DirectResults.ResultSet != nil {
			result = toResult(resp.DirectResults.ResultSet.Results, *resp.DirectResults.ResultSet.HasMoreRows)
		}
		if resp.DirectResults.ResultSetMetadata != nil {
			schema = toSchema(resp.DirectResults.ResultSetMetadata.Schema)
		}
	}

	return &ExecuteStatementResp{
		Status:          toRequestStatus(resp.Status),
		ExecutionHandle: resp.OperationHandle,
		Result:          result,
		Schema:          schema,
		ExecutionStatus: ExecutionStatus{
			SqlState:        sqlState,
			NumModifiedRows: numModifiedRows,
			ExecutionState:  executionState,
			Error:           execErr,
		},
	}, CheckStatus(resp)
}

// GetOperationStatus is a wrapper around the thrift operation GetOperationStatus
// If RecordResults is true, the results will be marshalled to JSON format and written to GetOperationStatus<index>.json
func (tsc *ThriftServiceClient) GetExecutionStatus(ctx context.Context, req *GetExecutionStatusReq) (*GetExecutionStatusResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.ExecutionHandle.OperationId.GUID))
	defer log.Duration(logger.Track("GetOperationStatus"))
	resp, err := tsc.TCLIServiceClient.GetOperationStatus(ctx, &cli_service.TGetOperationStatusReq{
		OperationHandle: req.ExecutionHandle,
	})
	if err != nil {
		return nil, errors.Wrap(err, "get operation status request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("GetOperationStatus%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return &GetExecutionStatusResp{
		Status: toRequestStatus(resp.Status),
		ExecutionStatus: ExecutionStatus{
			SqlState:        *resp.SqlState,
			NumModifiedRows: *resp.NumModifiedRows,
			ExecutionState:  toExecutionState(resp.OperationState),
			Error:           &ExecutionError{},
		},
	}, CheckStatus(resp)
}

// CloseOperation is a wrapper around the thrift operation CloseOperation
// If RecordResults is true, the results will be marshalled to JSON format and written to CloseOperation<index>.json
func (tsc *ThriftServiceClient) CloseExecution(ctx context.Context, req *CloseExecutionReq) (*CloseExecutionResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.ExecutionHandle.OperationId.GUID))
	defer log.Duration(logger.Track("CloseOperation"))
	resp, err := tsc.TCLIServiceClient.CloseOperation(ctx, &cli_service.TCloseOperationReq{
		OperationHandle: req.ExecutionHandle,
	})
	if err != nil {
		return nil, errors.Wrap(err, "close operation request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("CloseOperation%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return &CloseExecutionResp{
		Status: toRequestStatus(resp.Status),
	}, CheckStatus(resp)
}

// CancelOperation is a wrapper around the thrift operation CancelOperation
// If RecordResults is true, the results will be marshalled to JSON format and written to CancelOperation<index>.json
func (tsc *ThriftServiceClient) CancelExecution(ctx context.Context, req *CancelExecutionReq) (*CancelExecutionResp, error) {
	log := logger.WithContext(driverctx.ConnIdFromContext(ctx), driverctx.CorrelationIdFromContext(ctx), SprintGuid(req.ExecutionHandle.OperationId.GUID))
	defer log.Duration(logger.Track("CancelOperation"))
	resp, err := tsc.TCLIServiceClient.CancelOperation(ctx, &cli_service.TCancelOperationReq{
		OperationHandle: req.ExecutionHandle,
	})
	if err != nil {
		return nil, errors.Wrap(err, "cancel operation request error")
	}
	if RecordResults {
		j, _ := json.MarshalIndent(resp, "", " ")
		_ = os.WriteFile(fmt.Sprintf("CancelOperation%d.json", resultIndex), j, 0600)
		resultIndex++
	}
	return &CancelExecutionResp{
		Status: toRequestStatus(resp.Status),
	}, CheckStatus(resp)
}

var _ DatabricksClient = (*ThriftServiceClient)(nil)

// InitThriftClient is a wrapper of the http transport, so we can have access to response code and headers.
// It is important to know the code and headers to know if we need to retry or not
func InitThriftClient(cfg *config.Config, httpclient *http.Client) (DatabricksClient, error) {
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
	tsClient := &ThriftServiceClient{tclient, cfg}
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

func toRequestStatus(s *cli_service.TStatus) *RequestStatus {
	var reqErr *RequestError
	if s.StatusCode == cli_service.TStatusCode_ERROR_STATUS {
		reqErr = &RequestError{Message: *s.DisplayMessage}
	}
	return &RequestStatus{
		Error:      reqErr,
		StatusCode: s.StatusCode.String(),
	}
}

func toSchema(s *cli_service.TTableSchema) *ResultSchema {
	cols := []*ColumnInfo{}
	for _, c := range s.Columns {
		entry := c.TypeDesc.Types[0].PrimitiveEntry
		dbtype := entry.Type.String()
		cols = append(cols, &ColumnInfo{
			Name:     c.ColumnName,
			Position: int(c.Position),
			TypeName: dbtype,
		})
	}
	return &ResultSchema{
		Columns: cols,
	}
}

func toResult(r *cli_service.TRowSet, hasMoreRows bool) *ResultData {
	return &ResultData{
		StartRowOffset: r.StartRowOffset,
		Rows:           r.Rows,
		Columns:        r.Columns,
		ArrowBatches:   r.ArrowBatches,
		ResultLinks:    r.ResultLinks,
		HasMoreRows:    hasMoreRows,
	}
}

func toExecutionState(s *cli_service.TOperationState) string {
	return s.String() //TODO
}

// SprintGuid is a convenience function to format a byte array into GUID.
func SprintGuid(bts []byte) string {
	if len(bts) == 16 {
		return fmt.Sprintf("%x-%x-%x-%x-%x", bts[0:4], bts[4:6], bts[6:8], bts[8:10], bts[10:16])
	}
	logger.Warn().Msgf("GUID not valid: %x", bts)
	return fmt.Sprintf("%x", bts)
}
