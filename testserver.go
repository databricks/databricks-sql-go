package dbsql

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
)

type thriftHandler struct {
	processor               thrift.TProcessor
	inPfactory, outPfactory thrift.TProtocolFactory
}

func (h *thriftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	thriftHandler := thrift.NewThriftHandlerFunc(h.processor, h.inPfactory, h.outPfactory)
	thriftHandler(w, r)
}

func initThriftTestServer(cfg *config.Config, handler cli_service.TCLIService) *httptest.Server {

	// endpoint := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
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
		panic(fmt.Errorf("invalid protocol specified %s", cfg.ThriftProtocol))
	}
	if cfg.ThriftDebugClientProtocol {
		protocolFactory = thrift.NewTDebugProtocolFactoryWithLogger(protocolFactory, "client:", thrift.StdLogger(nil))
	}

	processor := cli_service.NewTCLIServiceProcessor(handler)

	th := thriftHandler{
		processor,
		protocolFactory,
		protocolFactory,
	}

	ts := httptest.NewServer(&th)

	return ts
}

type serverHandler struct {
	// this will force the interface implementation
	cli_service.TCLIService
	openSession          func(ctx context.Context, req *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error)
	executeStatement     func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error)
	fetchResults         func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error)
	getOperationStatus   func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error)
	cancelOperation      func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error)
	getResultSetMetadata func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error)
}

func (h *serverHandler) OpenSession(ctx context.Context, req *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error) {
	if h.openSession != nil {
		return h.openSession(ctx, req)
	}
	return &cli_service.TOpenSessionResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
		SessionHandle: &cli_service.TSessionHandle{
			SessionId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
				Secret: []byte("a"),
			},
		},
	}, nil
}

func (h *serverHandler) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
	if h.executeStatement != nil {
		return h.executeStatement(ctx, req)
	}
	return &cli_service.TExecuteStatementResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
		OperationHandle: &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		},
	}, nil
}

func (h *serverHandler) FetchResults(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
	if h.fetchResults != nil {
		return h.fetchResults(ctx, req)
	}
	return &cli_service.TFetchResultsResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}

func (h *serverHandler) GetOperationStatus(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
	if h.getOperationStatus != nil {
		return h.getOperationStatus(ctx, req)
	}
	return &cli_service.TGetOperationStatusResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}

func (h *serverHandler) CancelOperation(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
	if h.cancelOperation != nil {
		return h.cancelOperation(ctx, req)
	}
	return &cli_service.TCancelOperationResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}

func (h *serverHandler) GetResultSetMetadata(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
	if h.getResultSetMetadata != nil {
		return h.getResultSetMetadata(ctx, req)
	}
	return &cli_service.TGetResultSetMetadataResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}
