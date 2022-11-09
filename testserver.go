package dbsql

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/apache/thrift/lib/go/thrift"
	ts "github.com/databricks/databricks-sql-go/internal/cli_service"
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

func initThriftTestServer(cfg *config.Config, handler ts.TCLIService) *httptest.Server {

	// endpoint := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
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
		panic(fmt.Errorf("invalid protocol specified %s", cfg.Thrift.Protocol))
	}
	if cfg.Thrift.DebugClientProtocol {
		protocolFactory = thrift.NewTDebugProtocolFactoryWithLogger(protocolFactory, "client:", thrift.StdLogger(nil))
	}

	processor := ts.NewTCLIServiceProcessor(handler)

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
	ts.TCLIService
	openSession          func(ctx context.Context, req *ts.TOpenSessionReq) (*ts.TOpenSessionResp, error)
	executeStatement     func(ctx context.Context, req *ts.TExecuteStatementReq) (*ts.TExecuteStatementResp, error)
	fetchResults         func(ctx context.Context, req *ts.TFetchResultsReq) (_r *ts.TFetchResultsResp, _err error)
	getOperationStatus   func(ctx context.Context, req *ts.TGetOperationStatusReq) (*ts.TGetOperationStatusResp, error)
	cancelOperation      func(ctx context.Context, req *ts.TCancelOperationReq) (*ts.TCancelOperationResp, error)
	getResultSetMetadata func(ctx context.Context, req *ts.TGetResultSetMetadataReq) (*ts.TGetResultSetMetadataResp, error)
}

func (h *serverHandler) OpenSession(ctx context.Context, req *ts.TOpenSessionReq) (*ts.TOpenSessionResp, error) {
	if h.openSession != nil {
		return h.openSession(ctx, req)
	}
	return &ts.TOpenSessionResp{
		Status: &ts.TStatus{
			StatusCode: ts.TStatusCode_SUCCESS_STATUS,
		},
		SessionHandle: &ts.TSessionHandle{
			SessionId: &ts.THandleIdentifier{
				GUID:   []byte("1"),
				Secret: []byte("a"),
			},
		},
	}, nil
}

func (h *serverHandler) ExecuteStatement(ctx context.Context, req *ts.TExecuteStatementReq) (*ts.TExecuteStatementResp, error) {
	if h.executeStatement != nil {
		return h.executeStatement(ctx, req)
	}
	return &ts.TExecuteStatementResp{
		Status: &ts.TStatus{
			StatusCode: ts.TStatusCode_SUCCESS_STATUS,
		},
		OperationHandle: &ts.TOperationHandle{
			OperationId: &ts.THandleIdentifier{
				GUID:   []byte("2"),
				Secret: []byte("b"),
			},
		},
	}, nil
}

func (h *serverHandler) FetchResults(ctx context.Context, req *ts.TFetchResultsReq) (*ts.TFetchResultsResp, error) {
	if h.fetchResults != nil {
		return h.fetchResults(ctx, req)
	}
	return &ts.TFetchResultsResp{
		Status: &ts.TStatus{
			StatusCode: ts.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}

func (h *serverHandler) GetOperationStatus(ctx context.Context, req *ts.TGetOperationStatusReq) (*ts.TGetOperationStatusResp, error) {
	if h.getOperationStatus != nil {
		return h.getOperationStatus(ctx, req)
	}
	return &ts.TGetOperationStatusResp{
		Status: &ts.TStatus{
			StatusCode: ts.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}

func (h *serverHandler) CancelOperation(ctx context.Context, req *ts.TCancelOperationReq) (*ts.TCancelOperationResp, error) {
	if h.cancelOperation != nil {
		return h.cancelOperation(ctx, req)
	}
	return &ts.TCancelOperationResp{
		Status: &ts.TStatus{
			StatusCode: ts.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}

func (h *serverHandler) GetResultSetMetadata(ctx context.Context, req *ts.TGetResultSetMetadataReq) (*ts.TGetResultSetMetadataResp, error) {
	if h.getResultSetMetadata != nil {
		return h.getResultSetMetadata(ctx, req)
	}
	return &ts.TGetResultSetMetadataResp{
		Status: &ts.TStatus{
			StatusCode: ts.TStatusCode_SUCCESS_STATUS,
		},
	}, nil
}
