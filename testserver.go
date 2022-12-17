package dbsql

import (
	"net/http"
	"net/http/httptest"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type thriftHandler struct {
	processor               thrift.TProcessor
	inPfactory, outPfactory thrift.TProtocolFactory
}

func (h *thriftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	thriftHandler := thrift.NewThriftHandlerFunc(h.processor, h.inPfactory, h.outPfactory)
	thriftHandler(w, r)
}

func initThriftTestServer(handler cli_service.TCLIService) *httptest.Server {

	tcfg := &thrift.TConfiguration{
		TLSConfig: nil,
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(tcfg)

	processor := cli_service.NewTCLIServiceProcessor(handler)

	th := thriftHandler{
		processor,
		protocolFactory,
		protocolFactory,
	}

	ts := httptest.NewServer(&th)

	return ts
}
