package hive

import (
	"errors"
	"fmt"
	"log"

	"github.com/databricks/databricks-sql-go/cli_service"
)

const (
	// TimestampFormat is JDBC compliant timestamp format
	TimestampFormat = "2006-01-02 15:04:05.999999999"
)

// RPCResponse respresents thrift rpc response
type RPCResponse interface {
	GetStatus() *cli_service.TStatus
}

func checkStatus(resp interface{}) error {
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

	log.Printf("response: %v", resp)
	return errors.New("thrift: invalid response")
}

func guid(b []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
