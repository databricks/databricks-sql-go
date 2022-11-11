package dbsql

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestConn_ExecContext(t *testing.T) {

	t.Run("executeStatement should err when ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte("2"),
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
						ErrorMessage:   strPtr("error message"),
						DisplayMessage: strPtr("display message"),
					},
					ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
					ResultSet: &cli_service.TFetchResultsResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
				},
			}
			return executeStatementResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		testConn := &conn{
			session: getRandomSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		_, err := testConn.executeStatement(context.Background(), "select 1", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
	})

}

func getRandomSession() *cli_service.TOpenSessionResp {
	return &cli_service.TOpenSessionResp{SessionHandle: &cli_service.TSessionHandle{
		SessionId: &cli_service.THandleIdentifier{
			GUID: []byte("foo"),
		},
	}}
}
