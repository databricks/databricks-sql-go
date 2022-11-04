package dbsql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

// a few important test cases

// 1. query canceled in context
// 2. query timeout from server
// 3. http client timeout?
// 4.

func TestCancelOperation(t *testing.T) {
	t.Skip("not ready")
	t.Run("context timeout and cancel success", func(t *testing.T) {

		cfg := newConfigWithDefaults()
		cfg.Host = "localhost"
		cfg.Port = 8083
		cfg.RunAsync = true
		// set up server

		var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			time.Sleep(time.Second)
			return &cli_service.TExecuteStatementResp{
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
					},
				},
			}, nil
		}

		var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
			// isOperationCanceled = true
			return &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}, nil
		}

		var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return &cli_service.TGetOperationStatusResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
				},
			}, nil
		}

		srv := initThriftTestServer(cfg, &serverHandler{
			executeStatement:   executeStatement,
			getOperationStatus: getOperationStatus,
			cancelOperation:    cancelOperation,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour)
		defer cancel()
		defer func() {
			if err := srv.Shutdown(ctx); err != nil {
				fmt.Println(err)
			}
		}()

		connector := &connector{cfg}

		db := sql.OpenDB(connector)

		db.SetConnMaxLifetime(0)
		db.SetMaxIdleConns(5)
		db.SetMaxOpenConns(5)

		var res int
		err1 := db.QueryRowContext(ctx, `select 2 * 3 as res;`).Scan(&res)

		fmt.Println(err1)
	})
}
