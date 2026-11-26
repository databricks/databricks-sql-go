package dbsql

import (
	"fmt"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/v2/errors"
	thriftbackend "github.com/databricks/databricks-sql-go/v2/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	"github.com/stretchr/testify/require"
)

func TestResultRowsAffected(t *testing.T) {
	zero, count, large := int64(0), int64(42), int64(1<<53+1)
	for _, report := range []bool{false, true} {
		for _, test := range []struct {
			name   string
			status *cli_service.TGetOperationStatusResp
			want   int64
		}{
			{"nil status", nil, 0},
			{"unset count", &cli_service.TGetOperationStatusResp{}, 0},
			{"reported zero", &cli_service.TGetOperationStatusResp{NumModifiedRows: &zero}, 0},
			{"reported count", &cli_service.TGetOperationStatusResp{NumModifiedRows: &count}, 42},
			{"large count", &cli_service.TGetOperationStatusResp{NumModifiedRows: &large}, large},
		} {
			t.Run(fmt.Sprintf("report=%t/%s", report, test.name), func(t *testing.T) {
				be := thriftbackend.NewForTest(&client.TestClient{}, nil, config.WithDefaults())
				res := newResult(be.OperationForTest(nil, test.status), report)
				for range 2 {
					got, err := res.RowsAffected()
					require.Equal(t, test.want, got)
					if report && (test.status == nil || test.status.NumModifiedRows == nil) {
						require.ErrorIs(t, err, dbsqlerr.ErrRowsAffectedUnknown)
					} else {
						require.NoError(t, err)
					}
					insertID, err := res.LastInsertId()
					require.NoError(t, err)
					require.Zero(t, insertID)
				}
			})
		}
	}
}
