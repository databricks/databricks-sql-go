//go:build e2e

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/backend"
	thriftbackend "github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestE2EGeospatialTypeNames(t *testing.T) {
	host, httpPath, token := pecoTestingCreds(t)
	connectorValue, err := NewConnector(WithServerHostname(host), WithHTTPPath(httpPath), WithAccessToken(token))
	require.NoError(t, err)
	capture := &timeWireClient{}
	connectorValue.(*connector).thriftBackendFactory = func(ctx context.Context, cfg *config.Config, httpClient *http.Client) (backend.Backend, error) {
		service, err := client.InitThriftClient(cfg, httpClient)
		if err != nil {
			return nil, err
		}
		capture.TCLIService = service
		return thriftbackend.NewForTest(capture, nil, cfg), nil
	}
	db := sql.OpenDB(connectorValue)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assertGeospatialColumnTypes(t, db, false)
	capture.mu.Lock()
	defer capture.mu.Unlock()
	cloudFetch := false
	for _, metadata := range capture.metadata {
		cloudFetch = cloudFetch || metadata.GetResultFormat() == cli_service.TSparkRowSetType_URL_BASED_SET
		reader, err := ipc.NewReader(bytes.NewReader(metadata.ArrowSchema))
		require.NoError(t, err)
		field := reader.Schema().Field(0)
		reader.Release()
		name, _ := field.Metadata.GetValue("Spark:DataType:SqlName")
		t.Logf("wire type=%s SQL name=%q format=%s", field.Type, name, metadata.GetResultFormat())
		require.Equal(t, "GEOMETRY(0)", name)
	}
	require.True(t, cloudFetch, "server must return URL_BASED_SET")
	require.True(t, capture.links, "server must return CloudFetch links")
}

func assertGeospatialColumnTypes(t *testing.T, db *sql.DB, native bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const projection = `st_geomfromtext('POINT(1 2)',0) AS geom,
		st_geogfromtext('POINT(1 2)') AS geog,
		st_geomfromtext('POINT(1 2)',3857) AS projected,
		CAST(st_geomfromtext('POINT(1 2)',0) AS GEOMETRY(ANY)) AS mixed,
		sha2(CAST(id AS STRING),512) AS plain,
		named_struct('g',st_geomfromtext('POINT(1 2)',0)) AS nested`
	wantNames := []string{"GEOMETRY(0)", "GEOGRAPHY(4326)", "GEOMETRY(3857)", "GEOMETRY(ANY)", "STRING", "STRUCT"}
	for _, test := range []struct {
		name       string
		projection string
		count      int
		null       bool
	}{
		{"inline", projection, 1, false},
		{"empty", projection, 0, false},
		{"NULL only", `CAST(NULL AS GEOMETRY(0)), CAST(NULL AS GEOGRAPHY(4326)),
			CAST(NULL AS GEOMETRY(3857)), CAST(NULL AS GEOMETRY(ANY)),
			CAST(NULL AS STRING), CAST(NULL AS STRUCT<g:GEOMETRY(0)>)`, 3, true},
		{"CloudFetch", projection, 100000, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM range(%d)", test.projection, test.count))
			require.NoError(t, err)
			defer result.Close() //nolint:errcheck
			columns, err := result.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, len(wantNames))
			for index, want := range wantNames {
				require.Equal(t, want, columns[index].DatabaseTypeName(), "column %d", index)
				if index < 4 {
					wantScan := reflect.TypeOf("")
					if native {
						wantScan = reflect.TypeOf(sql.RawBytes{})
					}
					require.Equal(t, wantScan, columns[index].ScanType())
					length, hasLength := columns[index].Length()
					plainLength, plainHasLength := columns[4].Length()
					require.Equal(t, plainLength, length)
					require.Equal(t, plainHasLength, hasLength)
				}
			}
			count := 0
			for result.Next() {
				var values [6]any
				require.NoError(t, result.Scan(&values[0], &values[1], &values[2], &values[3], &values[4], &values[5]))
				if test.null {
					for _, value := range values {
						require.Nil(t, value)
					}
				} else if !native {
					for index, want := range []string{"POINT(1 2)", "SRID=4326;POINT(1 2)", "SRID=3857;POINT(1 2)", "POINT(1 2)"} {
						require.Equal(t, want, values[index])
					}
				} else {
					for _, value := range values[:4] {
						require.IsType(t, []byte{}, value)
					}
				}
				count++
			}
			require.NoError(t, result.Err())
			require.Equal(t, test.count, count)
		})
	}
	connection, err := db.Conn(ctx)
	require.NoError(t, err)
	defer connection.Close() //nolint:errcheck
	require.NoError(t, connection.Raw(func(connection any) error {
		raw, err := connection.(driver.QueryerContext).QueryContext(ctx, "SELECT "+projection+" FROM range(1)", nil)
		if err != nil {
			return err
		}
		defer raw.Close() //nolint:errcheck
		batches, err := raw.(dbsqlrows.Rows).GetArrowBatches(ctx)
		if err != nil {
			return err
		}
		defer batches.Close()
		schema, err := batches.Schema()
		if err != nil {
			return err
		}
		for index := range 4 {
			field := schema.Field(index)
			t.Logf("Arrow column %d physical=%s metadata=%v", index, field.Type, field.Metadata)
			name, ok := field.Metadata.GetValue("Spark:DataType:SqlName")
			require.True(t, ok)
			require.Equal(t, wantNames[index], name)
			wantPhysical := arrow.STRING
			if native {
				wantPhysical = arrow.STRUCT
			}
			require.Equal(t, wantPhysical, field.Type.ID())
		}
		return nil
	}))
}
