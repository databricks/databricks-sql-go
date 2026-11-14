package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

func TestTimeScanDestinations(t *testing.T) {
	for _, test := range []struct {
		dataType arrow.DataType
		clock    string
		nanos    int
		empty    bool
	}{
		{arrow.FixedWidthTypes.Time32s, "09:15:00", 0, false},
		{arrow.FixedWidthTypes.Time32ms, "09:15:00.123", 123000000, false},
		{arrow.FixedWidthTypes.Time64us, "09:15:00.123456", 123456000, false},
		{arrow.FixedWidthTypes.Time64ns, "09:15:00.123456", 123456000, false},
		{arrow.FixedWidthTypes.Time64ns, "09:15:00.123456789", 123456789, false},
		{arrow.FixedWidthTypes.Time64ns, "00:00:00", 0, false},
		{arrow.FixedWidthTypes.Time64ns, "empty", 0, true},
	} {
		t.Run(test.dataType.String()+"/"+test.clock, func(t *testing.T) {
			state := &callState{}
			loadTestData(t, "OpenSessionSuccess.json", &state.openSessionResp)
			loadTestData(t, "CloseSessionSuccess.json", &state.closeSessionResp)
			loadTestData(t, "CloseOperationSuccess.json", &state.closeOperationResp)
			loadTestData(t, "ExecuteStatement5.json", &state.executeStatementResp)
			metadata := state.executeStatementResp.DirectResults.ResultSetMetadata
			metadata.Schema.Columns = nil
			fields := []arrow.Field{
				{Name: "typed", Type: test.dataType}, {Name: "dynamic", Type: test.dataType},
				{Name: "text", Type: test.dataType}, {Name: "missing", Type: test.dataType, Nullable: true},
				{Name: "sibling", Type: arrow.BinaryTypes.String},
				{Name: "missing_any", Type: test.dataType, Nullable: true},
				{Name: "nullable", Type: test.dataType, Nullable: true},
			}
			for index, field := range fields {
				metadata.Schema.Columns = append(metadata.Schema.Columns, &cli_service.TColumnDesc{
					ColumnName: field.Name, Position: int32(index + 1),
					TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_STRING_TYPE}}}},
				})
			}
			schema := arrow.NewSchema(fields, nil)
			builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer builder.Release()
			if !test.empty {
				for index := range 3 {
					require.NoError(t, builder.Field(index).AppendValueFromString(test.clock))
				}
				builder.Field(3).AppendNull()
				require.NoError(t, builder.Field(4).AppendValueFromString("still readable"))
				builder.Field(5).AppendNull()
				require.NoError(t, builder.Field(6).AppendValueFromString(test.clock))
			}
			record := builder.NewRecordBatch()
			defer record.Release()
			var schemaBytes, stream bytes.Buffer
			schemaWriter := ipc.NewWriter(&schemaBytes, ipc.WithSchema(schema))
			require.NoError(t, schemaWriter.Close())
			metadata.ArrowSchema = schemaBytes.Bytes()[:schemaBytes.Len()-8]
			streamWriter := ipc.NewWriter(&stream, ipc.WithSchema(schema))
			require.NoError(t, streamWriter.Write(record))
			require.NoError(t, streamWriter.Close())
			format := cli_service.TSparkRowSetType_ARROW_BASED_SET
			metadata.ResultFormat = &format
			state.executeStatementResp.DirectResults.ResultSet.Results = &cli_service.TRowSet{
				ArrowBatches: []*cli_service.TSparkArrowBatch{{Batch: stream.Bytes()[len(metadata.ArrowSchema):], RowCount: 1}},
			}
			if test.empty {
				state.executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches = []*cli_service.TSparkArrowBatch{}
			}
			server := getServer(state)
			defer server.Close()
			address, err := url.Parse(server.URL)
			require.NoError(t, err)
			port, err := strconv.Atoi(address.Port())
			require.NoError(t, err)
			connector, err := NewConnector(WithServerHostname("localhost"), WithPort(port), WithSessionParams(map[string]string{"timezone": "America/Los_Angeles"}))
			require.NoError(t, err)
			db := sql.OpenDB(connector)
			defer db.Close() //nolint:errcheck
			rows, err := db.QueryContext(context.Background(), "SELECT clock")
			require.NoError(t, err)
			defer rows.Close() //nolint:errcheck
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			for _, column := range columns[:4] {
				require.Equal(t, "TIME", column.DatabaseTypeName())
				require.Equal(t, reflect.TypeOf(time.Time{}), column.ScanType())
				_, hasLength := column.Length()
				require.False(t, hasLength)
			}
			require.Equal(t, "STRING", columns[4].DatabaseTypeName())
			if test.empty {
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
				return
			}
			require.True(t, rows.Next(), "%v", rows.Err())
			var typed time.Time
			var dynamic any
			var missingAny any = "not null"
			var text, sibling string
			var missing, nullable sql.NullTime
			require.NoError(t, rows.Scan(&typed, &dynamic, &text, &missing, &sibling, &missingAny, &nullable))
			want := time.Date(1, time.January, 1, 9, 15, 0, test.nanos, time.UTC)
			if test.clock == "00:00:00" {
				want = time.Time{}
				require.True(t, typed.IsZero())
				encoded, err := json.Marshal(struct {
					Clock time.Time `json:"clock,omitzero"`
				}{Clock: typed})
				require.NoError(t, err)
				require.JSONEq(t, `{}`, string(encoded))
			}
			require.Equal(t, want, typed)
			require.Equal(t, want, dynamic)
			require.Equal(t, want.Format(time.RFC3339Nano), text)
			require.False(t, missing.Valid)
			require.True(t, nullable.Valid)
			require.Equal(t, want, nullable.Time)
			require.Nil(t, missingAny)
			require.Equal(t, "still readable", sibling)
			columns, err = rows.ColumnTypes()
			require.NoError(t, err)
			require.Equal(t, "TIME", columns[0].DatabaseTypeName())
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}
