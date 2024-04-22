package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCloudURLFetch(t *testing.T) {

	var handler func(w http.ResponseWriter, r *http.Request)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	}))
	defer server.Close()
	testTable := []struct {
		name             string
		response         func(w http.ResponseWriter, r *http.Request)
		linkExpired      bool
		expectedResponse SparkArrowBatch
		expectedErr      error
	}{
		{
			name: "cloud-fetch-happy-case",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
				if err != nil {
					panic(err)
				}
			},
			linkExpired: false,
			expectedResponse: &sparkArrowBatch{
				Delimiter: rowscanner.NewDelimiter(0, 3),
				arrowRecords: []SparkArrowRecord{
					&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 3), Record: generateArrowRecord()},
					&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(3, 3), Record: generateArrowRecord()},
				},
			},
			expectedErr: nil,
		},
		{
			name: "cloud-fetch-expired_link",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
				if err != nil {
					panic(err)
				}
			},
			linkExpired:      true,
			expectedResponse: nil,
			expectedErr:      errors.New(dbsqlerr.ErrLinkExpired),
		},
		{
			name: "cloud-fetch-http-error",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			linkExpired:      false,
			expectedResponse: nil,
			expectedErr:      dbsqlerrint.NewDriverError(context.TODO(), errArrowRowsCloudFetchDownloadFailure, nil),
		},
	}

	for _, tc := range testTable {
		t.Run(tc.name, func(t *testing.T) {
			handler = tc.response

			expiryTime := time.Now()
			// If link expired, subtract 1 sec from current time to get expiration time
			if tc.linkExpired {
				expiryTime = expiryTime.Add(-1 * time.Second)
			} else {
				expiryTime = expiryTime.Add(10 * time.Second)
			}

			cu := &cloudURL{
				Delimiter:  rowscanner.NewDelimiter(0, 3),
				fileLink:   server.URL,
				expiryTime: expiryTime.Unix(),
			}

			ctx := context.Background()

			resp, err := cu.Fetch(ctx)

			if tc.expectedResponse != nil {
				assert.NotNil(t, resp)
				esab, ok := tc.expectedResponse.(*sparkArrowBatch)
				assert.True(t, ok)
				asab, ok2 := resp.(*sparkArrowBatch)
				assert.True(t, ok2)
				if !reflect.DeepEqual(esab.Delimiter, asab.Delimiter) {
					t.Errorf("expected (%v), got (%v)", esab.Delimiter, asab.Delimiter)
				}
				assert.Equal(t, len(esab.arrowRecords), len(asab.arrowRecords))
				for i := range esab.arrowRecords {
					er := esab.arrowRecords[i]
					ar := asab.arrowRecords[i]

					eb := generateMockArrowBytes(er)
					ab := generateMockArrowBytes(ar)
					assert.Equal(t, eb, ab)
				}
			}

			if !errors.Is(err, tc.expectedErr) {
				assert.EqualErrorf(t, err, fmt.Sprintf("%v", tc.expectedErr), "expected (%v), got (%v)", tc.expectedErr, err)
			}
		})
	}
}

func generateArrowRecord() arrow.Record {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)

	record := builder.NewRecord()

	return record
}

func generateMockArrowBytes(record arrow.Record) []byte {

	defer record.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return buf.Bytes()
}
