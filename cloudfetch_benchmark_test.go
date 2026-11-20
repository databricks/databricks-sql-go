package dbsql

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/arrowbased"
)

func BenchmarkCloudFetchDownload(b *testing.B) {
	payload := bytes.Repeat([]byte("cloudfetch"), 512*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	links := []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 1}}
	for _, transport := range []string{"default", "WithTransport"} {
		b.Run(transport, func(b *testing.B) {
			var options []ConnOption
			if transport == "WithTransport" {
				options = append(options, WithTransport(http.DefaultTransport))
			}
			con, err := NewConnector(options...)
			if err != nil {
				b.Fatal(err)
			}
			cfg := con.(*connector).cfg
			cfg.UseLz4Compression = false
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for range b.N {
				iterator, err := arrowbased.NewCloudIPCStreamIterator(context.Background(), links, 0, cfg, nil)
				if err != nil {
					b.Fatal(err)
				}
				stream, nextErr := iterator.Next()
				if nextErr != nil {
					iterator.Close()
					b.Fatal(nextErr)
				}
				_, readErr := io.Copy(io.Discard, stream)
				iterator.Close()
				if readErr != nil {
					b.Fatal(readErr)
				}
			}
		})
	}
}
