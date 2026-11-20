package arrowbased

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchClientTimeoutRetries(t *testing.T) {
	for _, stall := range []string{"headers", "body"} {
		t.Run(stall, func(t *testing.T) {
			payload := generateMockArrowBytes(generateArrowRecord())
			var attempts atomic.Int32
			cancelled := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if attempts.Add(1) == 1 {
					if stall == "body" {
						w.Header().Set("Content-Length", "100")
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
					close(cancelled)
					return
				}
				_, _ = w.Write(payload)
			}))
			defer server.Close()
			cfg := config.WithDefaults()
			cfg.ClientTimeout = 100 * time.Millisecond
			cfg.UseLz4Compression = false
			cfg.RetryMax = 1
			cfg.RetryWaitMin = time.Millisecond
			cfg.RetryWaitMax = time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			iterator, err := NewCloudBatchIterator(ctx, []*cli_service.TSparkArrowResultLink{{
				FileLink: server.URL, ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 6,
			}}, 0, nil, cfg, nil)
			require.NoError(t, err)
			defer iterator.Close()
			records, nextErr := iterator.Next()
			require.NoError(t, nextErr)
			records.Close()
			assert.Equal(t, int32(2), attempts.Load())
			select {
			case <-cancelled:
			case <-ctx.Done():
				t.Fatal("timed-out request was not cancelled")
			}
		})
	}
}
