package featureflags

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

func TestFetchFeatureFlagsWithRetryableClient(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				attempts.Add(1)
				writer.Header().Set("Retry-After", "60")
				writer.WriteHeader(status)
			}))
			defer server.Close()
			httpClient := client.RetryableClient(&config.Config{
				Authenticator: &noop.NoopAuth{},
				RetryMax:      4,
				RetryWaitMin:  time.Millisecond,
				RetryWaitMax:  time.Millisecond,
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _, err := fetchFeatureFlags(ctx, Request{Host: server.URL, HTTPClient: httpClient})
			require.Error(t, err)
			require.NoError(t, ctx.Err())
			require.Equal(t, int32(1), attempts.Load())
		})
	}
}
