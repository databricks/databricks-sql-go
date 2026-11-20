package arrowbased

import (
	"bytes"
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchDefaultClientTransportHonored(t *testing.T) {
	payload := generateMockArrowBytes(generateArrowRecord())
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirect" {
			http.Redirect(response, request, "/file", http.StatusFound)
			return
		}
		cookie, err := request.Cookie("cloudfetch-test")
		if err != nil || cookie.Value != "preserved" || request.Header.Get("X-Redirect-Policy") != "preserved" {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = response.Write(payload)
	}))
	defer server.Close()
	customClient := server.Client()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	jar.SetCookies(serverURL, []*http.Cookie{{Name: "cloudfetch-test", Value: "preserved", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode}})
	customClient.Jar = jar
	customClient.CheckRedirect = func(request *http.Request, previous []*http.Request) error {
		request.Header.Set("X-Redirect-Policy", "preserved")
		return nil
	}
	originalClient := http.DefaultClient
	http.DefaultClient = customClient
	defer func() { http.DefaultClient = originalClient }()
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.RetryMax = 0
	iterator, driverErr := NewCloudBatchIterator(context.Background(), []*cli_service.TSparkArrowResultLink{{
		FileLink: server.URL + "/redirect", ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 6,
	}}, 0, nil, cfg, nil)
	require.NoError(t, driverErr)
	defer iterator.Close()
	records, nextErr := iterator.Next()
	require.NoError(t, nextErr)
	records.Close()
	assert.Zero(t, customClient.Timeout)
}

func TestCloudFetchDefaultClientTimeoutWorkaround(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	customClient := *http.DefaultClient
	customClient.Timeout = 100 * time.Millisecond
	originalClient := http.DefaultClient
	http.DefaultClient = &customClient
	defer func() { http.DefaultClient = originalClient }()
	cfg := config.WithDefaults()
	cfg.ClientTimeout = 2 * time.Second
	cfg.RetryMax = 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	iterator, err := NewCloudIPCStreamIterator(ctx, []*cli_service.TSparkArrowResultLink{{
		FileLink: server.URL, ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 1,
	}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	started := time.Now()
	_, nextErr := iterator.Next()
	assert.ErrorIs(t, nextErr, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, 100*time.Millisecond, customClient.Timeout)
}

func TestCloudFetchDefaultClientTimeoutCap(t *testing.T) {
	for _, timeout := range []time.Duration{0, 100 * time.Millisecond, 5 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			customClient := *http.DefaultClient
			customClient.Timeout = timeout
			originalClient := http.DefaultClient
			http.DefaultClient = &customClient
			defer func() { http.DefaultClient = originalClient }()
			cfg := config.WithDefaults()
			cfg.ClientTimeout = time.Second
			iterator, err := NewCloudIPCStreamIterator(context.Background(), nil, 0, cfg, nil)
			require.NoError(t, err)
			defer iterator.Close()
			boundedClient := iterator.(*cloudIPCStreamIterator).httpClient
			assert.NotSame(t, http.DefaultClient, boundedClient)
			expected := cfg.ClientTimeout
			if timeout > 0 && timeout < expected {
				expected = timeout
			}
			assert.Equal(t, expected, boundedClient.Timeout)
			assert.Equal(t, timeout, customClient.Timeout)
		})
	}
}

func TestCloudFetchDownloadErrorsRedactQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	var output bytes.Buffer
	originalLogger := logger.Logger
	logger.Logger.Logger = logger.Logger.Output(zerolog.SyncWriter(&output)).Level(zerolog.DebugLevel)
	defer func() { logger.Logger = originalLogger }()
	cfg := config.WithDefaults()
	cfg.ClientTimeout = 50 * time.Millisecond
	cfg.RetryMax = 1
	cfg.RetryWaitMin = time.Millisecond
	cfg.RetryWaitMax = time.Millisecond
	iterator, err := NewCloudIPCStreamIterator(context.Background(), []*cli_service.TSparkArrowResultLink{{
		FileLink: server.URL + "/file?sig=SECRET_SIGNATURE", ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 1,
	}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	_, nextErr := iterator.Next()
	require.Error(t, nextErr)
	assert.ErrorIs(t, nextErr, context.DeadlineExceeded)
	assert.ErrorContains(t, nextErr, server.URL+"/file")
	assert.NotContains(t, nextErr.Error(), "SECRET_SIGNATURE")
	assert.Contains(t, output.String(), "retrying download")
	assert.Contains(t, output.String(), "failed to download")
	assert.NotContains(t, output.String(), "SECRET_SIGNATURE")
}

func TestCloudFetchErrorRedactionPreservesCause(t *testing.T) {
	cause := &url.Error{Op: "Get", URL: "https://storage.example/file?sig=SECRET_SIGNATURE", Err: context.DeadlineExceeded}
	original := &url.Error{Op: "Get", URL: "https://storage.example/redirect?sig=SECRET_SIGNATURE", Err: cause}
	redacted := redactCloudFetchError(original)
	assert.NotSame(t, original, redacted)
	assert.ErrorIs(t, redacted, context.DeadlineExceeded)
	assert.NotContains(t, redacted.Error(), "SECRET_SIGNATURE")
	assert.ErrorContains(t, redacted, "https://storage.example/file")
	assert.ErrorContains(t, redacted, "https://storage.example/redirect")
	assert.Contains(t, original.URL, "SECRET_SIGNATURE")
	assert.Contains(t, cause.URL, "SECRET_SIGNATURE")
}

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
