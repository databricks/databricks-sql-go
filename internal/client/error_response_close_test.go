package client

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bodyTracker counts response bodies that were returned but never closed.
type bodyTracker struct {
	base http.RoundTripper
	open atomic.Int32
}

func (b *bodyTracker) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := b.base.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		b.open.Add(1)
		resp.Body = &trackedBody{ReadCloser: resp.Body, t: b}
	}
	return resp, err
}

type trackedBody struct {
	io.ReadCloser
	t    *bodyTracker
	once sync.Once
}

func (tb *trackedBody) Close() error {
	tb.once.Do(func() { tb.t.open.Add(-1) })
	return tb.ReadCloser.Close()
}

// newStatusServer always answers with status and body over keep-alive
// HTTP/1.1, and reports how many server-side connections are still open.
func newStatusServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var open atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Databricks-Reason-Phrase", "reason-from-server")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			open.Add(1)
		case http.StateClosed, http.StateHijacked:
			open.Add(-1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, &open
}

// captureStdLog redirects the standard logger, where net/http reports a
// RoundTripper that returned both a response and an error.
func captureStdLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev, flags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	t.Cleanup(func() {
		log.SetOutput(prev)
		log.SetFlags(flags)
	})
	return &buf
}

func failingClientConfig() *config.Config {
	cfg := config.WithDefaults()
	cfg.Authenticator = &noop.NoopAuth{}
	cfg.RetryMax = 1
	cfg.RetryWaitMin = time.Millisecond
	cfg.RetryWaitMax = 2 * time.Millisecond
	return cfg
}

// A failed request (retries exhausted, or a status that isn't retried) must
// close the final response: net/http drops a response returned with an
// error, so nobody else can.
func TestRetryableClientClosesFailedResponse(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"503 retries exhausted", http.StatusServiceUnavailable, "service unavailable"},
		{"429 retries exhausted, empty body", http.StatusTooManyRequests, ""},
		{"500 not retried", http.StatusInternalServerError, "internal error"},
		{"401 not retried", http.StatusUnauthorized, "unauthorized"},
		{"503 with 64 KiB body", http.StatusServiceUnavailable, strings.Repeat("x", 64<<10)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, serverOpen := newStatusServer(t, tc.status, tc.body)
			cfg := failingClientConfig()
			tracker := &bodyTracker{base: PooledTransport(cfg)}
			cfg.Transport = tracker
			c := RetryableClient(cfg)
			stdlog := captureStdLog(t)

			for i := 0; i < failedRequests; i++ {
				req, err := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader("payload"))
				require.NoError(t, err)
				resp, err := c.Do(req) //nolint:bodyclose // asserting resp is nil
				require.Nil(t, resp)
				require.ErrorContains(t, err, "reason-from-server")
				if tc.status == http.StatusTooManyRequests {
					require.Equal(t, dbsqlerrint.CategoryRateLimitExceeded, dbsqlerrint.CategoryFromError(err))
				}
			}

			assertFailedResponsesReleased(t, tracker, serverOpen, stdlog)
		})
	}
}

// The Thrift client over RetryableClient must not leak the final 503 either.
func TestThriftExecuteClosesRetryExhaustedResponse(t *testing.T) {
	srv, serverOpen := newStatusServer(t, http.StatusServiceUnavailable, "service unavailable")
	tracker := &bodyTracker{base: PooledTransport(config.WithDefaults())}
	c := newExecClient(t, srv.URL, tracker, 0)
	stdlog := captureStdLog(t)

	for i := 0; i < failedRequests; i++ {
		require.ErrorContains(t, execute(context.Background(), c, "SELECT 1"), "reason-from-server")
	}

	assertFailedResponsesReleased(t, tracker, serverOpen, stdlog)
}

// failedRequests is well above PooledTransport's idle-connection cap, so a
// connection leaked per request can't pass as pooled idle connections.
const failedRequests = 25

// assertFailedResponsesReleased checks that every body was closed, only
// pooled idle connections remain, and net/http logged no dropped response.
func assertFailedResponsesReleased(t *testing.T, tracker *bodyTracker, serverOpen *atomic.Int32, stdlog *bytes.Buffer) {
	t.Helper()
	maxIdle := PooledTransport(config.WithDefaults()).MaxIdleConnsPerHost
	assert.Zero(t, tracker.open.Load(), "failed responses left unclosed")
	assert.Eventually(t, func() bool { return int(serverOpen.Load()) <= maxIdle }, 5*time.Second, 10*time.Millisecond,
		"server connections still open after %d failed requests", failedRequests)
	t.Logf("server connections open after %d failed requests: %d", failedRequests, serverOpen.Load())
	assert.Zero(t, strings.Count(stdlog.String(), "RoundTripper returned a response & error"),
		"net/http log lines about a dropped response")
}
