package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

func dialOpErr(err error) error {
	return &url.Error{Op: "Post", URL: "http://host", Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
}

func TestIsDialError(t *testing.T) {
	refused := &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}
	reset := &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}

	// Real errors from net.Dialer: a connect timeout and a cancelled dial.
	_, dialTimeout := (&net.Dialer{Timeout: time.Nanosecond}).Dial("tcp", "127.0.0.1:1")
	require.Error(t, dialTimeout)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, dialCancelled := (&net.Dialer{}).DialContext(cancelled, "tcp", "127.0.0.1:1")
	require.Error(t, dialCancelled)

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused", dialOpErr(refused), true},
		// RST before connect() completes: no connection existed, so nothing was written.
		{"reset during connect", dialOpErr(&os.SyscallError{Syscall: "connect", Err: syscall.ECONNRESET}), true},
		{"network unreachable", dialOpErr(&os.SyscallError{Syscall: "connect", Err: syscall.ENETUNREACH}), true},
		{"host unreachable", dialOpErr(&os.SyscallError{Syscall: "connect", Err: syscall.EHOSTUNREACH}), true},
		{"connect timeout", &url.Error{Op: "Post", URL: "http://host", Err: dialTimeout}, true},
		{"temporary DNS failure", dialOpErr(&net.DNSError{Err: "server misbehaving", Name: "host", IsTemporary: true}), true},
		{"DNS timeout", dialOpErr(&net.DNSError{Err: "i/o timeout", Name: "host", IsTimeout: true}), true},
		{"DNS not found", dialOpErr(&net.DNSError{Err: "no such host", Name: "host", IsNotFound: true}), false},
		{"proxy dial refused", &url.Error{Op: "Post", URL: "http://host",
			Err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: &net.OpError{Op: "dial", Net: "tcp", Err: refused}}}, true},
		{"proxy DNS not found", &url.Error{Op: "Post", URL: "http://host",
			Err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: &net.OpError{Op: "dial", Net: "tcp",
				Err: &net.DNSError{Err: "no such host", Name: "proxy", IsNotFound: true}}}}, false},
		{"proxy TLS failure", &url.Error{Op: "Post", URL: "http://host",
			Err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}}}, false},
		{"proxy CONNECT rejected", &url.Error{Op: "Post", URL: "http://host",
			Err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: errors.New("Forbidden")}}, false},
		{"cancelled dial", &url.Error{Op: "Post", URL: "http://host", Err: dialCancelled}, false},
		{"dial wrapping context.Canceled", dialOpErr(context.Canceled), false},
		{"context canceled", &url.Error{Op: "Post", URL: "http://host", Err: context.Canceled}, false},
		{"context deadline", &url.Error{Op: "Post", URL: "http://host", Err: context.DeadlineExceeded}, false},
		{"TLS record header", &url.Error{Op: "Post", URL: "http://host", Err: tls.RecordHeaderError{Msg: "bad record"}}, false},
		{"TLS alert", &url.Error{Op: "Post", URL: "http://host", Err: tls.AlertError(40)}, false},
		{"TLS unknown authority", &url.Error{Op: "Post", URL: "http://host",
			Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}, false},
		{"TLS handshake timeout", &url.Error{Op: "Post", URL: "http://host", Err: errors.New("net/http: TLS handshake timeout")}, false},
		{"reset during TLS handshake", &url.Error{Op: "Post", URL: "http://host", Err: &net.OpError{Op: "read", Net: "tcp", Err: reset}}, false},
		{"read reset after send", &url.Error{Op: "Post", URL: "http://host", Err: &net.OpError{Op: "read", Net: "tcp", Err: reset}}, false},
		{"write broken pipe", &url.Error{Op: "Post", URL: "http://host",
			Err: &net.OpError{Op: "write", Net: "tcp", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}}, false},
		{"connection broken mid-write", &url.Error{Op: "Post", URL: "http://host",
			Err: errors.Join(errors.New("net/http: HTTP/1.x transport connection broken"), &net.OpError{Op: "write", Net: "tcp", Err: reset})}, false},
		{"EOF", &url.Error{Op: "Post", URL: "http://host", Err: io.EOF}, false},
		{"unexpected EOF", &url.Error{Op: "Post", URL: "http://host", Err: io.ErrUnexpectedEOF}, false},
		{"server closed idle", &url.Error{Op: "Post", URL: "http://host", Err: errors.New("http: server closed idle connection")}, false},
		{"non-url error", errors.New("other error"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, isDialError(c.err))
		})
	}
}

func TestRetryPolicyExecuteStatementDialErrors(t *testing.T) {
	refused := dialOpErr(&os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED})
	reset := &url.Error{Op: "Post", URL: "http://host", Err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}}
	methodCtx := context.WithValue(context.Background(), ClientMethod, clientMethodExecuteStatement)
	attempt := &attemptState{}
	execCtx := context.WithValue(methodCtx, attemptStateKey, attempt)

	retry, err := RetryPolicy(execCtx, nil, refused)
	require.True(t, retry)
	require.NoError(t, err)

	// Without attempt state, or after a redirect, the request may have been delivered.
	retry, err = RetryPolicy(methodCtx, nil, refused)
	require.False(t, retry)
	require.Equal(t, refused, err)
	attempt.redirected.Store(true)
	retry, err = RetryPolicy(execCtx, nil, refused)
	require.False(t, retry)
	require.Equal(t, refused, err)
	attempt.redirected.Store(false)

	retry, err = RetryPolicy(execCtx, nil, reset)
	require.False(t, retry)
	require.Equal(t, reset, err)

	cancelled, cancel := context.WithCancel(execCtx)
	cancel()
	retry, err = RetryPolicy(cancelled, nil, refused)
	require.False(t, retry)
	require.ErrorIs(t, err, context.Canceled)

	expired, cancel2 := context.WithDeadline(execCtx, time.Now().Add(-time.Second))
	defer cancel2()
	retry, err = RetryPolicy(expired, nil, refused)
	require.False(t, retry)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Callers without a ClientMethod (e.g. the telemetry exporter) stay one-shot.
	retry, err = RetryPolicy(context.Background(), nil, refused)
	require.False(t, retry)
	require.Equal(t, refused, err)
}

// execServer runs the real Thrift processor and counts ExecuteStatement executions.
type execServer struct {
	executions atomic.Int32
	thrift     http.HandlerFunc
}

func newExecServer() *execServer {
	s := &execServer{}
	handler := &TestClient{FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
		s.executions.Add(1)
		return &cli_service.TExecuteStatementResp{Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}}, nil
	}}
	pf := thrift.NewTBinaryProtocolFactoryConf(&thrift.TConfiguration{})
	s.thrift = thrift.NewThriftHandlerFunc(cli_service.NewTCLIServiceProcessor(handler), pf, pf)
	return s
}

const testHTTPPath = "/sql/1.0/warehouses/test"

// newExecClient builds the production Thrift client over RetryableClient.
// A nil transport uses PooledTransport.
func newExecClient(t *testing.T, rawURL string, tr http.RoundTripper, clientTimeout time.Duration) *ThriftServiceClient {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	cfg := config.WithDefaults()
	cfg.Authenticator = &noop.NoopAuth{}
	cfg.Protocol = u.Scheme
	cfg.Host = u.Hostname()
	cfg.Port = port
	cfg.HTTPPath = testHTTPPath
	cfg.RetryWaitMin = time.Millisecond
	cfg.RetryWaitMax = 5 * time.Millisecond
	cfg.Transport = tr
	if clientTimeout > 0 {
		cfg.ClientTimeout = clientTimeout
	}
	c, err := InitThriftClient(cfg, RetryableClient(cfg))
	require.NoError(t, err)
	return c
}

// dialTracingCtx counts dial attempts made by the http.Transport.
func dialTracingCtx() (context.Context, *atomic.Int32) {
	var n atomic.Int32
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		ConnectStart: func(string, string) { n.Add(1) },
	})
	return ctx, &n
}

func execute(ctx context.Context, c *ThriftServiceClient, statement string) error {
	_, err := c.ExecuteStatement(ctx, &cli_service.TExecuteStatementReq{
		SessionHandle: &cli_service.TSessionHandle{SessionId: &cli_service.THandleIdentifier{
			GUID: make([]byte, 16), Secret: make([]byte, 16)}},
		Statement: statement,
	})
	return err
}

func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// resetConn aborts the request's connection with a TCP RST, without a response.
func resetConn(t *testing.T, w http.ResponseWriter) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Errorf("hijack: %v", err)
		return
	}
	_ = conn.(*net.TCPConn).SetLinger(0)
	_ = conn.Close()
}

func TestExecuteStatementRetriesOnlyNeverSentRequests(t *testing.T) {
	t.Run("connection refused is retried and executes once", func(t *testing.T) {
		srv := newExecServer()
		ts := httptest.NewServer(srv.thrift)
		defer ts.Close()
		refusedAddr := closedAddr(t)
		var dials atomic.Int32
		tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if dials.Add(1) <= 2 {
				addr = refusedAddr
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		defer tr.CloseIdleConnections()

		require.NoError(t, execute(context.Background(), newExecClient(t, ts.URL, tr, 0), "SELECT 1"))
		require.Equal(t, int32(3), dials.Load())
		require.Equal(t, int32(1), srv.executions.Load())
	})

	t.Run("persistent connection refused exhausts retries", func(t *testing.T) {
		ctx, dials := dialTracingCtx()
		err := execute(ctx, newExecClient(t, "http://"+closedAddr(t), nil, 0), "SELECT 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "execute statement request error")
		require.Contains(t, err.Error(), "request error after 5 attempt(s)")
		require.Contains(t, err.Error(), "connection refused")
		require.Equal(t, int32(5), dials.Load())
	})

	t.Run("proxy connection refused is retried", func(t *testing.T) {
		proxyURL, err := url.Parse("http://" + closedAddr(t))
		require.NoError(t, err)
		tr := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		ctx, dials := dialTracingCtx()
		err = execute(ctx, newExecClient(t, "http://"+closedAddr(t), tr, 0), "SELECT 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "proxyconnect")
		require.Equal(t, int32(5), dials.Load())
	})

	// Each fault below happens after the connection exists, so the request may
	// have reached the server: exactly one attempt.
	faults := []struct {
		name       string
		statement  string
		handler    func(t *testing.T, srv *execServer) http.HandlerFunc
		executions int32
	}{
		{
			name:      "reset after the server executed the request",
			statement: "INSERT INTO t VALUES (1)",
			handler: func(t *testing.T, srv *execServer) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					srv.thrift(httptest.NewRecorder(), r)
					resetConn(t, w)
				}
			},
			executions: 1,
		},
		{
			name:      "reset mid-body",
			statement: strings.Repeat("x", 16<<20),
			handler: func(t *testing.T, srv *execServer) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.ReadFull(r.Body, make([]byte, 1024))
					resetConn(t, w)
				}
			},
		},
		{
			name:      "reset after headers",
			statement: "INSERT INTO t VALUES (1)",
			handler: func(t *testing.T, srv *execServer) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) { resetConn(t, w) }
			},
		},
	}
	for _, f := range faults {
		t.Run(f.name, func(t *testing.T) {
			srv := newExecServer()
			var received atomic.Int32
			h := f.handler(t, srv)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Add(1)
				h(w, r)
			}))
			defer ts.Close()

			ctx, dials := dialTracingCtx()
			require.Error(t, execute(ctx, newExecClient(t, ts.URL, nil, 0), f.statement))
			require.Equal(t, int32(1), dials.Load())
			require.Equal(t, int32(1), received.Load())
			require.Equal(t, f.executions, srv.executions.Load())
		})
	}

	// The server resets the connection once the client's first bytes arrive:
	// HTTP request bytes, or the TLS ClientHello. (A reset that lands before
	// connect() completes is a dial error and is retried; see TestIsDialError.)
	for _, scheme := range []string{"http", "https"} {
		t.Run("reset after first bytes over "+scheme, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer l.Close() //nolint:errcheck
			var accepts atomic.Int32
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					accepts.Add(1)
					_, _ = c.Read(make([]byte, 1))
					_ = c.(*net.TCPConn).SetLinger(0)
					_ = c.Close()
				}
			}()

			ctx, dials := dialTracingCtx()
			require.Error(t, execute(ctx, newExecClient(t, scheme+"://"+l.Addr().String(), nil, 0), "SELECT 1"))
			require.Equal(t, int32(1), dials.Load())
			require.Eventually(t, func() bool { return accepts.Load() == 1 }, time.Second, 5*time.Millisecond)
		})
	}

	// A followed redirect means an earlier hop received the request, so a dial
	// failure on a later hop must not replay it.
	for _, status := range []int{http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run("dial failure after "+strconv.Itoa(status)+" redirect", func(t *testing.T) {
			srv := newExecServer()
			target := "http://" + closedAddr(t) + testHTTPPath
			var received atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Add(1)
				srv.thrift(httptest.NewRecorder(), r)
				http.Redirect(w, r, target, status)
			}))
			defer ts.Close()

			require.Error(t, execute(context.Background(), newExecClient(t, ts.URL, nil, 0), "INSERT INTO t VALUES (1)"))
			require.Equal(t, int32(1), received.Load())
			require.Equal(t, int32(1), srv.executions.Load())
		})
	}

	t.Run("redirect to a reachable server", func(t *testing.T) {
		dst := newExecServer()
		dts := httptest.NewServer(dst.thrift)
		defer dts.Close()
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, dts.URL+testHTTPPath, http.StatusTemporaryRedirect)
		}))
		defer ts.Close()

		require.NoError(t, execute(context.Background(), newExecClient(t, ts.URL, nil, 0), "SELECT 1"))
		require.Equal(t, int32(1), dst.executions.Load())
	})

	// Attempt 1 is redirected and gets a 429; attempt 2's first hop is refused.
	// The redirect belongs to attempt 1 only, so attempt 2 is still retried.
	t.Run("refused first hop after an earlier redirected attempt", func(t *testing.T) {
		dst := newExecServer()
		var rateLimited atomic.Bool
		dts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if rateLimited.CompareAndSwap(false, true) {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			dst.thrift(w, r)
		}))
		defer dts.Close()
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Connection", "close")
			http.Redirect(w, r, dts.URL+testHTTPPath, http.StatusTemporaryRedirect)
		}))
		defer ts.Close()

		origin := ts.Listener.Addr().String()
		refusedAddr := closedAddr(t)
		var originDials atomic.Int32
		tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if addr == origin && originDials.Add(1) == 2 {
				addr = refusedAddr
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		defer tr.CloseIdleConnections()

		require.NoError(t, execute(context.Background(), newExecClient(t, ts.URL, tr, 0), "SELECT 1"))
		require.Equal(t, int32(3), originDials.Load())
		require.Equal(t, int32(1), dst.executions.Load())
	})

	t.Run("untrusted certificate", func(t *testing.T) {
		srv := newExecServer()
		ts := httptest.NewTLSServer(srv.thrift)
		defer ts.Close()
		ctx, dials := dialTracingCtx()
		require.Error(t, execute(ctx, newExecClient(t, ts.URL, nil, 0), "SELECT 1"))
		require.Equal(t, int32(1), dials.Load())
		require.Equal(t, int32(0), srv.executions.Load())
	})

	t.Run("caller cancellation during dial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var dials atomic.Int32
		tr := &http.Transport{DialContext: func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			cancel()
			return (&net.Dialer{}).DialContext(dialCtx, network, closedAddr(t))
		}}
		err := execute(ctx, newExecClient(t, "http://"+closedAddr(t), tr, 0), "SELECT 1")
		require.Error(t, err)
		require.Equal(t, int32(1), dials.Load())
	})

	t.Run("client timeout during dial", func(t *testing.T) {
		done := make(chan struct{})
		defer close(done)
		var dials atomic.Int32
		tr := &http.Transport{DialContext: func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			select {
			case <-dialCtx.Done():
			case <-done:
			}
			return nil, &net.OpError{Op: "dial", Net: network, Err: dialCtx.Err()}
		}}
		require.Error(t, execute(context.Background(), newExecClient(t, "http://"+closedAddr(t), tr, 50*time.Millisecond), "SELECT 1"))
		require.Equal(t, int32(1), dials.Load())
	})
}
