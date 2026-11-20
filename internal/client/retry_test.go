package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/noop"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func retryTestConfig(retries int) *config.Config {
	return &config.Config{Authenticator: &noop.NoopAuth{}, RetryMax: retries}
}

func TestRetryStatusAndBodyReplay(t *testing.T) {
	for _, status := range []int{429, 500, 502, 503, 504} {
		for _, retryAfter := range []string{"", "0", "invalid", "Wed, 21 Oct 2015 07:28:00 GMT"} {
			t.Run(fmt.Sprintf("%d/%s", status, retryAfter), func(t *testing.T) {
				var attempts atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					body, err := io.ReadAll(request.Body)
					assert.NoError(t, err)
					assert.Equal(t, "statement bytes", string(body))
					assert.Equal(t, int64(len(body)), request.ContentLength)
					if attempts.Add(1) < 3 {
						writer.Header().Set("Retry-After", retryAfter)
						writer.WriteHeader(status)
					}
					_, _ = io.WriteString(writer, "response")
				}))
				defer server.Close()
				ctx := context.WithValue(context.Background(), ClientMethod, clientMethodOpenSession)
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, io.NopCloser(strings.NewReader("statement bytes")))
				require.NoError(t, err)
				request.ContentLength = int64(len("statement bytes"))
				httpClient := RetryableClient(retryTestConfig(2))
				response, err := httpClient.Do(request)
				require.NoError(t, err)
				defer response.Body.Close() //nolint:errcheck
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.Equal(t, int32(3), attempts.Load())
			})
		}
	}
}

type trackedRetryBody struct {
	io.ReadCloser
	read   atomic.Int64
	closed atomic.Bool
}

func (body *trackedRetryBody) Read(buffer []byte) (int, error) {
	count, err := body.ReadCloser.Read(buffer)
	body.read.Add(int64(count))
	return count, err
}

func (body *trackedRetryBody) Close() error {
	body.closed.Store(true)
	return body.ReadCloser.Close()
}

type trackingRetryTransport struct {
	*http.Transport
	mu     sync.Mutex
	bodies []*trackedRetryBody
}

func (transport *trackingRetryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.Transport.RoundTrip(request)
	if response != nil {
		body := &trackedRetryBody{ReadCloser: response.Body}
		response.Body = body
		transport.mu.Lock()
		transport.bodies = append(transport.bodies, body)
		transport.mu.Unlock()
	}
	return response, err
}

func TestRetryExhaustionClosesResponses(t *testing.T) {
	for _, retries := range []int{-1, 0, 3} {
		t.Run(fmt.Sprint(retries), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("X-Databricks-Reason-Phrase", "try later")
				writer.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(writer, strings.Repeat("x", 16384))
			}))
			defer server.Close()
			cfg := retryTestConfig(retries)
			transport := &trackingRetryTransport{Transport: PooledTransport(cfg)}
			defer transport.CloseIdleConnections()
			cfg.Transport = transport
			request, err := http.NewRequest(http.MethodGet, server.URL, nil)
			require.NoError(t, err)
			response, err := RetryableClient(cfg).Do(request)
			if response != nil {
				_ = response.Body.Close()
			}
			require.Error(t, err)
			require.Nil(t, response)
			attempts := max(1, retries+1)
			require.Contains(t, err.Error(), fmt.Sprintf("request error after %d attempt(s): databricks: retryableError: unexpected HTTP status 429 Too Many Requests", attempts))
			require.Contains(t, err.Error(), "try later")
			var requestErr dbsqlerr.DBError
			require.ErrorAs(t, err, &requestErr)
			require.Equal(t, dbsqlerrint.CategoryRateLimitExceeded, dbsqlerrint.CategoryFromError(err))
			transport.mu.Lock()
			defer transport.mu.Unlock()
			require.Len(t, transport.bodies, attempts)
			for index, body := range transport.bodies {
				defer body.Close() //nolint:errcheck
				assert.True(t, body.closed.Load(), "response %d must be closed", index)
				if index < attempts-1 {
					assert.Equal(t, int64(4096), body.read.Load())
				} else {
					assert.Zero(t, body.read.Load(), "do not read an already failed response")
				}
			}
		})
	}
}

func TestRetryEmptyBodies(t *testing.T) {
	for _, variant := range []string{"nil", "NoBody", "empty", "GetBody"} {
		t.Run(variant, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				assert.NoError(t, err)
				assert.Empty(t, body)
				assert.Zero(t, request.ContentLength)
				if attempts.Add(1) < 2 {
					writer.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			defer server.Close()
			request, err := http.NewRequest(http.MethodPost, server.URL, nil)
			require.NoError(t, err)
			switch variant {
			case "NoBody":
				request.Body = http.NoBody
			case "empty":
				request.Body = io.NopCloser(strings.NewReader(""))
			case "GetBody":
				request.Body = http.NoBody
				request.GetBody = func() (io.ReadCloser, error) { return http.NoBody, nil }
			}
			response, err := RetryableClient(retryTestConfig(1)).Do(request)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, int32(2), attempts.Load())
		})
	}
}

func TestRetryCancelDuringBackoff(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			received := make(chan struct{}, 1)
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				attempts.Add(1)
				writer.Header().Set("Retry-After", "60")
				writer.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(writer, "retry")
				received <- struct{}{}
			}))
			defer server.Close()
			cfg := retryTestConfig(3)
			transport := &trackingRetryTransport{Transport: PooledTransport(cfg)}
			defer transport.CloseIdleConnections()
			cfg.Transport = transport
			ctx, cancel := context.WithCancel(context.Background())
			wantErr := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				wantErr = context.DeadlineExceeded
			}
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, strings.NewReader("body"))
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() {
				response, doErr := RetryableClient(cfg).Do(request)
				if response != nil {
					_ = response.Body.Close()
				}
				result <- doErr
			}()
			select {
			case <-received:
			case <-time.After(2 * time.Second):
				t.Fatal("initial request not received")
			}
			require.Eventually(t, func() bool {
				transport.mu.Lock()
				defer transport.mu.Unlock()
				return len(transport.bodies) == 1 && transport.bodies[0].closed.Load()
			}, time.Second, time.Millisecond)
			if !deadline {
				cancel()
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, wantErr)
				require.NotContains(t, err.Error(), "request error after")
			case <-time.After(2 * time.Second):
				t.Fatal("backoff ignored cancellation")
			}
			require.Equal(t, int32(1), attempts.Load())
		})
	}
}

func TestRetryTerminalAndSkippedResponses(t *testing.T) {
	for _, status := range []int{200, 201, 400, 401, 429, 501, 503} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/skip=%t", status, skip), func(t *testing.T) {
				var attempts atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					attempts.Add(1)
					writer.WriteHeader(status)
					_, _ = io.WriteString(writer, "response")
				}))
				defer server.Close()
				ctx := context.Background()
				if skip {
					ctx = WithSkipTransientRetries(ctx)
				}
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
				require.NoError(t, err)
				response, err := RetryableClient(retryTestConfig(2)).Do(request)
				if status == 200 || skip && (status == 429 || status == 503) {
					require.NoError(t, err)
					body, readErr := io.ReadAll(response.Body)
					require.NoError(t, readErr)
					require.NoError(t, response.Body.Close())
					require.Equal(t, "response", string(body))
				} else {
					require.Error(t, err)
					require.Nil(t, response)
				}
				wantAttempts := int32(1)
				if !skip && (status == 429 || status == 503) {
					wantAttempts = 3
				}
				require.Equal(t, wantAttempts, attempts.Load())
			})
		}
	}
}

func TestRetryBackoffCompatibility(t *testing.T) {
	response := &http.Response{Header: make(http.Header)}
	require.Equal(t, time.Second, backoff(time.Second, 3*time.Second, 0, response))
	require.Equal(t, 2*time.Second, backoff(time.Second, 3*time.Second, 1, response))
	require.Equal(t, 3*time.Second, backoff(time.Second, 3*time.Second, 2, response))
	response.Header.Set("Retry-After", "60")
	require.Equal(t, time.Minute, backoff(time.Second, 3*time.Second, 0, response))
}

func TestRetryBodyPreparation(t *testing.T) {
	for _, useGetBody := range []bool{false, true} {
		t.Run(fmt.Sprintf("GetBody=%t", useGetBody), func(t *testing.T) {
			payload := "statement\x00bytes"
			wantContentLength := int64(-1)
			if useGetBody {
				wantContentLength = int64(len(payload))
			}
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				assert.NoError(t, err)
				assert.Equal(t, payload, string(body))
				assert.Equal(t, wantContentLength, request.ContentLength)
				if attempts.Add(1) < 3 {
					writer.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			defer server.Close()
			original := &trackedRetryBody{ReadCloser: io.NopCloser(strings.NewReader(payload))}
			request, err := http.NewRequest(http.MethodPost, server.URL, original)
			require.NoError(t, err)
			var bodyCalls int
			if useGetBody {
				request.ContentLength = int64(len(payload))
				request.GetBody = func() (io.ReadCloser, error) {
					bodyCalls++
					return io.NopCloser(strings.NewReader(payload)), nil
				}
			}
			response, err := RetryableClient(retryTestConfig(2)).Do(request)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, int32(3), attempts.Load())
			require.True(t, original.closed.Load())
			if useGetBody {
				require.Equal(t, 3, bodyCalls)
				require.Zero(t, original.read.Load())
			} else {
				require.Equal(t, int64(len(payload)), original.read.Load())
			}
			require.Same(t, original, request.Body)
		})
	}
}

type failingRetryReader struct {
	err error
}

func (reader failingRetryReader) Read([]byte) (int, error) {
	return 0, reader.err
}

func TestRetryBodyReadFailure(t *testing.T) {
	wantErr := errors.New("body read failed")
	body := &trackedRetryBody{ReadCloser: io.NopCloser(failingRetryReader{err: wantErr})}
	request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:1", body)
	require.NoError(t, err)
	response, err := RetryableClient(retryTestConfig(2)).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.NotContains(t, err.Error(), "request error after")
	require.True(t, body.closed.Load())
}

func TestRetryPreservesChunkedTrailers(t *testing.T) {
	for _, contentLength := range []int64{0, -1} {
		t.Run(fmt.Sprint(contentLength), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				assert.NoError(t, err)
				assert.Equal(t, "statement", string(body))
				assert.Equal(t, int64(-1), request.ContentLength)
				assert.Equal(t, []string{"chunked"}, request.TransferEncoding)
				assert.Equal(t, "abc", request.Trailer.Get("X-Checksum"))
				if attempts.Add(1) < 3 {
					writer.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			defer server.Close()
			request, err := http.NewRequest(http.MethodPost, server.URL, io.NopCloser(strings.NewReader("statement")))
			require.NoError(t, err)
			request.ContentLength = contentLength
			request.Trailer = http.Header{"X-Checksum": []string{"abc"}}
			response, err := RetryableClient(retryTestConfig(2)).Do(request)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, int32(3), attempts.Load())
			require.Equal(t, contentLength, request.ContentLength)
		})
	}
}

func TestRetryPreservesDeclaredContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
	}))
	defer server.Close()
	for _, contentLength := range []int64{1, 100} {
		t.Run(fmt.Sprint(contentLength), func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, server.URL, io.NopCloser(strings.NewReader("statement")))
			require.NoError(t, err)
			request.ContentLength = contentLength
			response, err := RetryableClient(retryTestConfig(0)).Do(request)
			if response != nil {
				_ = response.Body.Close()
			}
			require.Nil(t, response)
			require.ErrorContains(t, err, "ContentLength=")
		})
	}
}

func TestRetryGetBodyFailure(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts.Add(1)
	}))
	defer server.Close()
	body := &trackedRetryBody{ReadCloser: io.NopCloser(strings.NewReader("statement"))}
	request, err := http.NewRequest(http.MethodPost, server.URL, body)
	require.NoError(t, err)
	wantErr := errors.New("GetBody failed")
	request.GetBody = func() (io.ReadCloser, error) { return nil, wantErr }
	response, err := RetryableClient(retryTestConfig(2)).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.True(t, body.closed.Load())
	require.Zero(t, body.read.Load())
	require.Zero(t, attempts.Load())
}

func TestRetryWithoutAuthenticator(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if attempts.Add(1) < 2 {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	httpClient := RetryableClient(&config.Config{RetryMax: 1})
	retry, ok := httpClient.Transport.(*retryTransport)
	require.True(t, ok)
	transport, ok := retry.client.Transport.(*http.Transport)
	require.True(t, ok)
	defer transport.CloseIdleConnections()
	require.Equal(t, 90*time.Second, transport.IdleConnTimeout)
	require.Equal(t, runtime.GOMAXPROCS(0)+1, transport.MaxIdleConnsPerHost)
	require.Zero(t, transport.MaxConnsPerHost)
	response, err := httpClient.Get(server.URL)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, int32(2), attempts.Load())
}

func TestRetryPreservesRedirectReplayability(t *testing.T) {
	for _, replayable := range []bool{false, true} {
		t.Run(fmt.Sprint(replayable), func(t *testing.T) {
			var redirected atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				redirected.Add(1)
				body, err := io.ReadAll(request.Body)
				assert.NoError(t, err)
				assert.Equal(t, "statement", string(body))
			}))
			defer destination.Close()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				http.Redirect(writer, request, destination.URL, http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			request, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader("statement"))
			require.NoError(t, err)
			if !replayable {
				request.GetBody = nil
			}
			response, err := RetryableClient(retryTestConfig(2)).Do(request)
			if replayable {
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, int32(1), redirected.Load())
			} else {
				require.ErrorContains(t, err, "unexpected HTTP status 307 Temporary Redirect")
				require.Nil(t, response)
				require.Zero(t, redirected.Load())
			}
		})
	}
}

func TestRetryErrorUnwrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:1", nil)
	require.NoError(t, err)
	response, err := RetryableClient(retryTestConfig(2)).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	require.Nil(t, response)
	require.True(t, errors.Is(err, context.Canceled))
}

func BenchmarkRetryableClientSuccess(benchmark *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		_, _ = io.WriteString(writer, "ok")
	}))
	defer server.Close()
	httpClient := RetryableClient(retryTestConfig(4))
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for benchmark.Loop() {
		request, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(payload))
		if err != nil {
			benchmark.Fatal(err)
		}
		response, err := httpClient.Do(request)
		if err != nil {
			benchmark.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if err != nil {
			benchmark.Fatal(err)
		}
	}
}
