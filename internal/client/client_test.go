package client

import (
	"context"
	"crypto/x509"
	"database/sql/driver"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestSprintByteId(t *testing.T) {
	type args struct {
		bts []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "valid case",
			args: args{[]byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54}},
			want: "01020304-0217-0402-0302-030404df2236",
		},
		{
			name: "invalid case",
			args: args{[]byte{23}},
			want: "17",
		},
		{
			name: "invalid case",
			args: args{[]byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54, 78}},
			want: "0102030402170402030102030404df22364e",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SprintGuid(tt.args.bts); got != tt.want {
				t.Errorf("SprintByteId() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsRetryableServerResponse pins the transient-response predicate:
// 429 and 503 are the only status codes the retry layer treats as
// recoverable "server says try again" responses. Generic 5xx
// (500/502/504) takes a different code path (governed by ClientMethod);
// 2xx/3xx/4xx are not server-side transient failures.
func TestIsRetryableServerResponse(t *testing.T) {
	cases := []struct {
		status int
		want   bool
	}{
		{200, false},
		{201, false},
		{400, false},
		{401, false},
		{403, false},
		{404, false},
		{429, true},
		{500, false},
		{502, false},
		{503, true},
		{504, false},
	}
	for _, c := range cases {
		got := isRetryableServerResponse(&http.Response{StatusCode: c.status})
		require.Equal(t, c.want, got, "status %d", c.status)
	}
	require.False(t, isRetryableServerResponse(nil), "nil response is not retryable")
}

func TestRetryPolicy(t *testing.T) {

	t.Run("test handling error", func(t *testing.T) {
		resp := &http.Response{}

		retryableErrors := []error{
			&url.Error{Err: errors.New("EOF")},
			&url.Error{Err: errors.New("other error")},
			errors.New("non-url error")}

		nonRetryableErrors := []error{&url.Error{Err: x509.UnknownAuthorityError{}}}
		errMsgs := []string{"stopped after 12 redirects", "unsupported protocol scheme", "certificate is not trusted"}
		for _, msg := range errMsgs {
			nonRetryableErrors = append(nonRetryableErrors, &url.Error{Err: errors.New(msg)})
		}

		// should never retry if context is cancelled/timed out, regardless of client method or error
		checkCtxCancelled := func(callingContext context.Context, err error) {
			cancelled, cancel := context.WithCancel(callingContext)
			cancel()
			timedOut, cancel2 := context.WithDeadline(callingContext, time.Now().Add(-10*time.Second))
			defer cancel2()

			// should never retry if context is cancelled/timed out
			b, checkErr := RetryPolicy(cancelled, resp, err)
			require.False(t, b)
			require.Equal(t, cancelled.Err(), checkErr)
			b, checkErr = RetryPolicy(timedOut, resp, err)
			require.False(t, b)
			require.Equal(t, timedOut.Err(), checkErr)
		}

		for _, err := range retryableErrors {
			// Should retry only if client method is also retryable
			for cm := range _clientMethod_index {
				ctx := context.WithValue(context.Background(), ClientMethod, clientMethod(cm))

				checkCtxCancelled(ctx, err)

				_, nonRetryableClientMethod := nonRetryableClientMethods[clientMethod(cm)]
				b, checkErr := RetryPolicy(ctx, resp, err)
				if nonRetryableClientMethod {
					require.False(t, b)
					require.Equal(t, err, checkErr)
				} else {
					require.True(t, b)
					require.Nil(t, checkErr)
				}
			}
		}

		for _, err := range nonRetryableErrors {
			// Should non retry regardless of client method
			for cm := range _clientMethod_index {
				ctx := context.WithValue(context.Background(), ClientMethod, clientMethod(cm))

				checkCtxCancelled(ctx, err)

				b, checkErr := RetryPolicy(ctx, resp, err)
				require.False(t, b)
				require.Equal(t, err, checkErr)
			}
		}
	})

	t.Run("test handling status codes", func(t *testing.T) {
		var resp *http.Response
		retry, err := RetryPolicy(context.Background(), resp, nil)
		require.False(t, retry)
		require.Nil(t, err)

		resp = &http.Response{}
		// 429 nd 503 are always retryable
		retryableCodes := []int{429, 503}
		// maybe retryable codes: 0 and >= 500, but not 501
		maybeRetryableCodes := []int{500, 502, 504, 505, 506, 507, 508, 511, 0, 777}

		nonRetryableCodes := []int{200, 300, 400, 501}

		retryableOps := []clientMethod{
			clientMethodCloseSession,
			clientMethodGetResultSetMetadata,
			clientMethodGetOperationStatus,
			clientMethodCloseOperation,
			clientMethodCancelOperation,
			clientMethodFetchResults,
			clientMethodOpenSession,
		}

		nonRetryableOps := []clientMethod{clientMethodExecuteStatement}

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		for _, code := range retryableCodes {
			for _, op := range retryableOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.True(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}

			for _, op := range nonRetryableOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.True(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}
		}

		for _, code := range maybeRetryableCodes {
			for _, op := range retryableOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.True(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}

			for _, op := range nonRetryableOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.False(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}
		}

		for _, code := range nonRetryableCodes {
			for _, op := range retryableOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.False(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}

			for _, op := range nonRetryableOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.False(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}
		}

	})

	// SkipTransientRetries on the context turns 429 AND 503 into
	// non-retryable responses so the caller can own its own backoff.
	// Generic 5xx (500/502/504) and transport errors are not affected by
	// this flag — they remain governed by ClientMethod via
	// nonRetryableClientMethods.
	//
	// We use clientMethodOpenSession (which is a retryable method) so the
	// generic 5xx branch fires; the default clientMethodUnknown is itself
	// in nonRetryableClientMethods and would suppress 500 retries for an
	// unrelated reason.
	t.Run("SkipTransientRetries affects 429 and 503", func(t *testing.T) {
		base := context.WithValue(context.Background(), ClientMethod, clientMethodOpenSession)
		ctx := WithSkipTransientRetries(base)

		// 429/503 with the flag set: not retried AND no error returned,
		// so retryablehttp's StandardClient adapter (and net/http
		// underneath) deliver the response to the caller. The caller
		// inspects the status code directly.
		resp429 := &http.Response{StatusCode: http.StatusTooManyRequests}
		retry, err := RetryPolicy(ctx, resp429, nil)
		require.False(t, retry, "429 with SkipTransientRetries should not be retried")
		require.NoError(t, err, "no err so the response is preserved through net/http")

		resp503 := &http.Response{StatusCode: http.StatusServiceUnavailable}
		retry, err = RetryPolicy(ctx, resp503, nil)
		require.False(t, retry, "503 with SkipTransientRetries should not be retried")
		require.NoError(t, err, "no err so the response is preserved through net/http")

		// 500 takes the generic 5xx branch — unaffected by the flag for
		// a retryable client method.
		resp500 := &http.Response{StatusCode: http.StatusInternalServerError}
		retry, _ = RetryPolicy(ctx, resp500, nil)
		require.True(t, retry, "500 must still retry — flag is scoped to transient (429/503) responses")

		// Without the flag, 429 and 503 both retry as before.
		retry, _ = RetryPolicy(base, resp429, nil)
		require.True(t, retry, "default behavior: 429 retries")
		retry, _ = RetryPolicy(base, resp503, nil)
		require.True(t, retry, "default behavior: 503 retries")
	})

	t.Run("test handling client method errors", func(t *testing.T) {
		cases := []struct {
			base      string
			method    clientMethod
			isBadConn bool
		}{
			{"generic error", clientMethodUnknown, false},
			{"error with Invalid SessionHandle and stuff", clientMethodUnknown, true},
			{"generic error", clientMethodOpenSession, false},
			{"error with Invalid SessionHandle and stuff", clientMethodOpenSession, true},
			{"generic error", clientMethodCloseSession, false},
			{"error with Invalid SessionHandle and stuff", clientMethodCloseSession, true},
			{"generic error", clientMethodFetchResults, false},
			{"error with Invalid SessionHandle and stuff", clientMethodFetchResults, true},
			{"generic error", clientMethodGetResultSetMetadata, false},
			{"error with Invalid SessionHandle and stuff", clientMethodGetResultSetMetadata, true},
			{"generic error", clientMethodExecuteStatement, false},
			{"error with Invalid SessionHandle and stuff", clientMethodExecuteStatement, true},
			{"generic error", clientMethodGetOperationStatus, false},
			{"error with Invalid SessionHandle and stuff", clientMethodGetOperationStatus, true},
			{"generic error", clientMethodCloseOperation, false},
			{"error with Invalid SessionHandle and stuff", clientMethodCloseOperation, true},
			{"generic error", clientMethodCancelOperation, false},
			{"error with Invalid SessionHandle and stuff", clientMethodCancelOperation, true},
		}

		for i := range cases {
			c := cases[i]
			err := handleClientMethodError(context.WithValue(context.Background(), ClientMethod, c.method), errors.New(c.base))
			msg := clientMethodRequestErrorMsgs[c.method]
			require.True(t, strings.Contains(err.Error(), msg))
			require.True(t, strings.Contains(err.Error(), c.base))
			require.True(t, errors.Is(err, dbsqlerr.DatabricksError))
			require.True(t, errors.Is(err, dbsqlerr.RequestError))
			require.Equal(t, c.isBadConn, errors.Is(err, driver.ErrBadConn))
		}
	})
}
