package client

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/url"
	"testing"
	"time"

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
			closeSession,
			getResultSetMetadata,
			getOperationStatus,
			closeOperation,
			cancelOperation,
			fetchResults,
			openSession,
		}

		nonRetryableOps := []clientMethod{executeStatement}

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

}
