package client

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/url"
	"testing"

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

	t.Run("test handling url error", func(t *testing.T) {
		urlErr := &url.Error{Err: errors.New("EOF")}
		resp := &http.Response{}

		urlErr.Err = x509.UnknownAuthorityError{}
		b, err := RetryPolicy(context.Background(), resp, urlErr)
		require.False(t, b)
		require.Equal(t, urlErr, err)

		errMsgs := []string{"stopped after 12 redirects", "unsupported protocol scheme", "certificate is not trusted"}
		for _, msg := range errMsgs {
			urlErr.Err = errors.New(msg)
			b, err := RetryPolicy(context.Background(), resp, urlErr)
			require.False(t, b)
			require.Equal(t, urlErr, err)
		}

		urlErr.Err = errors.New("other error")
		b, err = RetryPolicy(context.Background(), resp, urlErr)
		require.True(t, b)
		require.Nil(t, err)

		b, err = RetryPolicy(context.Background(), resp, errors.New("non-url error"))
		require.True(t, b)
		require.Nil(t, err)

	})

	t.Run("test handling status codes", func(t *testing.T) {
		resp := &http.Response{}
		// 429 nd 503 are always retryable
		retryableCodes := []int{429, 503}
		// maybe retryable codes: 0 and >= 500, but not 501
		maybeRetryableCodes := []int{500, 502, 504, 505, 506, 507, 508, 511, 0, 777}

		nonRetryableCodes := []int{200, 300, 400, 501}

		idempotentOps := []clientMethod{closeSession, getResultSetMetadata, getOperationStatus, closeOperation, cancelOperation, fetchResults}
		nonIdempotentOps := []clientMethod{openSession, executeStatement}

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		for _, code := range retryableCodes {
			for _, op := range idempotentOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.True(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}

			for _, op := range nonIdempotentOps {
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
			for _, op := range idempotentOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.True(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}

			for _, op := range nonIdempotentOps {
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
			for _, op := range idempotentOps {
				resp.StatusCode = code
				ctx := context.WithValue(context.Background(), ClientMethod, op)
				retry, _ := RetryPolicy(ctx, resp, nil)
				require.False(t, retry)

				// should always return false if the context is cancelled or timed out
				retry, _ = RetryPolicy(cancelled, resp, nil)
				require.False(t, retry)
			}

			for _, op := range nonIdempotentOps {
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
