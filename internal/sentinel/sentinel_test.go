package sentinel

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatch(t *testing.T) {
	t.Parallel()
	t.Run("it should return immediatly", func(t *testing.T) {
		statusFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return true
			}, "completed", nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(context.Background(), 0, 0)
		assert.Equal(t, WatchSuccess, status)
		assert.Equal(t, "completed", res)
		assert.Equal(t, 1, statusFnCalls)
		assert.NoError(t, err)
	})
	t.Run("it should deal with long statusFn", func(t *testing.T) {
		statusFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				time.Sleep(1 * time.Second)
				return true
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(context.Background(), 50*time.Millisecond, 0)
		assert.Equal(t, WatchSuccess, status)
		assert.Nil(t, res)
		assert.Equal(t, 1, statusFnCalls)
		assert.NoError(t, err)
	})
	t.Run("it should timeout", func(t *testing.T) {
		statusFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(context.Background(), 100*time.Millisecond, 500*time.Millisecond)
		assert.Equal(t, WatchTimeout, status)
		assert.Nil(t, res)
		assert.Greater(t, statusFnCalls, 1)
		assert.Error(t, err)
	})
	t.Run("it should cancel with context timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		statusFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(ctx, 0, 1*time.Second)
		assert.Equal(t, WatchCanceled, status)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should cancel with timeout", func(t *testing.T) {
		statusFnCalls := 0
		cancelFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
			OnCancelFn: func() (any, error) {
				cancelFnCalls++
				return nil, nil
			},
		}
		status, res, err := s.Watch(context.Background(), 0, 100*time.Millisecond)
		assert.Equal(t, WatchTimeout, status)
		assert.Equal(t, 1, cancelFnCalls)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should cancel with context cancelation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		statusFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(ctx, 10*time.Millisecond, 15*time.Second)
		assert.Equal(t, WatchCanceled, status)
		assert.Nil(t, res)
		assert.Greater(t, statusFnCalls, 1)
		assert.Error(t, err)
	})
	t.Run("it should cancel with canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		statusFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(ctx, 0, 15*time.Second)
		assert.Equal(t, WatchCanceled, status)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should call cancelFn upon cancellation while polling", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		statusFnCalls := 0
		cancelFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
			OnCancelFn: func() (any, error) {
				cancelFnCalls++
				return nil, nil
			},
		}
		status, res, err := s.Watch(ctx, 0, 15*time.Second)
		assert.Equal(t, WatchCanceled, status)
		assert.Equal(t, cancelFnCalls, 1)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should timeout even when calling cancelFn", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		statusFnCalls := 0
		cancelFnCalls := 0
		var statusFn = func() (Done, any, error) {
			statusFnCalls++
			time.Sleep(5 * time.Second)
			return func() bool {
				return false
			}, nil, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
			OnCancelFn: func() (any, error) {
				cancelFnCalls++
				time.Sleep(10 * time.Second)
				return nil, nil
			},
		}
		status, res, err := s.Watch(ctx, 0, 200*time.Millisecond)
		assert.Equal(t, WatchCanceled, status)
		assert.Equal(t, cancelFnCalls, 1)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should call processFn upon Done", func(t *testing.T) {
		statusFnCalls := 0
		cancelFnCalls := 0

		s := Sentinel{
			StatusFn: func() (Done, any, error) {
				statusFnCalls++
				return func() bool {
					return true
				}, "completed", nil
			},
			OnCancelFn: func() (any, error) {
				cancelFnCalls++
				return nil, nil
			},
			OnDoneFn: func(statusResp any) (any, error) {
				return statusResp, nil
			},
		}
		status, res, err := s.Watch(context.Background(), 0, 0)
		res = res.(string)
		assert.Equal(t, WatchSuccess, status)
		assert.Equal(t, 0, cancelFnCalls)
		assert.Equal(t, 1, statusFnCalls)
		assert.Equal(t, "completed", res)
		assert.NoError(t, err)
	})
	t.Run("it should call processFn upon Done even when no statusFn", func(t *testing.T) {

		s := Sentinel{
			OnDoneFn: func(statusResp any) (any, error) {
				time.Sleep(50 * time.Millisecond)
				return "done", nil
			},
		}
		status, res, err := s.Watch(context.Background(), 100*time.Millisecond, 0)
		res, ok := res.(string)
		assert.True(t, ok)
		assert.Equal(t, WatchSuccess, status)
		assert.Equal(t, "done", res)
		assert.NoError(t, err)
	})
	t.Run("it should return processFn error", func(t *testing.T) {

		s := Sentinel{
			OnDoneFn: func(statusResp any) (any, error) {
				return nil, fmt.Errorf("failed")
			},
		}
		status, res, err := s.Watch(context.Background(), 100*time.Millisecond, 0)
		assert.Equal(t, WatchErr, status)
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "failed")
	})
	t.Run("it should return statusFn error", func(t *testing.T) {
		statusFnCalls := 0
		cancelFnCalls := 0

		s := Sentinel{
			StatusFn: func() (Done, any, error) {
				statusFnCalls++
				return func() bool {
					return false
				}, nil, fmt.Errorf("failed")
			},
			OnCancelFn: func() (any, error) {
				cancelFnCalls++
				return nil, nil
			},
			OnDoneFn: func(statusResp any) (any, error) {
				return statusResp, nil
			},
		}
		status, res, err := s.Watch(context.Background(), 0, 0)
		assert.Equal(t, WatchErr, status)
		assert.Equal(t, 1, statusFnCalls)
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "failed")
	})
}
