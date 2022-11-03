package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatch(t *testing.T) {
	t.Run("it should return immediatly", func(t *testing.T) {
		statusFnCalls := 0
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				return true
			}, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(context.Background(), 0, 0)
		assert.Equal(t, WatchSuccess, status)
		assert.Nil(t, res)
		assert.Equal(t, statusFnCalls, 1)
		assert.NoError(t, err)
	})
	t.Run("it should deal with long statusFn", func(t *testing.T) {
		statusFnCalls := 0
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				time.Sleep(1 * time.Second)
				return true
			}, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(context.Background(), 50*time.Millisecond, 0)
		assert.Equal(t, WatchSuccess, status)
		assert.Nil(t, res)
		assert.Equal(t, statusFnCalls, 1)
		assert.NoError(t, err)
	})
	t.Run("it should timeout", func(t *testing.T) {
		statusFnCalls := 0
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				return false
			}, nil
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
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				return false
			}, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(ctx, 0, 1*time.Second)
		assert.Equal(t, WatchCanceled, status)
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
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				return false
			}, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(ctx, 0, 15*time.Second)
		assert.Equal(t, WatchCanceled, status)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should cancel with canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		statusFnCalls := 0
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				return false
			}, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
		}
		status, res, err := s.Watch(ctx, 0, 15*time.Second)
		assert.Equal(t, WatchCanceled, status)
		assert.Nil(t, res)
		assert.Error(t, err)
	})
	t.Run("it should call cancelFn upon cancelation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		statusFnCalls := 0
		cancelFnCalls := 0
		var statusFn = func() (Done, error) {
			return func() bool {
				statusFnCalls++
				return false
			}, nil
		}
		s := Sentinel{
			StatusFn: statusFn,
			CancelFn: func() (any, error) {
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
	t.Run("it should call processFn upon Done", func(t *testing.T) {
		statusFnCalls := 0
		cancelFnCalls := 0

		s := Sentinel{
			StatusFn: func() (Done, error) {
				return func() bool {
					statusFnCalls++
					return true
				}, nil
			},
			CancelFn: func() (any, error) {
				cancelFnCalls++
				return nil, nil
			},
			ProcessFn: func() (any, error) {
				return "done", nil
			},
		}
		status, res, err := s.Watch(context.Background(), 0, 0)
		res = res.(string)
		assert.Equal(t, WatchSuccess, status)
		assert.Equal(t, cancelFnCalls, 0)
		assert.Equal(t, statusFnCalls, 1)
		assert.Equal(t, "done", res)
		assert.NoError(t, err)
	})
	t.Run("it should call processFn upon Done even when no statusFn", func(t *testing.T) {
		cancelFnCalls := 0

		s := Sentinel{
			ProcessFn: func() (any, error) {
				time.Sleep(50 * time.Millisecond)
				return "done", nil
			},
		}
		status, res, err := s.Watch(context.Background(), 100*time.Millisecond, 0)
		res, ok := res.(string)
		assert.True(t, ok)
		assert.Equal(t, WatchSuccess, status)
		assert.Equal(t, cancelFnCalls, 0)
		assert.Equal(t, "done", res)
		assert.NoError(t, err)
	})
}
