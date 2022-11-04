package sentinel

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	DEFAULT_TIMEOUT  = 0
	DEFAULT_INTERVAL = 100 * time.Millisecond
)

type WatchStatus int

const (
	WatchSuccess WatchStatus = iota
	WatchErr
	WatchExecuting
	WatchTimeout
	WatchCanceled
)

func (s WatchStatus) String() string {
	switch s {
	case WatchSuccess:
		return "SUCCESS"
	case WatchErr:
		return "ERROR"
	case WatchExecuting:
		return "EXECUTING"
	case WatchCanceled:
		return "CANCELED"
	case WatchTimeout:
		return "TIMEOUT"
	}
	return "<UNSET>"
}

type Done func() bool

type Sentinel struct {
	StatusFn   func() (Done, error)
	OnCancelFn func() (any, error)
	OnDoneFn   func() (any, error)
}

// Wait takes care of checking the status of something on a given interval, up to a timeout.
// If statusFn returns WaitExecuting, the check will continue until status changes.
// Context cancelation is supported and in that case it will return WaitCanceled status.
func (s Sentinel) Watch(ctx context.Context, interval, timeout time.Duration) (WatchStatus, any, error) {
	if s.StatusFn == nil {
		s.StatusFn = func() (Done, error) { return func() bool { return true }, nil }
	}
	if timeout == 0 {
		timeout = DEFAULT_TIMEOUT
	}
	if interval == 0 {
		interval = DEFAULT_INTERVAL
	}

	var timeoutTimerCh <-chan time.Time
	if timeout != 0 {
		timeoutTimer := time.NewTimer(timeout)
		timeoutTimerCh = timeoutTimer.C
		defer timeoutTimer.Stop()
	}

	intervalTimer := time.NewTimer(interval)
	defer intervalTimer.Stop()

	resCh := make(chan any, 1)
	errCh := make(chan error, 1)
	processor := func() {
		ret, err := s.OnDoneFn()
		if err != nil {
			errCh <- err
		} else {
			resCh <- ret
		}
	}

	for {
		select {
		case <-intervalTimer.C:
			done, err := s.StatusFn()
			log.Debug().Msg("status checked")
			if err != nil {
				return WatchErr, nil, err
			}
			// resetting it here so statusFn is called again after interval time
			_ = intervalTimer.Reset(interval)
			if done() {
				intervalTimer.Stop()
				if s.OnDoneFn != nil {
					go processor()
				} else {
					return WatchSuccess, nil, nil
				}
			}
		case err := <-errCh:
			return WatchErr, nil, err
		case res := <-resCh:
			return WatchSuccess, res, nil
		case <-ctx.Done():
			_ = intervalTimer.Stop()
			if s.OnCancelFn != nil {
				ret, err := s.OnCancelFn()
				if err == nil {
					err = ctx.Err()
				}
				return WatchCanceled, ret, err
			}
			return WatchCanceled, nil, ctx.Err()
		case <-timeoutTimerCh:
			_ = intervalTimer.Stop()
			log.Info().Msgf("wait timed out after %s", timeout.String())
			return WatchTimeout, nil, fmt.Errorf("sentinel timed out")
		}
	}
}
