//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"
*/
import "C"

import (
	"context"
	"fmt"
	"sync"
)

// ctxWatcher bridges a Go context deadline/cancellation onto a kernel cancel
// token so a blocking cgo call (connect or a hung result-stream fetch) can be
// interrupted when the caller's ctx fires.
//
// The kernel's C ABI cannot observe a Go ctx mid-call: kernel_session_open and
// kernel_result_stream_next_batch block the calling OS thread inside Rust until
// the operation completes. The *_cancellable variants take a
// kernel_cancel_token_t that a *different* thread fires; firing drops the raced
// future in the kernel and unblocks the calling thread promptly. On connect that
// is a real abort — kernel_session_open awaits its request inline, so the
// in-flight reqwest future cancels on drop. On mid-fetch it is a prompt
// stop-waiting: the CloudFetch download runs on a detached kernel task, so the
// call returns at the deadline but that chunk's download drains in the
// background (bounded by its ~60s read-timeout). Either way the caller's OS
// thread / goroutine is freed at the deadline. This helper owns that token plus
// a watcher goroutine that fires it on ctx.Done().
//
// It mirrors the execute-path canceller watcher in operation.go (same
// done-channel + WaitGroup drain shape), but drives the generic call-cancel
// token rather than the statement canceller, and so applies to the connect and
// result-fetch calls the statement canceller can't reach.
type ctxWatcher struct {
	token *C.kernel_cancel_token_t
	done  chan struct{}
	wg    sync.WaitGroup
}

// newCtxWatcher creates a cancel token and, when ctx is cancellable, starts a
// goroutine that fires the token on ctx.Done(). Returns nil when ctx is nil or
// non-cancellable (ctx.Done() == nil) — the caller then passes a NULL token to
// the plain-equivalent behavior, so there is zero overhead on the common
// background-context path. The caller MUST call stop() to drain the watcher and
// free the token (typically via defer), after the blocking call returns.
//
// A token-creation failure also yields nil (degrade to uncancellable rather
// than fail the operation): cancellation is a robustness improvement, not a
// correctness precondition, so a failure to allocate it must not break connect
// or fetch.
func newCtxWatcher(ctx context.Context) *ctxWatcher {
	if ctx == nil || ctx.Done() == nil {
		return nil
	}
	var token *C.kernel_cancel_token_t
	if err := call(func() C.KernelStatusCode { return C.kernel_cancel_token_new(&token) }); err != nil {
		klog("cancel token_new failed (proceeding uncancellable): %v", err)
		return nil
	}
	w := &ctxWatcher{token: token, done: make(chan struct{})}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		select {
		case <-ctx.Done():
			klog("ctxWatcher: ctx.Done (%v) → firing cancel token", ctx.Err())
			// Fire is purely local in the kernel (flip an atomic, wake the
			// select) — no RPC, so this never blocks. Errors are swallowed: the
			// only failure is a null handle, which can't happen here.
			_ = call(func() C.KernelStatusCode { return C.kernel_cancel_token_cancel(w.token) })
		case <-w.done:
		}
	}()
	return w
}

// cancelledErr builds the error returned when a blocking kernel call (connect,
// execute, or a result-stream fetch) was interrupted by the caller's context. It
// is the single home for the subtle dual-%w wrap the three cancellable call sites
// (OpenSession, execute, nextBatch) share, so the contract lives in one place
// rather than being re-derived — and re-risked — at each site.
//
// It prefers the ctx error (database/sql convention: a cancelled call reports the
// cancellation) while keeping the kernel error reachable: BOTH are wrapped with %w
// so errors.Is(err, context.Canceled / context.DeadlineExceeded) still matches AND
// the underlying *KernelError stays reachable via errors.As. Without the second
// wrap, a session-fatal failure racing a cancel would lose its sqlstate / queryId —
// the one handle to what actually went wrong server-side.
//
// op names the interrupted call ("session_open", "execute", "next_batch") for the
// message. kernelErr is the already-classified kernel error (toConnError on the
// connect path, toStatementError on the statement path); ctxErr is ctx.Err().
//
// It lives here (a tagged file) rather than in the untagged errors_classify.go
// because its only callers are the tagged cgo call sites — under the default
// CGO_ENABLED=0 build the linter would see it as unused.
func cancelledErr(op string, ctxErr, kernelErr error) error {
	return fmt.Errorf("kernel: %s cancelled: %w (kernel error: %w)", op, ctxErr, kernelErr)
}

// tokenPtr returns the underlying token pointer to pass to a *_cancellable
// entry point, or NULL for a nil watcher (uncancellable ctx → the call behaves
// exactly like its plain, non-cancellable variant).
func (w *ctxWatcher) tokenPtr() *C.kernel_cancel_token_t {
	if w == nil {
		return nil
	}
	return w.token
}

// stop drains the watcher goroutine and frees the token. Safe to defer
// immediately after newCtxWatcher. Ordering matters: the watcher may be
// mid-fire (inside kernel_cancel_token_cancel) when the blocking call returns,
// so we close(done) to stop a not-yet-fired watcher, Wait() for any in-flight
// fire to finish, and only then free the token — never free it out from under a
// concurrent cancel (the kernel documents the token as single-owner for
// teardown).
func (w *ctxWatcher) stop() {
	if w == nil {
		return
	}
	close(w.done)
	w.wg.Wait()
	C.kernel_cancel_token_free(w.token)
	w.token = nil
}

// tokenFiredForTest reports whether this watcher's kernel token has been fired
// (kernel_cancel_token_is_cancelled). A test seam so a tagged unit test can
// assert the watcher-goroutine → token-fire wiring end to end through the real
// cgo boundary — without putting cgo in a _test.go file (which Go forbids). Not
// used in production. A nil watcher (uncancellable ctx) reports false.
func (w *ctxWatcher) tokenFiredForTest() bool {
	if w == nil {
		return false
	}
	var fired C.bool
	if err := call(func() C.KernelStatusCode {
		return C.kernel_cancel_token_is_cancelled(w.token, &fired)
	}); err != nil {
		return false
	}
	return bool(fired)
}
