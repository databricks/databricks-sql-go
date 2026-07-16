//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"testing"
	"time"
)

// A cancellable ctx that fires drives the watcher goroutine to fire the kernel
// cancel token — the wiring the *_cancellable entry points rely on. Exercised
// through the real cgo boundary via the tokenFiredForTest seam (cgo cannot be
// used directly in a _test.go file). This is the safety-critical
// ctx→token bridge; keeping it in the tagged unit test means it runs in the
// build-and-test-kernel CI job, not only in the live e2e.
func TestCtxWatcherFiresTokenOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := newCtxWatcher(ctx)
	if w == nil {
		t.Fatal("newCtxWatcher returned nil for a cancellable ctx")
	}
	defer w.stop()

	if w.tokenFiredForTest() {
		t.Fatal("token fired before ctx was cancelled")
	}

	cancel()

	// The watcher fires asynchronously; poll briefly for the flip.
	fired := false
	for i := 0; i < 200; i++ {
		if w.tokenFiredForTest() {
			fired = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !fired {
		t.Fatal("token was not fired after ctx cancellation")
	}
}

// A ctx whose deadline elapses also fires the token (the connect / fetch
// deadline path).
func TestCtxWatcherFiresTokenOnDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	w := newCtxWatcher(ctx)
	if w == nil {
		t.Fatal("newCtxWatcher returned nil for a ctx with a deadline")
	}
	defer w.stop()

	fired := false
	for i := 0; i < 200; i++ {
		if w.tokenFiredForTest() {
			fired = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !fired {
		t.Fatal("token was not fired after the ctx deadline elapsed")
	}
}

// A nil / non-cancellable ctx yields a nil watcher (NULL token → the call
// behaves exactly like its plain, non-cancellable variant), and stop()/tokenPtr
// are nil-safe so the call sites need no special-casing.
func TestCtxWatcherNilForUncancellableCtx(t *testing.T) {
	if w := newCtxWatcher(nil); w != nil {
		t.Error("newCtxWatcher(nil) should return nil")
	}
	// context.Background() has a nil Done() channel — non-cancellable.
	if w := newCtxWatcher(context.Background()); w != nil {
		t.Error("newCtxWatcher(background) should return nil (non-cancellable)")
	}
	// nil watcher is safe to use.
	var w *ctxWatcher
	if w.tokenPtr() != nil {
		t.Error("nil watcher tokenPtr should be NULL")
	}
	w.stop() // must not panic
	if w.tokenFiredForTest() {
		t.Error("nil watcher tokenFiredForTest should be false")
	}
}

// A watcher whose ctx never fires cleans up without the token being fired —
// the normal-completion path (stop() drains the idle watcher goroutine).
func TestCtxWatcherCleanUpWithoutFire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newCtxWatcher(ctx)
	if w == nil {
		t.Fatal("newCtxWatcher returned nil for a cancellable ctx")
	}
	if w.tokenFiredForTest() {
		t.Fatal("token fired without a cancel")
	}
	w.stop() // drains the still-waiting watcher; must not hang or fire
}
