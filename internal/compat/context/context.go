package context

import (
	"context"
	"time"
)

// This implementation has been copied from https://github.com/golang/go/blob/master/src/context/context.go#L581
// and adapted to work with this package.

// WithoutCancel returns a derived context that points to the parent context
// and is not canceled when parent is canceled.
// The returned context returns no Deadline or Err, and its Done channel is nil.
func WithoutCancel(parent context.Context) context.Context {
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	return withoutCancelCtx{parent}
}

type withoutCancelCtx struct {
	c context.Context
}

func (withoutCancelCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (withoutCancelCtx) Done() <-chan struct{} {
	return nil
}

func (withoutCancelCtx) Err() error {
	return nil
}

func (c withoutCancelCtx) Value(key any) any {
	return c.c.Value(key)
}
