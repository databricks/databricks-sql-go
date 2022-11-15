package driverctx

import (
	"context"
)

// Key name to look for Correlation Id in context
// using custom type to prevent key collision
type contextKey int

const (
	CorrelationIdContextKey contextKey = iota
	ConnIdContextKey
)

func NewContextWithCorrelationId(ctx context.Context, correlationId string) context.Context {
	return context.WithValue(ctx, CorrelationIdContextKey, correlationId)
}

func CorrelationIdFromContext(ctx context.Context) string {
	corrId, ok := ctx.Value(CorrelationIdContextKey).(string)
	if !ok {
		return ""
	}
	return corrId
}

func NewContextWithConnId(ctx context.Context, connId string) context.Context {
	return context.WithValue(ctx, ConnIdContextKey, connId)
}

func ConnIdFromContext(ctx context.Context) string {
	connId, ok := ctx.Value(ConnIdContextKey).(string)
	if !ok {
		return ""
	}
	return connId
}
