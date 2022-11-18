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

// NewContextWithCorrelationId creates a new context with correlationId value. Used by Logger to populate field corrId.
func NewContextWithCorrelationId(ctx context.Context, correlationId string) context.Context {
	return context.WithValue(ctx, CorrelationIdContextKey, correlationId)
}

// CorrelationIdFromContext retrieves the correlationId stored in context.
func CorrelationIdFromContext(ctx context.Context) string {
	corrId, ok := ctx.Value(CorrelationIdContextKey).(string)
	if !ok {
		return ""
	}
	return corrId
}

// NewContextWithConnId creates a new context with connectionId value.
func NewContextWithConnId(ctx context.Context, connId string) context.Context {
	return context.WithValue(ctx, ConnIdContextKey, connId)
}

// ConnIdFromContext retrieves the connectionId stored in context.
func ConnIdFromContext(ctx context.Context) string {
	connId, ok := ctx.Value(ConnIdContextKey).(string)
	if !ok {
		return ""
	}
	return connId
}
