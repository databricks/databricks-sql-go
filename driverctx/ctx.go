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
	QueryIdContextKey
	QueryIdCallbackKey
	ConnIdCallbackKey
	StagingAllowedLocalPathKey
	QueryTagsContextKey
)

type IdCallbackFunc func(string)

// NewContextWithCorrelationId creates a new context with correlationId value. Used by Logger to populate field corrId.
func NewContextWithCorrelationId(ctx context.Context, correlationId string) context.Context {
	return context.WithValue(ctx, CorrelationIdContextKey, correlationId)
}

// CorrelationIdFromContext retrieves the correlationId stored in context.
func CorrelationIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	corrId, ok := ctx.Value(CorrelationIdContextKey).(string)
	if !ok {
		return ""
	}
	return corrId
}

// NewContextWithConnId creates a new context with connectionId value.
// The connection ID will be displayed in log messages and other dianostic information.
func NewContextWithConnId(ctx context.Context, connId string) context.Context {
	if callback, ok := ctx.Value(ConnIdCallbackKey).(IdCallbackFunc); ok {
		callback(connId)
	}
	return context.WithValue(ctx, ConnIdContextKey, connId)
}

// ConnIdFromContext retrieves the connectionId stored in context.
func ConnIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	connId, ok := ctx.Value(ConnIdContextKey).(string)
	if !ok {
		return ""
	}
	return connId
}

// NewContextWithQueryId creates a new context with queryId value.
// The query id will be displayed in log messages and other diagnostic information.
func NewContextWithQueryId(ctx context.Context, queryId string) context.Context {
	if callback, ok := ctx.Value(QueryIdCallbackKey).(IdCallbackFunc); ok {
		callback(queryId)
	}

	return context.WithValue(ctx, QueryIdContextKey, queryId)
}

// QueryIdFromContext retrieves the queryId stored in context.
func QueryIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	queryId, ok := ctx.Value(QueryIdContextKey).(string)
	if !ok {
		return ""
	}
	return queryId
}

// QueryIdFromContext retrieves the queryId stored in context.
func StagingPathsFromContext(ctx context.Context) []string {
	if ctx == nil {
		return []string{}
	}

	stagingAllowedLocalPath, ok := ctx.Value(StagingAllowedLocalPathKey).([]string)
	if !ok {
		return []string{}
	}
	return stagingAllowedLocalPath
}

func NewContextWithQueryIdCallback(ctx context.Context, callback IdCallbackFunc) context.Context {
	return context.WithValue(ctx, QueryIdCallbackKey, callback)
}

func NewContextWithConnIdCallback(ctx context.Context, callback IdCallbackFunc) context.Context {
	return context.WithValue(ctx, ConnIdCallbackKey, callback)
}

func NewContextWithStagingInfo(ctx context.Context, stagingAllowedLocalPath []string) context.Context {
	return context.WithValue(ctx, StagingAllowedLocalPathKey, stagingAllowedLocalPath)
}

// NewContextWithQueryTags creates a new context with per-statement query tags.
// These tags are serialized and passed via confOverlay as "query_tags" in TExecuteStatementReq.
// They apply only to the statement executed with this context and do not persist across queries.
func NewContextWithQueryTags(ctx context.Context, queryTags map[string]string) context.Context {
	return context.WithValue(ctx, QueryTagsContextKey, queryTags)
}

// QueryTagsFromContext retrieves the per-statement query tags stored in context.
func QueryTagsFromContext(ctx context.Context) map[string]string {
	if ctx == nil {
		return nil
	}

	queryTags, ok := ctx.Value(QueryTagsContextKey).(map[string]string)
	if !ok {
		return nil
	}
	return queryTags
}

func NewContextFromBackground(ctx context.Context) context.Context {
	connId := ConnIdFromContext(ctx)
	corrId := CorrelationIdFromContext(ctx)
	queryId := QueryIdFromContext(ctx)
	stagingPaths := StagingPathsFromContext(ctx)
	queryTags := QueryTagsFromContext(ctx)

	newCtx := NewContextWithConnId(context.Background(), connId)
	newCtx = NewContextWithCorrelationId(newCtx, corrId)
	newCtx = NewContextWithQueryId(newCtx, queryId)
	newCtx = NewContextWithStagingInfo(newCtx, stagingPaths)
	if queryTags != nil {
		newCtx = NewContextWithQueryTags(newCtx, queryTags)
	}

	return newCtx
}
