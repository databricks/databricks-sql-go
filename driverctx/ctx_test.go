package driverctx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewContextWithQueryTags(t *testing.T) {
	t.Run("stores and retrieves query tags", func(t *testing.T) {
		tags := map[string]string{"team": "engineering", "app": "etl"}
		ctx := NewContextWithQueryTags(context.Background(), tags)
		result := QueryTagsFromContext(ctx)
		assert.Equal(t, tags, result)
	})

	t.Run("returns nil for context without query tags", func(t *testing.T) {
		result := QueryTagsFromContext(context.Background())
		assert.Nil(t, result)
	})

	t.Run("it maintains timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		tags := map[string]string{"team": "eng"}
		ctx1 := NewContextWithQueryTags(ctx, tags)
		result := QueryTagsFromContext(ctx1)
		assert.Equal(t, tags, result)
		dl, ok := ctx.Deadline()
		dl1, ok1 := ctx1.Deadline()
		assert.Equal(t, dl, dl1)
		assert.True(t, ok)
		assert.True(t, ok1)
	})

	t.Run("NewContextFromBackground preserves query tags", func(t *testing.T) {
		tags := map[string]string{"team": "eng"}
		ctx := NewContextWithConnId(context.Background(), "conn-1")
		ctx = NewContextWithCorrelationId(ctx, "corr-1")
		ctx = NewContextWithQueryTags(ctx, tags)

		newCtx := NewContextFromBackground(ctx)
		assert.Equal(t, tags, QueryTagsFromContext(newCtx))
		assert.Equal(t, "conn-1", ConnIdFromContext(newCtx))
		assert.Equal(t, "corr-1", CorrelationIdFromContext(newCtx))
	})

	t.Run("NewContextFromBackground without query tags", func(t *testing.T) {
		ctx := NewContextWithConnId(context.Background(), "conn-1")
		newCtx := NewContextFromBackground(ctx)
		assert.Nil(t, QueryTagsFromContext(newCtx))
	})
}

func TestNewContextWithCorrelationId(t *testing.T) {
	t.Run("base case", func(t *testing.T) {

		ctx := NewContextWithCorrelationId(context.Background(), "abc")
		ctx1 := NewContextWithCorrelationId(context.Background(), "dfg")
		ctx2 := NewContextWithCorrelationId(context.Background(), "ghj")
		corrId := CorrelationIdFromContext(ctx)
		corrId1 := CorrelationIdFromContext(ctx1)
		corrId2 := CorrelationIdFromContext(ctx2)
		assert.Equal(t, "abc", corrId)
		assert.Equal(t, "dfg", corrId1)
		assert.Equal(t, "ghj", corrId2)
	})
	t.Run("it maintains timeout", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		ctx1 := NewContextWithCorrelationId(ctx, "dfg")
		corrId1 := CorrelationIdFromContext(ctx1)
		assert.Equal(t, "dfg", corrId1)
		dl, ok := ctx.Deadline()
		dl1, ok1 := ctx1.Deadline()
		assert.Equal(t, dl, dl1)
		assert.True(t, ok)
		assert.True(t, ok1)
	})

}
