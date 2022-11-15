package queryctx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
