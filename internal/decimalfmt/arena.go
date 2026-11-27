package decimalfmt

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

const arenaChunkSize = 32 << 10

// Arena renders exact decimals into append-only Go-owned string chunks.
// Returned strings remain valid after Reset. An Arena must not be copied or
// used concurrently; each retained string pins at most one chunk.
type Arena struct {
	buffer strings.Builder
}

// ExactString shares the current chunk instead of allocating per value.
func (arena *Arena) ExactString(number decimal128.Num, scale int32) string {
	var scratch [decMaxLen]byte
	rendered := Append(scratch[:0], number, scale)
	if arena.buffer.Cap()-arena.buffer.Len() < len(rendered) {
		arena.buffer.Reset()
		arena.buffer.Grow(max(arenaChunkSize, len(rendered)))
	}
	start := arena.buffer.Len()
	_, _ = arena.buffer.Write(rendered)
	return arena.buffer.String()[start:]
}

// Reset releases the arena's reference without modifying returned strings.
func (arena *Arena) Reset() {
	arena.buffer.Reset()
}
