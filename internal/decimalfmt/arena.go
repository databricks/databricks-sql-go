package decimalfmt

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

const arenaChunkSize = 4 << 10

// Arena renders exact decimals into append-only Go-owned string chunks.
// Returned strings remain valid after Reset. An Arena must not be copied or
// used concurrently; each retained string pins at most one chunk.
type Arena struct {
	buffer []byte
}

// ExactString shares the current chunk instead of allocating per value.
func (arena *Arena) ExactString(number decimal128.Num, scale int32) string {
	if scale > decMaxLen-3 {
		return ExactString(number, scale)
	}
	if cap(arena.buffer)-len(arena.buffer) < decMaxLen {
		arena.buffer = make([]byte, 0, arenaChunkSize)
	}
	start := len(arena.buffer)
	arena.buffer = Append(arena.buffer, number, scale)
	return unsafe.String(&arena.buffer[start], len(arena.buffer)-start) //nolint:gosec // G103: exposed Go-owned bytes are never modified or pooled.
}

// Reset releases the arena's reference without modifying returned strings.
func (arena *Arena) Reset() {
	arena.buffer = nil
}
