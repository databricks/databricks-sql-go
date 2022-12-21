package arrowbased

import (
	"io"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/stretchr/testify/assert"
)

func TestLengArrowRowScanner(t *testing.T) {
	var dummy *arrowRowScanner
	assert.Equal(t, int64(0), dummy.NRows())

	rowSet := &cli_service.TRowSet{}

	ars, _ := NewArrowRowScanner(nil, nil, nil, nil)
	assert.Equal(t, int64(0), ars.NRows())

	ars, _ = NewArrowRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(0), ars.NRows())

	rowSet.ArrowBatches = []*cli_service.TSparkArrowBatch{}
	ars, _ = NewArrowRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(0), ars.NRows())

	rowSet.ArrowBatches = []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}
	ars, _ = NewArrowRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(5), ars.NRows())
}

func TestChunkedByteReader(t *testing.T) {
	c := chunkedByteReader{}
	nbytes, err := c.Read(nil)
	assert.Equal(t, io.EOF, err)
	assert.Zero(t, nbytes)

	chunkSets := [][][]byte{
		{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}},
		{{1, 2, 3, 4, 5, 6}, {7, 8}, {9}, {10, 11, 12}},
	}

	for i := range chunkSets {

		c.chunks = chunkSets[i]
		buf := make([]byte, 10)
		testReadingChunks(t, c, buf, []int{10, 2})

		c.reset()
		buf = make([]byte, 3)
		testReadingChunks(t, c, buf, []int{3, 3, 3, 3})

		c.reset()
		buf = make([]byte, 2)
		testReadingChunks(t, c, buf, []int{2, 2, 2, 2, 2, 2})

		c.reset()
		buf = make([]byte, 20)
		testReadingChunks(t, c, buf, []int{12})

		c.reset()
		buf = make([]byte, 1)
		testReadingChunks(t, c, buf, []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})

		c.reset()
		buf = make([]byte, 5)
		testReadingChunks(t, c, buf, []int{5, 5, 2})
	}
}

func testReadingChunks(t *testing.T, c chunkedByteReader, target []byte, readSizes []int) {

	for i, expectedSize := range readSizes {
		n, err := c.Read(target)
		assert.Equal(t, expectedSize, n)
		if i == len(readSizes)-1 {
			assert.Equal(t, io.EOF, err)
		} else {
			assert.Nil(t, err)
		}
	}

}
