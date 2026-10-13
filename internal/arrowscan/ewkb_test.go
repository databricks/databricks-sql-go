package arrowscan

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestWKBToEWKBConvertsISODimensions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		isoType  uint32
		wantType uint32
		dims     int
	}{
		{name: "XY", isoType: 1, wantType: ewkbSRIDFlag | 1, dims: 2},
		{name: "Z", isoType: 1001, wantType: ewkbZFlag | ewkbSRIDFlag | 1, dims: 3},
		{name: "M", isoType: 2001, wantType: ewkbMFlag | ewkbSRIDFlag | 1, dims: 3},
		{name: "ZM", isoType: 3001, wantType: ewkbZFlag | ewkbMFlag | ewkbSRIDFlag | 1, dims: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := pointWKB(binary.LittleEndian, tc.isoType, tc.dims)
			got, err := wkbToEWKB(input, 4326)
			if err != nil {
				t.Fatal(err)
			}
			if got[0] != 1 || binary.LittleEndian.Uint32(got[1:5]) != tc.wantType {
				t.Fatalf("EWKB header = %x, want type %#x", got[:5], tc.wantType)
			}
			if binary.LittleEndian.Uint32(got[5:9]) != 4326 {
				t.Fatalf("EWKB SRID = %d, want 4326", binary.LittleEndian.Uint32(got[5:9]))
			}
			if !bytes.Equal(got[9:], input[5:]) {
				t.Fatalf("coordinate payload changed: got %x, want %x", got[9:], input[5:])
			}
		})
	}
}

func TestWKBToEWKBHandlesBigEndian(t *testing.T) {
	input := pointWKB(binary.BigEndian, 1001, 3)
	got, err := wkbToEWKB(input, 3857)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0 || binary.BigEndian.Uint32(got[1:5]) != ewkbZFlag|ewkbSRIDFlag|1 {
		t.Fatalf("EWKB header = %x", got[:5])
	}
	if binary.BigEndian.Uint32(got[5:9]) != 3857 {
		t.Fatalf("EWKB SRID = %d, want 3857", binary.BigEndian.Uint32(got[5:9]))
	}
}

func TestWKBToEWKBRewritesNestedCollectionHeaders(t *testing.T) {
	input := []byte{1}
	input = appendTestUint32(input, binary.LittleEndian, 1007)
	input = appendTestUint32(input, binary.LittleEndian, 1)
	input = append(input, pointWKB(binary.LittleEndian, 1001, 3)...)

	got, err := wkbToEWKB(input, 4326)
	if err != nil {
		t.Fatal(err)
	}
	if outer := binary.LittleEndian.Uint32(got[1:5]); outer != ewkbZFlag|ewkbSRIDFlag|7 {
		t.Fatalf("collection type = %#x", outer)
	}
	if child := binary.LittleEndian.Uint32(got[14:18]); child != ewkbZFlag|1 {
		t.Fatalf("nested point type = %#x", child)
	}
}

func TestWKBToEWKBRejectsMalformedPayloads(t *testing.T) {
	for _, input := range [][]byte{
		{},
		{2, 1, 0, 0, 0},
		{1, 1, 0, 0, 0},
		append(pointWKB(binary.LittleEndian, 1, 2), 0),
	} {
		if _, err := wkbToEWKB(input, 0); err == nil {
			t.Fatalf("wkbToEWKB(%x) succeeded, want error", input)
		}
	}
}

func pointWKB(order binary.ByteOrder, geometryType uint32, dimensions int) []byte {
	marker := byte(0)
	if order == binary.LittleEndian {
		marker = 1
	}
	output := appendTestUint32([]byte{marker}, order, geometryType)
	for ordinate := 0; ordinate < dimensions; ordinate++ {
		var encoded [8]byte
		order.PutUint64(encoded[:], uint64(ordinate+1))
		output = append(output, encoded[:]...)
	}
	return output
}

func appendTestUint32(output []byte, order binary.ByteOrder, value uint32) []byte {
	var encoded [4]byte
	order.PutUint32(encoded[:], value)
	return append(output, encoded[:]...)
}
