package arrowscan

import (
	"encoding/binary"
	"fmt"
)

const ewkbSRIDFlag uint32 = 0x20000000

// wkbToEWKB keeps Databricks' WKB payload intact and adds the standard EWKB
// SRID flag and value to the outer geometry header. Nested geometries inherit
// the outer SRID, so collections require no recursive rewriting.
func wkbToEWKB(wkb []byte, srid int32) ([]byte, error) {
	if len(wkb) < 5 {
		return nil, fmt.Errorf("malformed WKB: geometry header is truncated")
	}

	var order binary.ByteOrder
	switch wkb[0] {
	case 0:
		order = binary.BigEndian
	case 1:
		order = binary.LittleEndian
	default:
		return nil, fmt.Errorf("malformed WKB: invalid byte order %d", wkb[0])
	}

	geometryType := order.Uint32(wkb[1:5])
	if geometryType&ewkbSRIDFlag != 0 {
		return nil, fmt.Errorf("expected OGC WKB without an embedded SRID")
	}

	ewkb := make([]byte, len(wkb)+4)
	ewkb[0] = wkb[0]
	order.PutUint32(ewkb[1:5], geometryType|ewkbSRIDFlag)
	order.PutUint32(ewkb[5:9], uint32(srid))
	copy(ewkb[9:], wkb[5:])
	return ewkb, nil
}
