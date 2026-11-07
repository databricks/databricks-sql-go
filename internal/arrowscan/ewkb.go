package arrowscan

import (
	"encoding/binary"
	"fmt"
)

const (
	ewkbZFlag    uint32 = 0x80000000
	ewkbMFlag    uint32 = 0x40000000
	ewkbSRIDFlag uint32 = 0x20000000
	ewkbBBoxFlag uint32 = 0x10000000
)

// wkbToEWKB converts ISO/OGC WKB dimensional type offsets to EWKB flags at
// every geometry header and embeds the row SRID in the outer header.
func wkbToEWKB(wkb []byte, srid int32) ([]byte, error) {
	rewriter := wkbRewriter{
		input:  wkb,
		output: make([]byte, 0, len(wkb)+4),
	}
	if _, err := rewriter.rewriteGeometry(true, srid); err != nil {
		return nil, err
	}
	if rewriter.offset != len(wkb) {
		return nil, fmt.Errorf("malformed WKB: %d trailing bytes", len(wkb)-rewriter.offset)
	}
	return rewriter.output, nil
}

type wkbRewriter struct {
	input  []byte
	offset int
	output []byte
}

func (r *wkbRewriter) rewriteGeometry(includeSRID bool, srid int32) (uint32, error) {
	if len(r.input)-r.offset < 5 {
		return 0, fmt.Errorf("malformed WKB: geometry header is truncated")
	}

	marker := r.input[r.offset]
	r.offset++
	var order binary.ByteOrder
	switch marker {
	case 0:
		order = binary.BigEndian
	case 1:
		order = binary.LittleEndian
	default:
		return 0, fmt.Errorf("malformed WKB: invalid byte order %d", marker)
	}

	rawType, err := r.readUint32(order)
	if err != nil {
		return 0, err
	}
	baseType, dimensions, dimensionFlags, err := decodeISOWKBType(rawType)
	if err != nil {
		return 0, err
	}

	r.output = append(r.output, marker)
	outputType := baseType | dimensionFlags
	if includeSRID {
		outputType |= ewkbSRIDFlag
	}
	r.appendUint32(order, outputType)
	if includeSRID {
		r.appendUint32(order, uint32(srid)) // #nosec G115 -- EWKB stores the exact int32 bit pattern
	}

	switch baseType {
	case 1: // POINT
		err = r.copyBytes(uint64(dimensions) * 8)
	case 2: // LINESTRING
		err = r.copyCoordinateSequence(order, dimensions)
	case 3: // POLYGON
		var rings uint32
		rings, err = r.copyUint32(order)
		if err == nil {
			for ring := uint32(0); ring < rings; ring++ {
				if err = r.copyCoordinateSequence(order, dimensions); err != nil {
					break
				}
			}
		}
	case 4, 5, 6, 7: // MULTI* / GEOMETRYCOLLECTION
		var children uint32
		children, err = r.copyUint32(order)
		if err == nil {
			for child := uint32(0); child < children; child++ {
				var childType uint32
				childType, err = r.rewriteGeometry(false, 0)
				if err != nil {
					break
				}
				if baseType != 7 && childType != baseType-3 {
					err = fmt.Errorf(
						"malformed WKB: geometry type %d contains child type %d",
						baseType,
						childType,
					)
					break
				}
			}
		}
	default:
		return 0, fmt.Errorf("malformed WKB: unsupported geometry type %d", baseType)
	}
	if err != nil {
		return 0, err
	}
	return baseType, nil
}

func decodeISOWKBType(rawType uint32) (baseType uint32, dimensions uint32, flags uint32, err error) {
	if rawType&(ewkbZFlag|ewkbMFlag|ewkbSRIDFlag|ewkbBBoxFlag) != 0 {
		return 0, 0, 0, fmt.Errorf("malformed WKB: expected ISO/OGC type code, got EWKB flags in %#x", rawType)
	}

	switch {
	case rawType >= 3000 && rawType < 4000:
		baseType, dimensions, flags = rawType-3000, 4, ewkbZFlag|ewkbMFlag
	case rawType >= 2000 && rawType < 3000:
		baseType, dimensions, flags = rawType-2000, 3, ewkbMFlag
	case rawType >= 1000 && rawType < 2000:
		baseType, dimensions, flags = rawType-1000, 3, ewkbZFlag
	default:
		baseType, dimensions = rawType, 2
	}
	if baseType < 1 || baseType > 7 {
		return 0, 0, 0, fmt.Errorf("malformed WKB: unsupported geometry type %d", rawType)
	}
	return baseType, dimensions, flags, nil
}

func (r *wkbRewriter) readUint32(order binary.ByteOrder) (uint32, error) {
	if len(r.input)-r.offset < 4 {
		return 0, fmt.Errorf("malformed WKB: truncated uint32")
	}
	value := order.Uint32(r.input[r.offset : r.offset+4])
	r.offset += 4
	return value, nil
}

func (r *wkbRewriter) appendUint32(order binary.ByteOrder, value uint32) {
	var encoded [4]byte
	order.PutUint32(encoded[:], value)
	r.output = append(r.output, encoded[:]...)
}

func (r *wkbRewriter) copyUint32(order binary.ByteOrder) (uint32, error) {
	value, err := r.readUint32(order)
	if err != nil {
		return 0, err
	}
	r.appendUint32(order, value)
	return value, nil
}

func (r *wkbRewriter) copyCoordinateSequence(order binary.ByteOrder, dimensions uint32) error {
	points, err := r.copyUint32(order)
	if err != nil {
		return err
	}
	return r.copyBytes(uint64(points) * uint64(dimensions) * 8)
}

func (r *wkbRewriter) copyBytes(count uint64) error {
	remaining := r.input[r.offset:]
	if count > uint64(len(remaining)) {
		return fmt.Errorf("malformed WKB: payload is truncated")
	}
	payload := remaining[:count]
	r.output = append(r.output, payload...)
	r.offset += len(payload)
	return nil
}
