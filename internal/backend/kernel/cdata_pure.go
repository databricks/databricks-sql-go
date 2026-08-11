//go:build databricks_kernel_dynamic

// Pure-Go (CGO_ENABLED=0) importer for the Arrow C Data Interface.
//
// This is the data-plane counterpart to dynamic_loader.go. The shipped cgo
// backend imports result batches with arrow-go/v12's `cdata` package, but that
// package is cgo (every non-test file does `import "C"`), so it cannot be used
// from a CGO_ENABLED=0 build. This file reimplements the *import* side of the C
// Data Interface in pure Go:
//
//   - The flat C structs (ArrowSchema / ArrowArray) are mirrored as Go structs
//     with a byte-for-byte identical 64-bit layout, read via unsafe.Pointer.
//   - The `release` callback embedded in each struct (a C function pointer) is
//     invoked with purego.SyscallN — the pure-Go equivalent of calling
//     ArrowArrayRelease / ArrowSchemaRelease.
//   - Buffers are referenced zero-copy (unsafe.Slice over the C pointers), so the
//     imported arrow.Record shares the kernel's memory exactly as the cgo path
//     does; a runtime finalizer calls the release callback when the Go GC
//     reclaims the ArrayData.
//
// The logic is a faithful port of apache/arrow-go v12.0.1
// (arrow/cdata/cdata.go + interface.go, Apache-2.0), which is the version this
// driver pins — kept close to the original so it can be diffed against upstream.
// Only the import path is ported (the kernel pulls batches via next_batch; the
// ArrowArrayStream reader is not needed). Export is not needed either.
package kernel

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/ebitengine/purego"
)

// Arrow C Data Interface flags (arrow/c/abi.h).
const (
	flagDictionaryOrdered = 1
	flagNullable          = 2
	flagMapKeysSorted     = 4
)

// cArrowSchema mirrors `struct ArrowSchema` from the Arrow C Data Interface
// (also declared in databricks_kernel.h). 64-bit layout, all fields 8-byte
// aligned so there is no padding:
//
//	const char* format;        // 0
//	const char* name;          // 8
//	const char* metadata;      // 16
//	int64_t     flags;         // 24
//	int64_t     n_children;    // 32
//	ArrowSchema** children;    // 40
//	ArrowSchema*  dictionary;  // 48
//	void (*release)(...);      // 56
//	void*       private_data;  // 64  (total 72)
type cArrowSchema struct {
	format     uintptr
	name       uintptr
	metadata   uintptr
	flags      int64
	nChildren  int64
	children   uintptr
	dictionary uintptr
	release    uintptr
	privData   uintptr
}

// cArrowArray mirrors `struct ArrowArray`. 64-bit layout, all 8-byte aligned:
//
//	int64_t length;            // 0
//	int64_t null_count;        // 8
//	int64_t offset;            // 16
//	int64_t n_buffers;         // 24
//	int64_t n_children;        // 32
//	const void** buffers;      // 40
//	ArrowArray** children;     // 48
//	ArrowArray*  dictionary;   // 56
//	void (*release)(...);      // 64
//	void*       private_data;  // 72  (total 80)
type cArrowArray struct {
	length     int64
	nullCount  int64
	offset     int64
	nBuffers   int64
	nChildren  int64
	buffers    uintptr
	children   uintptr
	dictionary uintptr
	release    uintptr
	privData   uintptr
}

// releaseCArray invokes the array's C release callback (idempotent: the callback
// nulls its own release pointer; we also clear ours). This is the pure-Go
// ArrowArrayRelease. The callback receives &arr synchronously and does not
// retain it, so passing a Go pointer is safe (arr holds no Go pointers).
func releaseCArray(arr *cArrowArray) {
	if arr != nil && arr.release != 0 {
		purego.SyscallN(arr.release, uintptr(unsafe.Pointer(arr)))
		arr.release = 0
	}
}

// releaseCSchema is the pure-Go ArrowSchemaRelease.
func releaseCSchema(s *cArrowSchema) {
	if s != nil && s.release != 0 {
		purego.SyscallN(s.release, uintptr(unsafe.Pointer(s)))
		s.release = 0
	}
}

// arrayMove is the pure-Go ArrowArrayMove: copy the struct and mark the source
// released so only the destination owns the memory.
func arrayMove(src, dst *cArrowArray) {
	*dst = *src
	src.release = 0
}

// formatToSimpleType maps C Data format strings to param-free arrow types.
var formatToSimpleType = map[string]arrow.DataType{
	"n": arrow.Null, "b": arrow.FixedWidthTypes.Boolean,
	"c": arrow.PrimitiveTypes.Int8, "C": arrow.PrimitiveTypes.Uint8,
	"s": arrow.PrimitiveTypes.Int16, "S": arrow.PrimitiveTypes.Uint16,
	"i": arrow.PrimitiveTypes.Int32, "I": arrow.PrimitiveTypes.Uint32,
	"l": arrow.PrimitiveTypes.Int64, "L": arrow.PrimitiveTypes.Uint64,
	"e": arrow.FixedWidthTypes.Float16, "f": arrow.PrimitiveTypes.Float32,
	"g": arrow.PrimitiveTypes.Float64, "z": arrow.BinaryTypes.Binary,
	"Z": arrow.BinaryTypes.LargeBinary, "u": arrow.BinaryTypes.String,
	"U": arrow.BinaryTypes.LargeString, "tdD": arrow.FixedWidthTypes.Date32,
	"tdm": arrow.FixedWidthTypes.Date64, "tts": arrow.FixedWidthTypes.Time32s,
	"ttm": arrow.FixedWidthTypes.Time32ms, "ttu": arrow.FixedWidthTypes.Time64us,
	"ttn": arrow.FixedWidthTypes.Time64ns, "tDs": arrow.FixedWidthTypes.Duration_s,
	"tDm": arrow.FixedWidthTypes.Duration_ms, "tDu": arrow.FixedWidthTypes.Duration_us,
	"tDn": arrow.FixedWidthTypes.Duration_ns, "tiM": arrow.FixedWidthTypes.MonthInterval,
	"tiD": arrow.FixedWidthTypes.DayTimeInterval, "tin": arrow.FixedWidthTypes.MonthDayNanoInterval,
}

// decodeCMetadata decodes C Data metadata (int32-length-prefixed key/value
// pairs). Faithful port of the cgo version.
func decodeCMetadata(md uintptr) arrow.Metadata {
	if md == 0 {
		return arrow.Metadata{}
	}
	pos := md
	readint32 := func() int32 {
		v := *(*int32)(unsafe.Pointer(pos))
		pos += 4
		return v
	}
	readstr := func() string {
		l := readint32()
		s := string(unsafe.Slice((*byte)(unsafe.Pointer(pos)), l))
		pos += uintptr(l)
		return s
	}
	npairs := readint32()
	if npairs == 0 {
		return arrow.Metadata{}
	}
	keys := make([]string, npairs)
	vals := make([]string, npairs)
	for i := int32(0); i < npairs; i++ {
		keys[i] = readstr()
		vals[i] = readstr()
	}
	return arrow.NewMetadata(keys, vals)
}

func schemaChildrenSlice(s *cArrowSchema) []*cArrowSchema {
	if s.nChildren == 0 || s.children == 0 {
		return nil
	}
	ptrs := unsafe.Slice((*uintptr)(unsafe.Pointer(s.children)), s.nChildren)
	out := make([]*cArrowSchema, len(ptrs))
	for i, p := range ptrs {
		out[i] = (*cArrowSchema)(unsafe.Pointer(p))
	}
	return out
}

// importSchema converts a cArrowSchema to an arrow.Field, always releasing the
// schema (even on error), matching the cgo semantics.
func importSchema(schema *cArrowSchema) (ret arrow.Field, err error) {
	defer releaseCSchema(schema)

	var childFields []arrow.Field
	if schema.nChildren > 0 {
		kids := schemaChildrenSlice(schema)
		childFields = make([]arrow.Field, len(kids))
		for i, c := range kids {
			childFields[i], err = importSchema(c)
			if err != nil {
				return
			}
		}
	}

	ret.Name = goStringFromC(schema.name)
	ret.Nullable = (schema.flags & flagNullable) != 0
	ret.Metadata = decodeCMetadata(schema.metadata)

	f := goStringFromC(schema.format)
	if dt, ok := formatToSimpleType[f]; ok {
		ret.Type = dt
		if schema.dictionary != 0 {
			valueField, e := importSchema((*cArrowSchema)(unsafe.Pointer(schema.dictionary)))
			if e != nil {
				return ret, e
			}
			ret.Type = &arrow.DictionaryType{
				IndexType: ret.Type,
				ValueType: valueField.Type,
				Ordered:   (*cArrowSchema)(unsafe.Pointer(schema.dictionary)).flags&flagDictionaryOrdered != 0,
			}
		}
		return
	}

	var dt arrow.DataType
	typs := strings.Split(f, ":")
	const defaulttz = "UTC"
	switch typs[0] {
	case "tss":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Second, TimeZone: tz}
	case "tsm":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: tz}
	case "tsu":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tz}
	case "tsn":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: tz}
	case "w":
		byteWidth, e := strconv.Atoi(typs[1])
		if e != nil {
			return ret, e
		}
		dt = &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}
	case "d":
		propList := strings.Split(typs[1], ",")
		if len(propList) == 3 {
			return ret, errors.New("only decimal128 is supported")
		}
		precision, _ := strconv.Atoi(propList[0])
		scale, _ := strconv.Atoi(propList[1])
		dt = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
	}

	if f[0] == '+' { // nested types
		switch f[1] {
		case 'l':
			dt = arrow.ListOfField(childFields[0])
		case 'L':
			dt = arrow.LargeListOfField(childFields[0])
		case 'w':
			listSize, e := strconv.Atoi(strings.Split(f, ":")[1])
			if e != nil {
				return ret, e
			}
			dt = arrow.FixedSizeListOfField(int32(listSize), childFields[0])
		case 's':
			dt = arrow.StructOf(childFields...)
		case 'm':
			st := childFields[0].Type.(*arrow.StructType)
			dt = arrow.MapOf(st.Field(0).Type, st.Field(1).Type)
			dt.(*arrow.MapType).KeysSorted = (schema.flags & flagMapKeysSorted) != 0
		case 'u':
			var mode arrow.UnionMode
			switch f[2] {
			case 'd':
				mode = arrow.DenseMode
			case 's':
				mode = arrow.SparseMode
			default:
				return ret, fmt.Errorf("%w: invalid union type", arrow.ErrInvalid)
			}
			codes := strings.Split(strings.Split(f, ":")[1], ",")
			typeCodes := make([]arrow.UnionTypeCode, 0, len(codes))
			for _, i := range codes {
				v, e := strconv.ParseInt(i, 10, 8)
				if e != nil {
					return ret, fmt.Errorf("%w: invalid type code: %s", arrow.ErrInvalid, e)
				}
				if v < 0 {
					return ret, fmt.Errorf("%w: negative type code in union: %s", arrow.ErrInvalid, f)
				}
				typeCodes = append(typeCodes, arrow.UnionTypeCode(v))
			}
			if len(childFields) != len(typeCodes) {
				return ret, fmt.Errorf("%w: children incompatible with format string", arrow.ErrInvalid)
			}
			dt = arrow.UnionOf(mode, childFields, typeCodes)
		}
	}

	if dt == nil {
		err = errors.New("unimplemented type: " + f)
	} else {
		ret.Type = dt
	}
	return
}

// cimporter tracks state while importing a cArrowArray tree.
type cimporter struct {
	dt       arrow.DataType
	arr      *cArrowArray
	data     arrow.ArrayData
	parent   *cimporter
	children []cimporter
	cbuffers []uintptr
}

func (imp *cimporter) importChild(parent *cimporter, src *cArrowArray) error {
	imp.parent = parent
	return imp.doImport(src)
}

func (imp *cimporter) arrayChildrenSlice() []*cArrowArray {
	if imp.arr.nChildren == 0 || imp.arr.children == 0 {
		return nil
	}
	ptrs := unsafe.Slice((*uintptr)(unsafe.Pointer(imp.arr.children)), imp.arr.nChildren)
	out := make([]*cArrowArray, len(ptrs))
	for i, p := range ptrs {
		out[i] = (*cArrowArray)(unsafe.Pointer(p))
	}
	return out
}

func (imp *cimporter) doImportChildren() error {
	children := imp.arrayChildrenSlice()
	if len(children) > 0 {
		imp.children = make([]cimporter, len(children))
	}
	switch imp.dt.ID() {
	case arrow.LIST:
		imp.children[0].dt = imp.dt.(*arrow.ListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.LARGE_LIST:
		imp.children[0].dt = imp.dt.(*arrow.LargeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.FIXED_SIZE_LIST:
		imp.children[0].dt = imp.dt.(*arrow.FixedSizeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.STRUCT:
		st := imp.dt.(*arrow.StructType)
		for i, c := range children {
			imp.children[i].dt = st.Field(i).Type
			if err := imp.children[i].importChild(imp, c); err != nil {
				return err
			}
		}
	case arrow.MAP:
		imp.children[0].dt = imp.dt.(*arrow.MapType).ValueType()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.DENSE_UNION:
		dt := imp.dt.(*arrow.DenseUnionType)
		for i, c := range children {
			imp.children[i].dt = dt.Fields()[i].Type
			if err := imp.children[i].importChild(imp, c); err != nil {
				return err
			}
		}
	case arrow.SPARSE_UNION:
		dt := imp.dt.(*arrow.SparseUnionType)
		for i, c := range children {
			imp.children[i].dt = dt.Fields()[i].Type
			if err := imp.children[i].importChild(imp, c); err != nil {
				return err
			}
		}
	}
	return nil
}

func (imp *cimporter) doImport(src *cArrowArray) error {
	imp.arr = new(cArrowArray)
	// Move src into our heap struct so a finalizer on the resulting ArrayData
	// releases the C memory when the GC reclaims it (mirrors the cgo path).
	arrayMove(src, imp.arr)
	movedArr := imp.arr
	defer func() {
		if imp.data != nil {
			// Finalizer safety net: release the kernel buffers when the Go GC
			// reclaims the ArrayData. Release is idempotent, so an explicit
			// Release() elsewhere never double-frees. (arrow-go itself sets a
			// finalizer on the ArrayData too; this one frees the C-side array.)
			setArrayDataFinalizer(imp.data, movedArr)
		}
	}()

	if err := imp.doImportChildren(); err != nil {
		return err
	}

	if imp.arr.nBuffers > 0 {
		imp.cbuffers = unsafe.Slice((*uintptr)(unsafe.Pointer(imp.arr.buffers)), imp.arr.nBuffers)
	}

	switch dt := imp.dt.(type) {
	case *arrow.NullType:
		if err := imp.checkNoChildren(); err != nil {
			return err
		}
		imp.data = array.NewData(dt, int(imp.arr.length), nil, nil, int(imp.arr.nullCount), int(imp.arr.offset))
	case arrow.FixedWidthDataType:
		return imp.importFixedSizePrimitive()
	case *arrow.StringType:
		return imp.importStringLike(int64(arrow.Int32SizeBytes))
	case *arrow.BinaryType:
		return imp.importStringLike(int64(arrow.Int32SizeBytes))
	case *arrow.LargeStringType:
		return imp.importStringLike(int64(arrow.Int64SizeBytes))
	case *arrow.LargeBinaryType:
		return imp.importStringLike(int64(arrow.Int64SizeBytes))
	case *arrow.ListType:
		return imp.importListLike()
	case *arrow.LargeListType:
		return imp.importListLike()
	case *arrow.MapType:
		return imp.importListLike()
	case *arrow.FixedSizeListType:
		if err := imp.checkNumChildren(1); err != nil {
			return err
		}
		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}
		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}
		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.nullCount), int(imp.arr.offset))
	case *arrow.StructType:
		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}
		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}
		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}
		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, children, int(imp.arr.nullCount), int(imp.arr.offset))
	default:
		return fmt.Errorf("unimplemented type %s", dt)
	}
	return nil
}

func (imp *cimporter) importStringLike(offsetByteWidth int64) (err error) {
	if err = imp.checkNoChildren(); err != nil {
		return
	}
	if err = imp.checkNumBuffers(3); err != nil {
		return
	}
	var nulls, offsets, values *memory.Buffer
	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}
	if offsets, err = imp.importOffsetsBuffer(1, offsetByteWidth); err != nil {
		return
	}
	var nvals int64
	switch offsetByteWidth {
	case 4:
		typedOffsets := arrow.Int32Traits.CastFromBytes(offsets.Bytes())
		nvals = int64(typedOffsets[imp.arr.offset+imp.arr.length])
	case 8:
		typedOffsets := arrow.Int64Traits.CastFromBytes(offsets.Bytes())
		nvals = typedOffsets[imp.arr.offset+imp.arr.length]
	}
	if values, err = imp.importVariableValuesBuffer(2, 1, nvals); err != nil {
		return
	}
	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets, values}, nil, int(imp.arr.nullCount), int(imp.arr.offset))
	return
}

func (imp *cimporter) importListLike() (err error) {
	if err = imp.checkNumChildren(1); err != nil {
		return
	}
	if err = imp.checkNumBuffers(2); err != nil {
		return
	}
	var nulls, offsets *memory.Buffer
	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}
	offsetSize := imp.dt.Layout().Buffers[1].ByteWidth
	if offsets, err = imp.importOffsetsBuffer(1, int64(offsetSize)); err != nil {
		return
	}
	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.nullCount), int(imp.arr.offset))
	return
}

func (imp *cimporter) importFixedSizePrimitive() error {
	if err := imp.checkNoChildren(); err != nil {
		return err
	}
	if err := imp.checkNumBuffers(2); err != nil {
		return err
	}
	nulls, err := imp.importNullBitmap(0)
	if err != nil {
		return err
	}
	var values *memory.Buffer
	fw := imp.dt.(arrow.FixedWidthDataType)
	if bitutil.IsMultipleOf8(int64(fw.BitWidth())) {
		values, err = imp.importFixedSizeBuffer(1, bitutil.BytesForBits(int64(fw.BitWidth())))
	} else {
		if fw.BitWidth() != 1 {
			return errors.New("invalid bitwidth")
		}
		values, err = imp.importBitsBuffer(1)
	}
	if err != nil {
		return err
	}
	var dict *array.Data
	if dt, ok := imp.dt.(*arrow.DictionaryType); ok {
		dictImp := &cimporter{dt: dt.ValueType}
		if err := dictImp.doImport((*cArrowArray)(unsafe.Pointer(imp.arr.dictionary))); err != nil {
			return err
		}
		defer dictImp.data.Release()
		dict = dictImp.data.(*array.Data)
	}
	imp.data = array.NewDataWithDictionary(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, values}, int(imp.arr.nullCount), int(imp.arr.offset), dict)
	return nil
}

func (imp *cimporter) checkNoChildren() error { return imp.checkNumChildren(0) }

func (imp *cimporter) checkNumChildren(n int64) error {
	if imp.arr.nChildren != n {
		return fmt.Errorf("expected %d children for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.nChildren)
	}
	return nil
}

func (imp *cimporter) checkNumBuffers(n int64) error {
	if imp.arr.nBuffers != n {
		return fmt.Errorf("expected %d buffers for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.nBuffers)
	}
	return nil
}

func (imp *cimporter) importBuffer(bufferID int, sz int64) (*memory.Buffer, error) {
	if imp.cbuffers[bufferID] == 0 {
		if sz != 0 {
			return nil, errors.New("invalid buffer")
		}
		return memory.NewBufferBytes([]byte{}), nil
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(imp.cbuffers[bufferID])), sz)
	return memory.NewBufferBytes(data), nil
}

func (imp *cimporter) importBitsBuffer(bufferID int) (*memory.Buffer, error) {
	bufsize := bitutil.BytesForBits(imp.arr.length + imp.arr.offset)
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importNullBitmap(bufferID int) (*memory.Buffer, error) {
	if imp.arr.nullCount > 0 && imp.cbuffers[bufferID] == 0 {
		return nil, fmt.Errorf("ArrowArray has null bitmap buffer but non-zero null_count %d", imp.arr.nullCount)
	}
	if imp.arr.nullCount == 0 && imp.cbuffers[bufferID] == 0 {
		return nil, nil
	}
	return imp.importBitsBuffer(bufferID)
}

func (imp *cimporter) importFixedSizeBuffer(bufferID int, byteWidth int64) (*memory.Buffer, error) {
	return imp.importBuffer(bufferID, byteWidth*(imp.arr.length+imp.arr.offset))
}

func (imp *cimporter) importOffsetsBuffer(bufferID int, offsetsize int64) (*memory.Buffer, error) {
	return imp.importBuffer(bufferID, offsetsize*(imp.arr.length+imp.arr.offset+1))
}

func (imp *cimporter) importVariableValuesBuffer(bufferID int, byteWidth, nvals int64) (*memory.Buffer, error) {
	return imp.importBuffer(bufferID, byteWidth*nvals)
}

func importCArrayAsType(arr *cArrowArray, dt arrow.DataType) (*cimporter, error) {
	imp := &cimporter{dt: dt}
	err := imp.doImport(arr)
	return imp, err
}

// importCArrowSchema imports a record-batch schema (top level must be a struct).
func importCArrowSchema(out *cArrowSchema) (*arrow.Schema, error) {
	ret, err := importSchema(out)
	if err != nil {
		return nil, err
	}
	st, ok := ret.Type.(*arrow.StructType)
	if !ok {
		return nil, errors.New("recordbatch schema import must be of struct type")
	}
	return arrow.NewSchema(st.Fields(), &ret.Metadata), nil
}

// importCRecordBatchWithSchema imports an array as a record batch, schema known.
func importCRecordBatchWithSchema(arr *cArrowArray, sc *arrow.Schema) (arrow.Record, error) {
	imp, err := importCArrayAsType(arr, arrow.StructOf(sc.Fields()...))
	if err != nil {
		return nil, err
	}
	st := array.NewStructData(imp.data)
	defer st.Release()
	cols := make([]arrow.Array, st.NumField())
	for i := 0; i < st.NumField(); i++ {
		cols[i] = st.Field(i)
	}
	return array.NewRecord(sc, cols, int64(st.Len())), nil
}
