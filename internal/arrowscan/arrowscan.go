// Package arrowscan converts Arrow array cells to database/sql driver.Values,
// with nested types (List/Map/Struct, and VARIANT which arrives nested) rendered
// to a JSON string byte-identical to the Thrift arrow path
// (internal/rows/arrowbased). It is pure Go (no cgo), so it is shared by the
// kernel backend and testable in the default CGO_ENABLED=0 build — the tests here
// are the regression guard for the exact rendering rules (native float32, exact
// decimals, time.Time formatting, JSON grammar) both backends must agree on.
//
// Rendering to JSON (not a Go map/slice) is deliberate: it is what the Thrift
// path returns, so a query's result is identical across backends.
//   - list        → [v0,v1,...]
//   - map         → {"k0":v0,"k1":v1,...}   (keys stringified)
//   - struct      → {"field0":v0,...}
//   - nested NULL → null
//   - time.Time   → quoted .String()  (matches the Thrift marshal() special-case)
//   - nested decimal → exact scale-applied JSON number literal (never a lossy
//     float64), matching Thrift's marshalScalar → ValueString
//   - float32     → native float32 (not widened to float64), so JSON renders
//     3.14, not 3.140000104904175
package arrowscan

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/databricks/databricks-sql-go/internal/decimalfmt"
)

// ScanCell extracts one cell as a driver.Value. Scalars map to their Go value:
// bool, all int/uint widths, float (native float32/float64), string, binary,
// date, timestamp, and top-level decimal (as an exact fixed-point string,
// matching the Thrift path — a float64 would lose precision beyond ~17 digits).
// Nested types (List/Map/Struct, and VARIANT which
// arrives nested) render to a JSON string byte-identical to the Thrift path;
// GEOMETRY arrives as a WKB/WKT string and is handled by the string arm. INTERVAL
// day-time/year-month arrive as native arrow duration/month-interval and format to
// the same string the Thrift path receives pre-formatted from the server. NULLs
// map to nil. A genuinely unhandled type returns an error rather than a silently
// wrong value. loc renders DATE / TIMESTAMP in the session time zone (nil = UTC,
// arrow's ToTime default).
func ScanCell(col arrow.Array, row int, loc *time.Location) (driver.Value, error) {
	return ScanCellCached(col, row, loc, nil)
}

// StructKeyCache memoizes the JSON-escaped `"name":` prefixes for a struct type,
// so writeStructJSON doesn't re-marshal constant field names on every row. It is
// caller-owned and must be scoped to a single result set (e.g. one driver.Rows)
// and discarded with it — NOT a process-global, which would leak.
//
// The Arrow C Data import allocates a fresh *StructType per batch, so a key is
// only ever hit within the batch that created it: across a multi-batch result the
// map would otherwise accrue one never-evicted entry per batch for the whole Rows
// lifetime. Callers should therefore Reset() the cache at each batch boundary —
// all rows of a batch share one imported Record, so the intra-batch win (escape
// each field name once per batch, not once per row) is fully preserved while the
// map stays bounded to a single batch's struct types. A nil cache is valid:
// rendering just recomputes the keys inline.
type StructKeyCache struct {
	m map[*arrow.StructType][]string
}

// NewStructKeyCache returns a cache ready to pass to ScanCellCached.
func NewStructKeyCache() *StructKeyCache {
	return &StructKeyCache{m: make(map[*arrow.StructType][]string)}
}

// Reset drops all memoized prefixes. Callers scope the cache to one batch by
// calling this when a new batch is imported (see StructKeyCache): the prior
// batch's *StructType keys can never be hit again, so keeping them only grows the
// map. Safe on a nil receiver.
func (c *StructKeyCache) Reset() {
	if c == nil {
		return
	}
	for k := range c.m {
		delete(c.m, k)
	}
}

// keyPrefixes returns the escaped `"name":` prefix for each field of st,
// memoized. Safe on a nil receiver (computes without caching).
func (c *StructKeyCache) keyPrefixes(st *arrow.StructType) []string {
	if c != nil {
		if p, ok := c.m[st]; ok {
			return p
		}
	}
	fields := st.Fields()
	prefixes := make([]string, len(fields))
	for i := range fields {
		name, _ := json.Marshal(fields[i].Name) // JSON-escapes the field name
		prefixes[i] = string(name) + ":"
	}
	if c != nil {
		c.m[st] = prefixes
	}
	return prefixes
}

// ScanCellCached is ScanCell with a caller-owned StructKeyCache (see
// StructKeyCache) so struct field-name keys are escaped once per result set
// rather than once per row. Pass nil for the un-memoized one-shot behavior.
func ScanCellCached(col arrow.Array, row int, loc *time.Location, keys *StructKeyCache) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch c := col.(type) {
	case *array.Null:
		return nil, nil
	case *array.Boolean:
		return c.Value(row), nil
	case *array.Int8:
		// Return the native width (int8/16/32), NOT a widened int64, to match the
		// Thrift path — its columnValuesTyped[*array.Int8, int8] returns a raw int8,
		// so a top-level TINYINT scanned into `any` is int8 on both backends.
		// (database/sql's convertAssign still coerces these into a typed *int64.)
		// NOTE: the driver.Value spec names only int64 for integers, so both
		// backends are technically off-spec here; unifying on int64 across Thrift +
		// kernel is a deliberate driver-wide follow-up (needs maintainer sign-off,
		// as it changes the prod Thrift path's observable type) — matching Thrift is
		// the correct choice until then, so the two backends stay identical.
		return c.Value(row), nil
	case *array.Int16:
		return c.Value(row), nil
	case *array.Int32:
		return c.Value(row), nil
	case *array.Int64:
		return c.Value(row), nil
	case *array.Uint8:
		return int64(c.Value(row)), nil
	case *array.Uint16:
		return int64(c.Value(row)), nil
	case *array.Uint32:
		return int64(c.Value(row)), nil
	case *array.Uint64:
		// Databricks SQL has no unsigned types, so a Uint64 column does not occur
		// in practice; this arm is defensive. driver.Value has no uint64 and the
		// driver convention is int64 for integers, so a value above MaxInt64 would
		// wrap — acceptable for an unreachable path.
		return int64(c.Value(row)), nil // #nosec G115 -- see above; unreachable for Databricks types
	case *array.Float32:
		// Return the native float32, NOT a widened float64: the Thrift path returns
		// a float32 driver.Value for a bare FLOAT column, and database/sql's
		// asString formats it at bit-size 32 — so widening here would render
		// CAST(0.1 AS FLOAT) as "0.10000000149011612" vs Thrift's "0.1".
		return c.Value(row), nil
	case *array.Float64:
		return c.Value(row), nil
	case *array.String:
		return c.Value(row), nil
	case *array.LargeString:
		return c.Value(row), nil
	case *array.Binary:
		return c.Value(row), nil
	case *array.Date32:
		return inLocation(c.Value(row).ToTime(), loc), nil
	case *array.Date64:
		return inLocation(c.Value(row).ToTime(), loc), nil
	case *array.Timestamp:
		dt, ok := col.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("timestamp column has unexpected datatype %s", col.DataType())
		}
		// Honor the schema's declared unit. This is intentionally stricter than the
		// Thrift path (arrowRows hardcodes Timestamp_us): Databricks TIMESTAMP is
		// always microseconds, so both agree in practice — but if a non-µs TIMESTAMP
		// ever arrived, this renders the correct instant while Thrift would misread
		// it. The cross-backend parity test pins both sides to µs (matching the wire
		// reality), so it can't exercise a non-µs value; TestScanCellTimestampUnits
		// covers this arm's unit-correctness directly instead.
		return inLocation(c.Value(row).ToTime(dt.Unit), loc), nil
	case *array.Decimal128:
		dt := col.DataType().(*arrow.Decimal128Type)
		return decimalfmt.ExactString(c.Value(row), dt.Scale), nil
	case *array.Duration:
		// INTERVAL DAY TO SECOND arrives as an arrow duration. The kernel returns the
		// native arrow value, so we format it Go-side to the same "D HH:MM:SS.nnnnnnnnn"
		// string the Thrift path gets pre-formatted from the server (its native-interval
		// config is off in prod, so it never scans a duration array — hence there is no
		// shared renderer to reuse, and this stays kernel-side).
		dt := col.DataType().(*arrow.DurationType)
		return formatDayTimeInterval(int64(c.Value(row)), dt.Unit), nil
	case *array.MonthInterval:
		// INTERVAL YEAR TO MONTH arrives as a month count; Thrift's server string is
		// "years-months".
		return formatYearMonthInterval(int32(c.Value(row))), nil
	case *array.List, *array.LargeList, *array.FixedSizeList, *array.Map, *array.Struct:
		// Nested types (and VARIANT, which arrives as a nested value) render to a
		// JSON string matching the Thrift path.
		return renderJSONString(col, row, loc, keys)
	default:
		return nil, fmt.Errorf("scanning arrow type %s is not supported", col.DataType())
	}
}

// formatDayTimeInterval renders an arrow duration (in the given time unit) as the
// Thrift path's "D HH:MM:SS.nnnnnnnnn" — days, then zero-padded hours:minutes:seconds
// with 9 fractional digits, negated with a leading '-'.
func formatDayTimeInterval(v int64, unit arrow.TimeUnit) string {
	neg := v < 0
	// Derive every component from the SIGNED value (Go integer division/modulo
	// truncate toward zero, so each component simply carries v's sign), then take
	// the magnitude of each bounded component when formatting. We deliberately do
	// NOT negate the full magnitude up front (`v = -v`): at math.MinInt64 that
	// wraps back to a negative value, so the components would come out negative
	// *and* a '-' would be prepended — a doubly-negated garbage string — and
	// math.MinInt64 μs is a representable Spark day-time bound.
	//
	// We also must NOT scale the full magnitude up to nanoseconds first: Spark
	// day-time intervals run up to ~Long.MaxValue microseconds (~292 years), so
	// v*1e3 (or *1e6/*1e9) would overflow int64 and silently produce a wrong
	// string. Deriving seconds by dividing keeps every intermediate in range; only
	// the bounded sub-second remainder is scaled up.
	var secs, frac int64
	switch unit {
	case arrow.Second:
		secs = v
	case arrow.Millisecond:
		secs = v / 1e3
		frac = (v % 1e3) * 1e6
	case arrow.Microsecond:
		secs = v / 1e6
		frac = (v % 1e6) * 1e3
	default: // Nanosecond
		secs = v / 1e9
		frac = v % 1e9
	}
	days := secs / 86400
	rem := secs % 86400
	h := rem / 3600
	rem %= 3600
	m := rem / 60
	s := rem % 60
	sign := ""
	if neg {
		sign = "-"
	}
	// Every component is bounded well within int64 (days ≤ ~1.07e8, h < 24, m/s <
	// 60, frac < 1e9), so abs64 here can never hit the math.MinInt64 abs overflow.
	return fmt.Sprintf("%s%d %02d:%02d:%02d.%09d", sign, abs64(days), abs64(h), abs64(m), abs64(s), abs64(frac))
}

// formatYearMonthInterval renders a month count as the Thrift path's "years-months",
// negated with a leading '-'.
func formatYearMonthInterval(months int32) string {
	neg := months < 0
	// Widen to int64 BEFORE negating: negating math.MinInt32 as an int32 overflows
	// (wraps back negative → doubly-negated garbage), but math.MinInt32 fits in
	// int64 where the negation is exact. math.MinInt32 months is a representable
	// Spark year-month bound.
	m := int64(months)
	if neg {
		m = -m
	}
	y := m / 12
	mo := m % 12
	sign := ""
	if neg {
		sign = "-"
	}
	return fmt.Sprintf("%s%d-%d", sign, y, mo)
}

// abs64 returns the absolute value of x. It is only ever called on components
// already bounded well within int64 (see formatDayTimeInterval), so the classic
// abs(math.MinInt64) overflow can't arise; the trivial form is intentional.
func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// inLocation renders t in loc, matching the Thrift path's .In(location); a nil
// loc leaves the value in UTC (arrow's ToTime default).
func inLocation(t time.Time, loc *time.Location) time.Time {
	if loc == nil {
		return t
	}
	return t.In(loc)
}

// renderJSONString renders one nested cell as a JSON string (nil for a NULL
// cell). loc is applied to any timestamp/date leaves, matching the top-level
// scan and the Thrift path.
func renderJSONString(col arrow.Array, row int, loc *time.Location, keys *StructKeyCache) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	var b strings.Builder
	if err := writeJSON(&b, col, row, loc, keys); err != nil {
		return nil, err
	}
	return b.String(), nil
}

// writeJSON writes the JSON form of col[row] into b, recursing for nested types.
func writeJSON(b *strings.Builder, col arrow.Array, row int, loc *time.Location, keys *StructKeyCache) error {
	if col.IsNull(row) {
		b.WriteString("null")
		return nil
	}
	switch c := col.(type) {
	case *array.List:
		// Use the offset-aware ValueOffsets, NOT Offsets()[row]: arrow-go's
		// Offsets() returns the full un-sliced buffer, so a List with a non-zero
		// logical offset (a sliced array, or a List field of a struct — Struct.Field
		// re-slices preserving data.offset) must index offsets[row+data.offset].
		// ValueOffsets does that; this mirrors the Thrift path's ValueOffsets use.
		s, e := c.ValueOffsets(row)
		return writeListJSON(b, c.ListValues(), int(s), int(e), loc, keys)
	case *array.LargeList:
		// arrow-go's LargeList.ValueOffsets (unlike List's) does NOT add data.offset,
		// so add it by hand to stay offset-correct.
		off := c.Data().Offset()
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row+off]), int(c.Offsets()[row+off+1]), loc, keys)
	case *array.FixedSizeList:
		n := int(c.DataType().(*arrow.FixedSizeListType).Len())
		base := (row + c.Data().Offset()) * n
		return writeListJSON(b, c.ListValues(), base, base+n, loc, keys)
	case *array.Map:
		return writeMapJSON(b, c, row, loc, keys)
	case *array.Struct:
		return writeStructJSON(b, c, row, loc, keys)
	case *array.Decimal128:
		// Emit the exact scale-applied decimal as a raw JSON number literal, not a
		// float64 — a float64 would render DECIMAL(5,2) 19.99 as 19.990000000000002
		// and corrupt high-precision values. Matches the Thrift path's marshalScalar
		// → ValueString.
		b.WriteString(decimalfmt.ExactString(c.Value(row), col.DataType().(*arrow.Decimal128Type).Scale))
		return nil
	case *array.Float32:
		// Marshal the native float32, NOT a widened float64: json.Marshal(float32
		// (3.14)) is "3.14" but json.Marshal(float64(float32(3.14))) is
		// "3.140000104904175". The Thrift nested path marshals the native float32,
		// so widening here would break byte-parity for ARRAY/MAP/STRUCT<…FLOAT…>.
		return writeScalarJSON(b, c.Value(row))
	default:
		v, err := scalarForJSON(col, row, loc)
		if err != nil {
			return err
		}
		return writeScalarJSON(b, v)
	}
}

func writeListJSON(b *strings.Builder, values arrow.Array, start, end int, loc *time.Location, keys *StructKeyCache) error {
	b.WriteByte('[')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		if err := writeJSON(b, values, i, loc, keys); err != nil {
			return err
		}
	}
	b.WriteByte(']')
	return nil
}

func writeMapJSON(b *strings.Builder, m *array.Map, row int, loc *time.Location, keys *StructKeyCache) error {
	// Deferred (tracked): every entry key is written unconditionally in Arrow
	// order, so an Arrow-legal MAP with duplicate keys renders non-unique JSON
	// object keys (e.g. {"1":"a","1":"b"}). This mirrors the Thrift path exactly
	// (parity holds), so it is intentionally NOT changed here — deciding dedup-last
	// vs. error is a cross-backend contract change that belongs in its own PR so
	// both renderers stay byte-identical.
	//
	// Map embeds *List, so ValueOffsets is offset-aware (adds data.offset); use it
	// rather than Offsets()[row] for the same reason as writeJSON's List case.
	s, e := m.ValueOffsets(row)
	start, end := int(s), int(e)
	mapKeys := m.Keys()
	items := m.Items()
	b.WriteByte('{')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		// JSON object keys must be strings; stringify the key value.
		kv, err := scalarForJSON(mapKeys, i, loc)
		if err != nil {
			return err
		}
		writeJSONKey(b, kv)
		b.WriteByte(':')
		if err := writeJSON(b, items, i, loc, keys); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

func writeStructJSON(b *strings.Builder, s *array.Struct, row int, loc *time.Location, keys *StructKeyCache) error {
	// Field-name keys are constant across rows; keys.keyPrefixes memoizes them per
	// result set so this per-row/per-cell path doesn't re-marshal them (a nil cache
	// recomputes inline — correct, just not memoized).
	prefixes := keys.keyPrefixes(s.DataType().(*arrow.StructType))
	b.WriteByte('{')
	for f := 0; f < s.NumField(); f++ {
		if f > 0 {
			b.WriteByte(',')
		}
		b.WriteString(prefixes[f]) // pre-escaped `"name":`
		if err := writeJSON(b, s.Field(f), row, loc, keys); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

// writeJSONKey writes a value as a JSON object key (always a quoted string),
// matching the Thrift path's mapValueContainer: marshal the value the same way a
// leaf is marshaled (marshalScalar → json.Marshal, so a []byte key renders as
// base64 "YWJj", NOT fmt "%v" [97 98 99]), then quote-wrap the result if it is
// not already a JSON string (numbers/bools become "7"/"true" keys).
func writeJSONKey(b *strings.Builder, v any) {
	// A time.Time key renders as its quoted .String() (same special-case as
	// writeScalarJSON / the Thrift marshal()), not RFC3339.
	if t, ok := v.(time.Time); ok {
		kb, _ := json.Marshal(t.String())
		b.Write(kb)
		return
	}
	kb, err := json.Marshal(v)
	if err != nil {
		// Unmarshalable key value — fall back to its stringified form, quoted.
		kb, _ = json.Marshal(fmt.Sprintf("%v", v))
	}
	if len(kb) > 0 && kb[0] == '"' {
		b.Write(kb) // already a JSON string (string / []byte via marshal)
		return
	}
	b.WriteByte('"')
	b.Write(kb)
	b.WriteByte('"')
}

// writeScalarJSON writes a scalar leaf, mirroring the Thrift marshal(): a
// time.Time becomes a quoted .String(); everything else uses json.Marshal.
func writeScalarJSON(b *strings.Builder, v any) error {
	if v == nil {
		b.WriteString("null")
		return nil
	}
	if t, ok := v.(time.Time); ok {
		b.WriteByte('"')
		b.WriteString(t.String())
		b.WriteByte('"')
		return nil
	}
	vb, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b.Write(vb)
	return nil
}

// scalarForJSON returns the Go value used for a nested leaf that is not itself a
// container — today only a map key (values are written directly by writeJSON).
// It reuses ScanCell's scalar arm, so a decimal key renders via the exact-string
// path (writeJSONKey then quotes it), never a lossy float64.
func scalarForJSON(col arrow.Array, row int, loc *time.Location) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	return ScanCell(col, row, loc)
}
