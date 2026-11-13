package arrowscan

import (
	"encoding/json"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
)

// sparkJSONTypeMetadataKey is the Arrow field-metadata key carrying a column's
// Spark JSON data type (the SEA manifest's type_json, copied by the kernel).
const sparkJSONTypeMetadataKey = "Spark:DataType:JsonType"

// WithNestedTimestampTypes returns schema with every nested timestamp child
// labelled TIMESTAMP_NTZ or TIMESTAMP (databricks.type_name), read from its
// top-level column's Spark:DataType:JsonType. The kernel attaches logical-type
// metadata to top-level fields only and gives a nested NTZ child the physical
// zone "UTC", so without the label a nested NTZ wall clock would be shifted
// into the session location (databricks/databricks-sql-go#520). Columns whose
// JSON type is missing, malformed or shaped differently from their timestamp
// subtrees are left unchanged; schema itself is returned when nothing changes.
func WithNestedTimestampTypes(schema *arrow.Schema) *arrow.Schema {
	var fields []arrow.Field
	for i, f := range schema.Fields() {
		labelled, ok := withNestedTimestampTypes(f)
		if !ok {
			continue
		}
		if fields == nil {
			fields = append([]arrow.Field(nil), schema.Fields()...)
		}
		fields[i] = labelled
	}
	if fields == nil {
		return schema
	}
	md := schema.Metadata()
	return arrow.NewSchema(fields, &md)
}

func withNestedTimestampTypes(f arrow.Field) (arrow.Field, bool) {
	if !isNested(f.Type) || !hasTimestamp(f.Type) {
		return f, false
	}
	raw, ok := f.Metadata.GetValue(sparkJSONTypeMetadataKey)
	if !ok {
		return f, false
	}
	st, err := parseSparkType(json.RawMessage(raw))
	if err != nil {
		return f, false
	}
	t, changed, ok := labelType(f.Type, st)
	if !changed || !ok {
		return f, false
	}
	f.Type = t
	return f, true
}

func isNested(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.MAP, arrow.STRUCT:
		return true
	}
	return false
}

func hasTimestamp(dt arrow.DataType) bool {
	switch t := dt.(type) {
	case *arrow.TimestampType:
		return true
	case arrow.NestedType:
		for _, c := range t.Fields() {
			if hasTimestamp(c.Type) {
				return true
			}
		}
	}
	return false
}

// sparkType is the subset of a Spark JSON data type needed to find timestamp
// leaves: a lower-case type name and, for array/map/struct, its children
// (element; key, value; fields in order).
type sparkType struct {
	name     string
	children []*sparkType
}

func parseSparkType(raw json.RawMessage) (*sparkType, error) {
	var name string
	if err := json.Unmarshal(raw, &name); err == nil {
		return &sparkType{name: strings.ToLower(name)}, nil
	}
	var obj struct {
		Type        string          `json:"type"`
		ElementType json.RawMessage `json:"elementType"`
		KeyType     json.RawMessage `json:"keyType"`
		ValueType   json.RawMessage `json:"valueType"`
		Fields      []struct {
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	st := &sparkType{name: strings.ToLower(obj.Type)}
	var parts []json.RawMessage
	switch st.name {
	case "array":
		parts = []json.RawMessage{obj.ElementType}
	case "map":
		parts = []json.RawMessage{obj.KeyType, obj.ValueType}
	case "struct":
		for _, f := range obj.Fields {
			parts = append(parts, f.Type)
		}
	}
	for _, p := range parts {
		c, err := parseSparkType(p)
		if err != nil {
			return nil, err
		}
		st.children = append(st.children, c)
	}
	return st, nil
}

// labelField labels f's timestamp leaves from st. changed reports whether f
// changed; ok is false when a subtree holding a timestamp does not match st, so
// the caller keeps the whole column as is. Subtrees without timestamps are not
// compared. A timestamp that already carries a non-blank logical type keeps it.
func labelField(f arrow.Field, st *sparkType) (out arrow.Field, changed, ok bool) {
	if !hasTimestamp(f.Type) {
		return f, false, true
	}
	if _, isTimestamp := f.Type.(*arrow.TimestampType); !isTimestamp {
		t, changed, ok := labelType(f.Type, st)
		if changed {
			f.Type = t
		}
		return f, changed, ok
	}
	var name string
	switch st.name {
	case "timestamp_ntz":
		name = "TIMESTAMP_NTZ"
	case "timestamp":
		name = "TIMESTAMP"
	default:
		return f, false, false
	}
	for _, key := range [...]string{databricksTypeNameMetadataKey, sparkSQLNameMetadataKey} {
		if v, ok := f.Metadata.GetValue(key); ok && strings.TrimSpace(v) != "" {
			return f, false, true
		}
	}
	// Drop any blank type name: metadata lookups return the first match.
	keys := []string{databricksTypeNameMetadataKey}
	values := []string{name}
	for i, k := range f.Metadata.Keys() {
		if k != databricksTypeNameMetadataKey {
			keys = append(keys, k)
			values = append(values, f.Metadata.Values()[i])
		}
	}
	f.Metadata = arrow.NewMetadata(keys, values)
	return f, true, true
}

// labelType rebuilds a nested type with labelled children (see labelField for
// changed and ok). Only field metadata changes, so the physical layout is
// identical to dt.
func labelType(dt arrow.DataType, st *sparkType) (out arrow.DataType, changed, ok bool) {
	switch t := dt.(type) {
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType:
		if st.name != "array" || len(st.children) != 1 {
			return dt, false, false
		}
		e, changed, ok := labelField(t.(interface{ ElemField() arrow.Field }).ElemField(), st.children[0])
		if !changed || !ok {
			return dt, false, ok
		}
		switch t := t.(type) {
		case *arrow.LargeListType:
			return arrow.LargeListOfField(e), true, true
		case *arrow.FixedSizeListType:
			return arrow.FixedSizeListOfField(t.Len(), e), true, true
		default:
			return arrow.ListOfField(e), true, true
		}
	case *arrow.MapType:
		if st.name != "map" || len(st.children) != 2 {
			return dt, false, false
		}
		k, keyChanged, keyOK := labelField(t.KeyField(), st.children[0])
		v, itemChanged, itemOK := labelField(t.ItemField(), st.children[1])
		if !keyOK || !itemOK || (!keyChanged && !itemChanged) {
			return dt, false, keyOK && itemOK
		}
		m := arrow.MapOfWithMetadata(k.Type, k.Metadata, v.Type, v.Metadata)
		m.KeysSorted = t.KeysSorted
		m.SetItemNullable(v.Nullable)
		return m, true, true
	case *arrow.StructType:
		if st.name != "struct" || len(st.children) != len(t.Fields()) {
			return dt, false, false
		}
		fields := make([]arrow.Field, len(t.Fields()))
		anyChanged := false
		for i := range fields {
			var changed, ok bool
			fields[i], changed, ok = labelField(t.Field(i), st.children[i])
			if !ok {
				return dt, false, false
			}
			anyChanged = anyChanged || changed
		}
		if !anyChanged {
			return dt, false, true
		}
		return arrow.StructOf(fields...), true, true
	}
	return dt, false, false
}
