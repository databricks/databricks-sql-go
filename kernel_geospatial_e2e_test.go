//go:build cgo && databricks_kernel

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/array"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

// Runs unchanged on the DBSQL and Reyden legs. A server rejection is a failure,
// not a stale geospatial skip. The kernel archive must include the geospatial PR.
func TestKernelE2EGeospatialRepresentations(t *testing.T) {
	const query = `SELECT st_geomfromtext('POINT(1 2)',4326) AS geom,
		st_geogfromtext('POINT(1 2)') AS geog, st_geomfromtext(NULL,4326) AS missing,
		named_struct('g',st_geomfromtext('POINT(1 2)',4326),
		'blob',unhex('0001027F80FF'),'n',st_geomfromtext(NULL,4326)) AS nested`
	wkb, _ := hex.DecodeString("0101000000000000000000f03f0000000000000040")
	ewkb, _ := hex.DecodeString("0101000020e6100000000000000000f03f0000000000000040")
	blob, _ := hex.DecodeString("0001027F80FF")
	const text = "SRID=4326;POINT(1 2)"
	for _, mode := range []struct {
		name   string
		native bool
		extra  []ConnOption
	}{
		{"default", false, nil},
		{"enabled", true, []ConnOption{WithEnableGeoSpatialSupport(true)}},
		{"disabled", false, []ConnOption{WithEnableGeoSpatialSupport(false)}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			db := kernelTestDBWith(t, append(mode.extra, WithSessionParams(map[string]string{"timezone": "UTC"}))...)
			defer db.Close()
			rows, err := db.QueryContext(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			if err != nil {
				t.Fatal(err)
			}
			for i, family := range []string{"GEOMETRY", "GEOGRAPHY"} {
				wantType, wantScan := "STRING", reflect.TypeOf("")
				if mode.native {
					wantType, wantScan = family, reflect.TypeOf(sql.RawBytes{})
				}
				if columns[i].DatabaseTypeName() != wantType || columns[i].ScanType() != wantScan {
					t.Fatalf("column %d: type=%s scan=%v, want %s/%v", i, columns[i].DatabaseTypeName(), columns[i].ScanType(), wantType, wantScan)
				}
			}
			if !rows.Next() {
				t.Fatalf("missing row: %v", rows.Err())
			}
			var geom, geog, missing any
			var nested string
			if err := rows.Scan(&geom, &geog, &missing, &nested); err != nil {
				t.Fatal(err)
			}
			for _, value := range []any{geom, geog} {
				if mode.native {
					if data, ok := value.([]byte); !ok || !bytes.Equal(data, ewkb) {
						t.Fatalf("got %T %v, want EWKB %x", value, value, ewkb)
					}
				} else if value != text {
					t.Fatalf("got %T %v, want EWKT", value, value)
				}
			}
			if missing != nil {
				t.Fatalf("SQL NULL = %#v", missing)
			}
			var gotNested map[string]any
			if err := json.Unmarshal([]byte(nested), &gotNested); err != nil {
				t.Fatalf("invalid public JSON %q: %v", nested, err)
			}
			var geoLeaf any = text
			if mode.native {
				geoLeaf = map[string]any{"srid": float64(4326), "wkb": base64.StdEncoding.EncodeToString(wkb)}
			}
			wantNested := map[string]any{"g": geoLeaf, "blob": base64.StdEncoding.EncodeToString(blob), "n": nil}
			if !reflect.DeepEqual(gotNested, wantNested) {
				t.Fatalf("nested = %s, want %v", nested, wantNested)
			}
			if rows.Next() || rows.Err() != nil {
				t.Fatalf("unexpected trailing row/error: %v", rows.Err())
			}
			rows.Close()

			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var raw driver.Rows
			if err := conn.Raw(func(c any) error {
				var err error
				raw, err = c.(driver.QueryerContext).QueryContext(context.Background(), query, nil)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			batches, err := raw.(dbsqlrows.Rows).GetArrowBatches(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer batches.Close()
			var count int64
			for batches.HasNext() {
				record, err := batches.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				func() {
					defer record.Release()
					for row := 0; row < int(record.NumRows()); row++ {
						for col := 0; col < 2; col++ {
							if mode.native {
								value, ok := record.Column(col).(*array.Struct)
								if !ok {
									t.Fatalf("raw geo type = %s, want struct<srid,wkb>", record.Column(col).DataType())
								}
								if value.Field(0).(*array.Int32).Value(row) != 4326 || !bytes.Equal(value.Field(1).(*array.Binary).Value(row), wkb) {
									t.Fatal("raw Arrow SRID/WKB changed")
								}
							} else if value, ok := record.Column(col).(*array.String); !ok || value.Value(row) != text {
								t.Fatalf("raw text = %v", record.Column(col))
							}
						}
						if !record.Column(2).IsNull(row) {
							t.Fatal("raw Arrow NULL was lost")
						}
						value := record.Column(3).(*array.Struct)
						if !bytes.Equal(value.Field(1).(*array.Binary).Value(row), blob) || !value.Field(2).IsNull(row) {
							t.Fatal("raw Arrow binary/null siblings changed")
						}
					}
					count += record.NumRows()
				}()
			}
			if count != 1 {
				t.Fatalf("raw Arrow row count = %d, want 1", count)
			}
		})
	}
}
