package dbsql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPinger(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests")
	}

	conn := open(t)
	defer conn.Close()
	err := conn.Ping()
	if err != nil {
		t.Errorf("Failed to hit database")
	}
}

func TestSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests")
	}

	sampletime, _ := time.Parse(time.RFC3339, "2019-01-01T12:00:00Z")

	tests := []struct {
		sql string
		res interface{}
	}{
		{sql: "1", res: int32(1)},
		{sql: "cast(1 as smallint)", res: int16(1)},
		{sql: "cast(1 as int)", res: int32(1)},
		{sql: "cast(1 as bigint)", res: int64(1)},
		{sql: "cast(1.0 as float)", res: float64(1)},
		{sql: "cast(1.0 as double)", res: float64(1)},
		{sql: "cast(1.0 as real)", res: float64(1)},
		{sql: "'str'", res: "str"},
		{sql: "cast('2019-01-01 12:00:00' as timestamp)", res: sampletime},
	}

	var res interface{}
	db := open(t)
	defer db.Close()

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			err := db.QueryRow(fmt.Sprintf("select %s", tt.sql)).Scan(&res)
			if err != nil {
				t.Error(err)
			}

			if res != tt.res {
				t.Errorf("got: %v (%T), want: %v (%T)", res, res, tt.res, tt.res)
			}
		})
	}
}

func TestExhaustCursor(t *testing.T) {
	// GIVEN: Session MaxRows < expected result length
	// WHEN: Caller fetches all results
	// THEN: The expected result length is pulled by successive calls to

	// MaxRows is defined in databricks.go as 10_000

	db := open(t)
	defer db.Close()

	// Pull more results than the known MaxRows value
	ctx := context.Background()
	rows, _ := db.QueryContext(ctx, "SELECT id FROM RANGE(100000)")

	rowIds := make([]int, 0)

	// Fetch all results from the `rows` iterator
	var rowId int
	for rows.Next() {
		rows.Scan(&rowId)
		rowIds = append(rowIds, rowId)
	}

	expectedLength := 100_000
	actualLength := len(rowIds)

	if expectedLength != actualLength {
		t.Errorf("Result length mismatch. Expected %d, Actual: %d", expectedLength, actualLength)

	}
}

func open(t *testing.T) *sql.DB {
	dsn := os.Getenv("DATABRICKS_DSN")
	if dsn == "" {
		t.Skip("No connection string")
	}
	db, err := sql.Open("databricks", dsn)
	if err != nil {
		t.Fatalf("Could not connect to %s: %s", dsn, err)
	}
	return db
}
