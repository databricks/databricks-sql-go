package main

import (
	"context"
	"database/sql"
	"fmt"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

type row struct {
	symbol           string
	companyName      string
	industry         string
	date             string
	open             float64
	high             float64
	low              float64
	close            float64
	volume           int
	change           float64
	changePercentage float64
	upTrend          bool
	volatile         bool
}

func runTest(withCloudFetch bool, query string) ([]row, error) {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		return nil, err
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithTimeout(10),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
		dbsql.WithEnableCloudFetch(withCloudFetch),
	)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	rows, err1 := db.QueryContext(context.Background(), query)
	defer rows.Close()

	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return nil, err
		} else {
			return nil, err
		}
	}
	var res []row
	for rows.Next() {
		r := row{}
		err := rows.Scan(&r.symbol, &r.companyName, &r.industry, &r.date, &r.open, &r.high, &r.low, &r.close, &r.volume, &r.change, &r.changePercentage, &r.upTrend, &r.volatile)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func TestCloudFetch(t *testing.T) {
	t.Run("Compare local batch to cloud fetch", func(t *testing.T) {
		query := "select * from stock_data where date is not null and volume is not null order by date, symbol limit 10000000"

		// Local arrow batch
		abRes, err := runTest(false, query)
		assert.NoError(t, err)

		// Cloud fetch batch
		cfRes, err := runTest(true, query)
		assert.NoError(t, err)

		for i := 0; i < len(abRes); i++ {
			assert.Equal(t, abRes[i], cfRes[i], fmt.Sprintf("not equal for row: %d", i))
		}
	})
}
