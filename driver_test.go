package dbsql

import (
	"io/ioutil"
	"testing"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		in  string
		out Options
	}{
		{
			"databricks://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a",
			Options{Host: "example.cloud.databricks.com", Port: "443", Token: "supersecret", HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a", MaxRows: 10000, Timeout: 0, LogOut: ioutil.Discard},
		},
		{
			"databricks://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a?timeout=123",
			Options{Host: "example.cloud.databricks.com", Port: "443", Token: "supersecret", HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a", Timeout: 123, MaxRows: 10000, LogOut: ioutil.Discard},
		},
		{
			"databricks://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a?timeout=123&maxRows=1000",
			Options{Host: "example.cloud.databricks.com", Port: "443", Token: "supersecret", HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a", Timeout: 123, MaxRows: 1000, LogOut: ioutil.Discard},
		},
		{
			"databricks://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a?maxRows=1000",
			Options{Host: "example.cloud.databricks.com", Port: "443", Token: "supersecret", HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a", MaxRows: 1000, Timeout: 0, LogOut: ioutil.Discard},
		},
		{
			"databricks://:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a?maxRows=1000",
			Options{Host: "example.cloud.databricks.com", Port: "443", Token: "supersecret", HTTPPath: "/sql/1.0/endpoints/12346a5b5b0e123a", MaxRows: 1000, Timeout: 0, LogOut: ioutil.Discard},
		},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			opts, err := parseURI(tt.in)
			if err != nil {
				t.Error(err)
				return
			}
			if *opts != tt.out {
				t.Errorf("got: %v, want: %v", opts, tt.out)
			}
		})
	}
}

func TestWrongParseURI(t *testing.T) {
	_, err := parseURI("databricks://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a?maxRows=fail")

	if err == nil {
		t.Errorf("should fail with misformmated maxRows")
	}

	_, err = parseURI("databricks://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a?timeout=fail")

	if err == nil {
		t.Errorf("should fail with misformmated timeout")
	}

	_, err = parseURI("https://token:supersecret@example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a")

	if err == nil {
		t.Errorf("should fail with wrong scheme")
	}

	_, err = parseURI("databricks://example.cloud.databricks.com/sql/1.0/endpoints/12346a5b5b0e123a")

	if err == nil {
		t.Errorf("should fail without token")
	}

	_, err = parseURI("databricks://token:supersecret@example.cloud.databricks.com")

	if err == nil {
		t.Errorf("should fail without path")
	}
}
