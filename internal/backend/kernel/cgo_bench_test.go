//go:build cgo && databricks_kernel

package kernel

import (
	"os"
	"testing"
)

// See cgo_bench.go for the drain helper. These benchmarks mirror
// BenchmarkDyn* (purego dynamic path) so ns/op is directly comparable.
//
//	DBX_KERNEL_HOST=... DBX_KERNEL_HTTPATH=... DBX_KERNEL_TOKEN=... \
//	CGO_ENABLED=1 go test -tags databricks_kernel -run x \
//	  -bench 'BenchmarkCgo' -benchtime 20x ./internal/backend/kernel/
func cgoBenchEnv(b *testing.B) (host, httpPath, token string) {
	host = os.Getenv("DBX_KERNEL_HOST")
	httpPath = os.Getenv("DBX_KERNEL_HTTPATH")
	token = os.Getenv("DBX_KERNEL_TOKEN")
	if host == "" || httpPath == "" || token == "" {
		b.Skip("set DBX_KERNEL_HOST, DBX_KERNEL_HTTPATH, DBX_KERNEL_TOKEN")
	}
	return
}

func BenchmarkCgoLowLatency(b *testing.B) {
	host, httpPath, token := cgoBenchEnv(b)
	s, err := CgoBenchOpen(host, httpPath, token)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Drain("SELECT 1 AS one"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCgoLargeResult(b *testing.B) {
	host, httpPath, token := cgoBenchEnv(b)
	s, err := CgoBenchOpen(host, httpPath, token)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := s.Drain("SELECT id, id*2 AS doubled, CAST(id AS STRING) AS s FROM range(0, 500000)")
		if err != nil {
			b.Fatal(err)
		}
		if n != 500000 {
			b.Fatalf("got %d rows", n)
		}
	}
}
