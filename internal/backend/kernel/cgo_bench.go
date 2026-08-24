//go:build cgo && databricks_kernel

// Benchmark helper for the cgo static-link path, used by cgo_bench_test.go to
// compare against the purego dynamic path (dynamic_bench_test.go). It lives in a
// non-test file because cgo (`import "C"`) is not supported directly in _test.go
// files. It is only referenced from benchmarks, so it adds nothing to a normal
// build beyond the already-tagged kernel package.
package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"
struct ArrowSchema;
struct ArrowArray;
*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/cdata"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

// CgoBenchSession is an open kernel session over the cgo static-link path.
type CgoBenchSession struct {
	session *C.kernel_session_t
}

// CgoBenchOpen opens a session via the cgo path (PAT auth over http path).
func CgoBenchOpen(host, httpPath, token string) (*CgoBenchSession, error) {
	initKernelLogging()
	var cfg *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&cfg) }); err != nil {
		return nil, err
	}
	ch := newCStr(host)
	defer ch.free()
	cp := newCStr(httpPath)
	defer cp.free()
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_set_http_path(cfg, ch.c, cp.c) }); err != nil {
		return nil, err
	}
	ct := newCStr(token)
	defer ct.free()
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_set_auth_pat(cfg, ct.c) }); err != nil {
		return nil, err
	}
	var sess *C.kernel_session_t
	if err := call(func() C.KernelStatusCode { return C.kernel_session_open(cfg, &sess) }); err != nil {
		return nil, err
	}
	return &CgoBenchSession{session: sess}, nil
}

func (s *CgoBenchSession) Close() {
	if s.session != nil {
		_ = call(func() C.KernelStatusCode { return C.kernel_session_close(s.session) })
		s.session = nil
	}
}

// Drain runs sql and scans every row through the same arrowscan scanner the
// dynamic path uses, returning the row count. Mirrors dynSession.drain.
func (s *CgoBenchSession) Drain(sql string) (int, error) {
	var stmt *C.kernel_statement_t
	if err := call(func() C.KernelStatusCode { return C.kernel_session_new_statement(s.session, &stmt) }); err != nil {
		return 0, err
	}
	defer C.kernel_statement_close(stmt)
	cs := newCStr(sql)
	defer cs.free()
	if err := call(func() C.KernelStatusCode { return C.kernel_statement_set_sql(stmt, cs.c) }); err != nil {
		return 0, err
	}
	var exec *C.kernel_executed_statement_t
	if err := call(func() C.KernelStatusCode { return C.kernel_statement_execute(stmt, &exec) }); err != nil {
		return 0, err
	}
	defer C.kernel_executed_statement_close(exec)
	var stream *C.kernel_result_stream_t
	if err := call(func() C.KernelStatusCode { return C.kernel_executed_statement_get_result_stream(exec, &stream) }); err != nil {
		return 0, err
	}
	defer C.kernel_result_stream_close(stream)

	var csch C.struct_ArrowSchema
	if err := call(func() C.KernelStatusCode { return C.kernel_result_stream_get_schema(stream, &csch) }); err != nil {
		return 0, err
	}
	sch, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&csch)))
	if err != nil {
		return 0, err
	}
	keyCache := arrowscan.NewStructKeyCache()
	ncols := len(sch.Fields())
	dest := make([]driver.Value, ncols)

	var cur arrow.Record
	rowInCur, n := 0, 0
	for {
		for cur == nil || rowInCur >= int(cur.NumRows()) {
			if cur != nil {
				cur.Release()
				cur = nil
			}
			var carr C.struct_ArrowArray
			var cs2 C.struct_ArrowSchema
			if err := call(func() C.KernelStatusCode {
				return C.kernel_result_stream_next_batch(stream, &carr, &cs2)
			}); err != nil {
				return 0, err
			}
			if carr.release == nil {
				if cur != nil {
					cur.Release()
				}
				return n, nil
			}
			rec, err := cdata.ImportCRecordBatch(
				(*cdata.CArrowArray)(unsafe.Pointer(&carr)),
				(*cdata.CArrowSchema)(unsafe.Pointer(&cs2)))
			if err != nil {
				return 0, err
			}
			cur = rec
			rowInCur = 0
			keyCache.Reset()
		}
		for c := 0; c < ncols; c++ {
			if _, err := arrowscan.ScanCellCached(cur.Column(c), rowInCur, time.UTC, keyCache); err != nil {
				return 0, fmt.Errorf("scan col %d: %w", c, err)
			}
			_ = dest
		}
		rowInCur++
		n++
	}
}
