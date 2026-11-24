//go:build cgo && databricks_kernel

package kernel

import (
	"fmt"
	"testing"
)

func TestEvictIfSessionFatalEvictsExpiredSession(t *testing.T) {
	k := &KernelBackend{valid: true}
	k.evictIfSessionFatal(fmt.Errorf("kernel: execute: %w", expiredSessionError()))
	if k.valid {
		t.Fatal("an expired session must mark the conn invalid so database/sql discards it")
	}
}

func TestEvictIfSessionFatalKeepsSessionOnOtherNotFound(t *testing.T) {
	k := &KernelBackend{valid: true}
	k.evictIfSessionFatal(&KernelError{Code: statusNotFound, Message: "[TABLE_OR_VIEW_NOT_FOUND] missing", SQLState: "42P01"})
	if !k.valid {
		t.Fatal("a missing table must not evict a usable session")
	}
}
