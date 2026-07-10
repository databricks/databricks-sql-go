package thrift

import (
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// Test helpers exported for the dbsql package's tests. They are defined in a
// non-_test file because Go's test-only visibility would hide them from a
// different package; they take internal-only types and are not part of the
// package's real API surface.

// ParamsToSparkForTest exposes the neutral-param -> TSparkParameter mapping so
// the dbsql parameter tests can assert on the Thrift wire form end-to-end.
func ParamsToSparkForTest(params []backend.Param) []*cli_service.TSparkParameter {
	return toSparkParameters(params)
}

// NewForTest builds a Thrift Backend from a mock client, a canned session, and a
// config, bypassing New and OpenSession, so the dbsql connection tests can back a
// conn with an injected mock client. It populates the cached sessionID from the
// handle exactly as OpenSession would.
func NewForTest(cli cli_service.TCLIService, session *cli_service.TOpenSessionResp, cfg *config.Config) *Backend {
	b := &Backend{cfg: cfg, client: cli, session: session}
	if session != nil && session.SessionHandle != nil {
		b.sessionID = client.SprintGuid(session.SessionHandle.GetSessionId().GUID)
	}
	return b
}

// OperationForTest builds an Operation wrapping a canned execute response and
// status, using this backend for any follow-up RPCs (e.g. the
// GetResultSetMetadata that IsStaging may issue).
func (b *Backend) OperationForTest(exStmtResp *cli_service.TExecuteStatementResp, opStatusResp *cli_service.TGetOperationStatusResp) *thriftOperation {
	return &thriftOperation{backend: b, exStmtResp: exStmtResp, opStatusResp: opStatusResp}
}
