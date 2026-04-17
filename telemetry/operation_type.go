package telemetry

const (
	OperationTypeUnspecified      = "TYPE_UNSPECIFIED"
	OperationTypeCreateSession    = "CREATE_SESSION"
	OperationTypeDeleteSession    = "DELETE_SESSION"
	OperationTypeExecuteStatement = "EXECUTE_STATEMENT"
	OperationTypeCloseStatement   = "CLOSE_STATEMENT"
)

// isTerminalOperationType returns true for operations that signal the end of a
// session or statement lifecycle. These flush immediately so metrics are not
// lost if the connection closes before the next periodic flush — matching JDBC.
func isTerminalOperationType(opType string) bool {
	return opType == OperationTypeDeleteSession || opType == OperationTypeCloseStatement
}
