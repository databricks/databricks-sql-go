package telemetry

import (
	"encoding/json"
	"time"
)

// TelemetryRequest is the top-level request sent to the telemetry endpoint.
type TelemetryRequest struct {
	UploadTime int64    `json:"uploadTime"`
	Items      []string `json:"items"`
	ProtoLogs  []string `json:"protoLogs"`
}

// TelemetryFrontendLog represents a single telemetry log entry.
type TelemetryFrontendLog struct {
	WorkspaceID         int64               `json:"workspace_id,omitempty"`
	FrontendLogEventID  string              `json:"frontend_log_event_id,omitempty"`
	Context             *FrontendLogContext `json:"context,omitempty"`
	Entry               *FrontendLogEntry   `json:"entry,omitempty"`
}

// FrontendLogContext contains the client context.
type FrontendLogContext struct {
	ClientContext *TelemetryClientContext `json:"client_context,omitempty"`
}

// TelemetryClientContext contains client-level information.
type TelemetryClientContext struct {
	ClientType    string `json:"client_type,omitempty"`
	ClientVersion string `json:"client_version,omitempty"`
}

// FrontendLogEntry contains the actual telemetry event.
type FrontendLogEntry struct {
	SQLDriverLog *TelemetryEvent `json:"sql_driver_log,omitempty"`
}

// TelemetryEvent contains the telemetry data for a SQL operation.
type TelemetryEvent struct {
	SessionID                  string                      `json:"session_id,omitempty"`
	SQLStatementID             string                      `json:"sql_statement_id,omitempty"`
	SystemConfiguration        *DriverSystemConfiguration  `json:"system_configuration,omitempty"`
	DriverConnectionParameters *DriverConnectionParameters `json:"driver_connection_params,omitempty"`
	AuthType                   string                      `json:"auth_type,omitempty"`
	SQLOperation               *SQLExecutionEvent          `json:"sql_operation,omitempty"`
	ErrorInfo                  *DriverErrorInfo            `json:"error_info,omitempty"`
	OperationLatencyMs         int64                       `json:"operation_latency_ms,omitempty"`
}

// DriverSystemConfiguration contains system-level configuration.
type DriverSystemConfiguration struct {
	OSName          string `json:"os_name,omitempty"`
	OSVersion       string `json:"os_version,omitempty"`
	OSArch          string `json:"os_arch,omitempty"`
	DriverName      string `json:"driver_name,omitempty"`
	DriverVersion   string `json:"driver_version,omitempty"`
	RuntimeName     string `json:"runtime_name,omitempty"`
	RuntimeVersion  string `json:"runtime_version,omitempty"`
	RuntimeVendor   string `json:"runtime_vendor,omitempty"`
	ClientAppName   string `json:"client_app_name,omitempty"`
	LocaleName      string `json:"locale_name,omitempty"`
	CharSetEncoding string `json:"char_set_encoding,omitempty"`
	ProcessName     string `json:"process_name,omitempty"`
}

// DriverConnectionParameters contains connection parameters.
type DriverConnectionParameters struct {
	Host                     string `json:"host,omitempty"`
	Port                     int    `json:"port,omitempty"`
	HTTPPath                 string `json:"http_path,omitempty"`
	EnableArrow              bool   `json:"enable_arrow,omitempty"`
	EnableDirectResults      bool   `json:"enable_direct_results,omitempty"`
	EnableMetricViewMetadata bool   `json:"enable_metric_view_metadata,omitempty"`
	SocketTimeoutSeconds     int64  `json:"socket_timeout,omitempty"`
	RowsFetchedPerBlock      int64  `json:"rows_fetched_per_block,omitempty"`
}

// SQLExecutionEvent contains SQL execution details.
type SQLExecutionEvent struct {
	ResultFormat      string           `json:"result_format,omitempty"`
	ChunkCount        int              `json:"chunk_count,omitempty"`
	BytesDownloaded   int64            `json:"bytes_downloaded,omitempty"`
	PollCount         int              `json:"poll_count,omitempty"`
	OperationDetail   *OperationDetail `json:"operation_detail,omitempty"`
}

// OperationDetail contains operation-specific details.
type OperationDetail struct {
	OperationType              string `json:"operation_type,omitempty"`
	NOperationStatusCalls      int64  `json:"n_operation_status_calls,omitempty"`
	OperationStatusLatencyMs   int64  `json:"operation_status_latency_millis,omitempty"`
	IsInternalCall             bool   `json:"is_internal_call,omitempty"`
}

// DriverErrorInfo contains error information.
type DriverErrorInfo struct {
	ErrorType    string `json:"error_type,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// TelemetryResponse is the response from the telemetry endpoint.
type TelemetryResponse struct {
	Errors             []string `json:"errors"`
	NumSuccess         int      `json:"numSuccess"`
	NumProtoSuccess    int      `json:"numProtoSuccess"`
	NumRealtimeSuccess int      `json:"numRealtimeSuccess"`
}

// createTelemetryRequest creates a telemetry request from metrics.
func createTelemetryRequest(metrics []*telemetryMetric, driverVersion string, connParams *DriverConnectionParameters) (*TelemetryRequest, error) {
	protoLogs := make([]string, 0, len(metrics))

	for _, metric := range metrics {
		frontendLog := &TelemetryFrontendLog{
			WorkspaceID:        0, // Will be populated if available
			FrontendLogEventID: generateEventID(),
			Context: &FrontendLogContext{
				ClientContext: &TelemetryClientContext{
					ClientType:    "golang",
					ClientVersion: driverVersion,
				},
			},
			Entry: &FrontendLogEntry{
				SQLDriverLog: &TelemetryEvent{
					SessionID:                  metric.sessionID,
					SQLStatementID:             metric.statementID,
					SystemConfiguration:        getSystemConfiguration(driverVersion),
					DriverConnectionParameters: connParams,
					OperationLatencyMs:         metric.latencyMs,
				},
			},
		}

		// Add SQL operation details if available
		if tags := metric.tags; tags != nil {
			sqlOp := &SQLExecutionEvent{}
			if v, ok := tags["result.format"].(string); ok {
				sqlOp.ResultFormat = v
			}
			if v, ok := tags["chunk_count"].(int); ok {
				sqlOp.ChunkCount = v
			}
			if v, ok := tags["bytes_downloaded"].(int64); ok {
				sqlOp.BytesDownloaded = v
			}
			if v, ok := tags["poll_count"].(int); ok {
				sqlOp.PollCount = v
			}

			// Add operation detail if operation_type is present
			if opType, ok := tags["operation_type"].(string); ok {
				sqlOp.OperationDetail = &OperationDetail{
					OperationType: opType,
				}
			}

			frontendLog.Entry.SQLDriverLog.SQLOperation = sqlOp
		}

		// Add error info if present
		if metric.errorType != "" {
			frontendLog.Entry.SQLDriverLog.ErrorInfo = &DriverErrorInfo{
				ErrorType: metric.errorType,
			}
		}

		// Marshal to JSON string (not base64 encoded)
		jsonBytes, err := json.Marshal(frontendLog)
		if err != nil {
			return nil, err
		}
		protoLogs = append(protoLogs, string(jsonBytes))
	}

	return &TelemetryRequest{
		UploadTime: time.Now().UnixMilli(),
		Items:      []string{}, // Required but empty
		ProtoLogs:  protoLogs,
	}, nil
}

// generateEventID generates a unique event ID.
func generateEventID() string {
	return time.Now().Format("20060102150405") + "-" + randomString(8)
}

// randomString generates a random alphanumeric string.
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}
