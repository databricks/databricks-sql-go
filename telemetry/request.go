package telemetry

import (
	"crypto/rand"
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
	WorkspaceID        int64               `json:"workspace_id,omitempty"`
	FrontendLogEventID string              `json:"frontend_log_event_id,omitempty"`
	Context            *FrontendLogContext `json:"context,omitempty"`
	Entry              *FrontendLogEntry   `json:"entry,omitempty"`
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

// TelemetryEvent maps to OssSqlDriverTelemetryLog in the proto schema.
type TelemetryEvent struct {
	SessionID                  string                      `json:"session_id,omitempty"`
	SQLStatementID             string                      `json:"sql_statement_id,omitempty"`
	SystemConfiguration        *DriverSystemConfiguration  `json:"system_configuration,omitempty"`
	DriverConnectionParameters *DriverConnectionParameters `json:"driver_connection_params,omitempty"`
	AuthType                   string                      `json:"auth_type,omitempty"`
	VolumeOperation            *VolumeOperationEvent       `json:"vol_operation,omitempty"`
	SQLOperation               *SQLExecutionEvent          `json:"sql_operation,omitempty"`
	ErrorInfo                  *DriverErrorInfo            `json:"error_info,omitempty"`
	OperationLatencyMs         int64                       `json:"operation_latency_ms,omitempty"`
}

// DriverSystemConfiguration maps to DriverSystemConfiguration in the proto schema.
type DriverSystemConfiguration struct {
	DriverVersion   string `json:"driver_version,omitempty"`
	RuntimeName     string `json:"runtime_name,omitempty"`
	RuntimeVersion  string `json:"runtime_version,omitempty"`
	RuntimeVendor   string `json:"runtime_vendor,omitempty"`
	OSName          string `json:"os_name,omitempty"`
	OSVersion       string `json:"os_version,omitempty"`
	OSArch          string `json:"os_arch,omitempty"`
	DriverName      string `json:"driver_name,omitempty"`
	ClientAppName   string `json:"client_app_name,omitempty"`
	LocaleName      string `json:"locale_name,omitempty"`
	CharSetEncoding string `json:"char_set_encoding,omitempty"`
	ProcessName     string `json:"process_name,omitempty"`
}

// HostDetails maps to HostDetails in the proto schema.
type HostDetails struct {
	HostURL       string `json:"host_url,omitempty"`
	Port          int32  `json:"port,omitempty"`
	ProxyAuthType string `json:"proxy_auth_type,omitempty"`
}

// DriverConnectionParameters maps to DriverConnectionParameters in the proto schema.
// Only fields populated by the Go driver are included; others are omitted.
type DriverConnectionParameters struct {
	HTTPPath              string       `json:"http_path,omitempty"`
	Mode                  string       `json:"mode,omitempty"`
	HostInfo              *HostDetails `json:"host_info,omitempty"`
	UseProxy              bool         `json:"use_proxy,omitempty"`
	AuthMech              string       `json:"auth_mech,omitempty"`
	AuthFlow              string       `json:"auth_flow,omitempty"`
	AuthScope             string       `json:"auth_scope,omitempty"`
	UseSystemProxy        bool         `json:"use_system_proxy,omitempty"`
	UseCFProxy            bool         `json:"use_cf_proxy,omitempty"`
	EnableArrow           bool         `json:"enable_arrow,omitempty"`
	EnableDirectResults   bool         `json:"enable_direct_results,omitempty"`
	QueryTags             string       `json:"query_tags,omitempty"`
	EnableMetricViewMeta  bool         `json:"enable_metric_view_metadata,omitempty"`
	SocketTimeout         int64        `json:"socket_timeout,omitempty"`
}

// SQLExecutionEvent maps to SqlExecutionEvent in the proto schema.
type SQLExecutionEvent struct {
	StatementType        string           `json:"statement_type,omitempty"`
	IsCompressed         bool             `json:"is_compressed,omitempty"`
	ExecutionResult      string           `json:"execution_result,omitempty"`
	ChunkID              int64            `json:"chunk_id,omitempty"`
	RetryCount           int64            `json:"retry_count,omitempty"`
	ChunkDetails         *ChunkDetails    `json:"chunk_details,omitempty"`
	ResultLatency        *ResultLatency   `json:"result_latency,omitempty"`
	OperationDetail      *OperationDetail `json:"operation_detail,omitempty"`
	JavaUsesPatchedArrow bool             `json:"java_uses_patched_arrow,omitempty"`
}

// ChunkDetails maps to ChunkDetails in the proto schema.
type ChunkDetails struct {
	InitialChunkLatencyMs    int64 `json:"initial_chunk_latency_millis,omitempty"`
	SlowestChunkLatencyMs    int64 `json:"slowest_chunk_latency_millis,omitempty"`
	TotalChunksPresent       int32 `json:"total_chunks_present,omitempty"`
	TotalChunksIterated      int32 `json:"total_chunks_iterated,omitempty"`
	SumChunksDownloadTimeMs  int64 `json:"sum_chunks_download_time_millis,omitempty"`
}

// ResultLatency maps to ResultLatency in the proto schema.
type ResultLatency struct {
	ResultSetReadyLatencyMs       int64 `json:"result_set_ready_latency_millis,omitempty"`
	ResultSetConsumptionLatencyMs int64 `json:"result_set_consumption_latency_millis,omitempty"`
}

// OperationDetail maps to OperationDetail in the proto schema.
type OperationDetail struct {
	NOperationStatusCalls    int32  `json:"n_operation_status_calls,omitempty"`
	OperationStatusLatencyMs int64  `json:"operation_status_latency_millis,omitempty"`
	OperationType            string `json:"operation_type,omitempty"`
	IsInternalCall           bool   `json:"is_internal_call,omitempty"`
}

// VolumeOperationEvent maps to VolumeOperationEvent in the proto schema.
type VolumeOperationEvent struct {
	VolumeOperationType string `json:"volume_operation_type,omitempty"`
	VolumePath          string `json:"volume_path,omitempty"`
	LocalFile           string `json:"local_file,omitempty"`
}

// DriverErrorInfo maps to DriverErrorInfo in the proto schema.
type DriverErrorInfo struct {
	ErrorName  string `json:"error_name,omitempty"`
	StackTrace string `json:"stack_trace,omitempty"`
}

// TelemetryResponse is the response from the telemetry endpoint.
type TelemetryResponse struct {
	Errors             []string `json:"errors"`
	NumSuccess         int      `json:"numSuccess"`
	NumProtoSuccess    int      `json:"numProtoSuccess"`
	NumRealtimeSuccess int      `json:"numRealtimeSuccess"`
}

// createTelemetryRequest creates a telemetry request from metrics.
func createTelemetryRequest(metrics []*telemetryMetric, driverVersion string) (*TelemetryRequest, error) {
	protoLogs := make([]string, 0, len(metrics))

	for _, metric := range metrics {
		frontendLog := &TelemetryFrontendLog{
			FrontendLogEventID: generateEventID(),
			Context: &FrontendLogContext{
				ClientContext: &TelemetryClientContext{
					ClientType:    "golang",
					ClientVersion: driverVersion,
				},
			},
			Entry: &FrontendLogEntry{
				SQLDriverLog: &TelemetryEvent{
					SessionID:           metric.sessionID,
					SQLStatementID:      metric.statementID,
					SystemConfiguration: getSystemConfiguration(driverVersion),
					OperationLatencyMs:  metric.latencyMs,
				},
			},
		}

		// Add SQL operation details if available.
		if tags := metric.tags; tags != nil {
			sqlOp := &SQLExecutionEvent{}

			if v, ok := tags["result.format"].(string); ok {
				sqlOp.ExecutionResult = v
			}
			if chunkCount, ok := tags["chunk_count"].(int); ok && chunkCount > 0 {
				sqlOp.ChunkDetails = &ChunkDetails{
					TotalChunksIterated: int32(chunkCount),
				}
			}

			if opType, ok := tags["operation_type"].(string); ok {
				detail := &OperationDetail{
					OperationType: opType,
				}
				if pollCount, ok := tags["poll_count"].(int); ok {
					detail.NOperationStatusCalls = int32(pollCount)
				}
				sqlOp.OperationDetail = detail
			}

			frontendLog.Entry.SQLDriverLog.SQLOperation = sqlOp
		}

		// Add error info if present.
		if metric.errorType != "" {
			frontendLog.Entry.SQLDriverLog.ErrorInfo = &DriverErrorInfo{
				ErrorName: metric.errorType,
			}
		}

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

// randomString generates a random alphanumeric string using crypto/rand.
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	_, _ = rand.Read(b)
	for i, v := range b {
		b[i] = charset[int(v)%len(charset)]
	}
	return string(b)
}
