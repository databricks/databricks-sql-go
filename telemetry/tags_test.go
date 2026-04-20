package telemetry

import "testing"

func TestTagConstants_NonEmpty(t *testing.T) {
	// Verify all exported tag constants have non-empty values.
	tags := []struct {
		name  string
		value string
	}{
		{"TagWorkspaceID", TagWorkspaceID},
		{"TagSessionID", TagSessionID},
		{"TagDriverVersion", TagDriverVersion},
		{"TagStatementID", TagStatementID},
		{"TagResultFormat", TagResultFormat},
		{"TagChunkCount", TagChunkCount},
		{"TagBytesDownloaded", TagBytesDownloaded},
		{"TagOperationType", TagOperationType},
		{"TagPollCount", TagPollCount},
		{"TagChunkInitialLatencyMs", TagChunkInitialLatencyMs},
		{"TagChunkSlowestLatencyMs", TagChunkSlowestLatencyMs},
		{"TagChunkSumLatencyMs", TagChunkSumLatencyMs},
		{"TagChunkTotalPresent", TagChunkTotalPresent},
	}
	for _, tt := range tags {
		if tt.value == "" {
			t.Errorf("Tag constant %s must not be empty", tt.name)
		}
	}
}
