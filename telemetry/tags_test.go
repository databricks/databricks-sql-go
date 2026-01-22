package telemetry

import (
	"testing"
)

func TestConnectionTags(t *testing.T) {
	tags := connectionTags()

	if len(tags) == 0 {
		t.Fatal("Expected connection tags to be defined")
	}

	// Verify required tags are present
	requiredTags := []string{TagWorkspaceID, TagSessionID}
	for _, requiredTag := range requiredTags {
		found := false
		for _, tag := range tags {
			if tag.name == requiredTag && tag.required {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Required tag %s not found in connection tags", requiredTag)
		}
	}

	// Verify server address is local-only
	for _, tag := range tags {
		if tag.name == TagServerAddress {
			if tag.exportScope&exportDatabricks != 0 {
				t.Error("server.address should not be exported to Databricks")
			}
			if tag.exportScope&exportLocal == 0 {
				t.Error("server.address should be exported locally")
			}
		}
	}
}

func TestStatementTags(t *testing.T) {
	tags := statementTags()

	if len(tags) == 0 {
		t.Fatal("Expected statement tags to be defined")
	}

	// Verify required tags are present
	requiredTags := []string{TagStatementID, TagSessionID}
	for _, requiredTag := range requiredTags {
		found := false
		for _, tag := range tags {
			if tag.name == requiredTag && tag.required {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Required tag %s not found in statement tags", requiredTag)
		}
	}
}

func TestShouldExportToDatabricks_ConnectionTags(t *testing.T) {
	tests := []struct {
		name     string
		tagName  string
		expected bool
	}{
		{"workspace.id should export", TagWorkspaceID, true},
		{"session.id should export", TagSessionID, true},
		{"driver.version should export", TagDriverVersion, true},
		{"server.address should NOT export", TagServerAddress, false},
		{"unknown tag should NOT export", "unknown.tag", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldExportToDatabricks("connection", tt.tagName)
			if result != tt.expected {
				t.Errorf("shouldExportToDatabricks(%q, %q) = %v, want %v",
					"connection", tt.tagName, result, tt.expected)
			}
		})
	}
}

func TestShouldExportToDatabricks_StatementTags(t *testing.T) {
	tests := []struct {
		name     string
		tagName  string
		expected bool
	}{
		{"statement.id should export", TagStatementID, true},
		{"session.id should export", TagSessionID, true},
		{"result.format should export", TagResultFormat, true},
		{"result.chunk_count should export", TagResultChunkCount, true},
		{"unknown tag should NOT export", "unknown.tag", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldExportToDatabricks("statement", tt.tagName)
			if result != tt.expected {
				t.Errorf("shouldExportToDatabricks(%q, %q) = %v, want %v",
					"statement", tt.tagName, result, tt.expected)
			}
		})
	}
}

func TestShouldExportToDatabricks_UnknownMetricType(t *testing.T) {
	result := shouldExportToDatabricks("unknown_type", TagWorkspaceID)
	if result {
		t.Error("shouldExportToDatabricks with unknown metric type should return false")
	}
}

func TestExportScopeFlags(t *testing.T) {
	// Test bit flag operations
	if exportNone != 0 {
		t.Error("exportNone should be 0")
	}

	if exportAll&exportLocal == 0 {
		t.Error("exportAll should include exportLocal")
	}

	if exportAll&exportDatabricks == 0 {
		t.Error("exportAll should include exportDatabricks")
	}

	if exportLocal&exportDatabricks != 0 {
		t.Error("exportLocal and exportDatabricks should be separate flags")
	}
}

func TestTagDefinitionStructure(t *testing.T) {
	// Verify tag definition structure is correct
	tags := connectionTags()
	for _, tag := range tags {
		if tag.name == "" {
			t.Error("Tag name should not be empty")
		}
		if tag.description == "" {
			t.Error("Tag description should not be empty")
		}
		// exportScope can be 0 (exportNone) which is valid
	}
}

func TestAllConnectionTagsHaveValidScopes(t *testing.T) {
	tags := connectionTags()
	for _, tag := range tags {
		// Each tag should have at least one export scope set
		// (except exportNone which is 0)
		if tag.exportScope != exportNone {
			hasValidScope := (tag.exportScope&exportLocal != 0) || (tag.exportScope&exportDatabricks != 0)
			if !hasValidScope {
				t.Errorf("Tag %s has invalid export scope: %d", tag.name, tag.exportScope)
			}
		}
	}
}

func TestAllStatementTagsHaveValidScopes(t *testing.T) {
	tags := statementTags()
	for _, tag := range tags {
		// Each tag should have at least one export scope set
		if tag.exportScope != exportNone {
			hasValidScope := (tag.exportScope&exportLocal != 0) || (tag.exportScope&exportDatabricks != 0)
			if !hasValidScope {
				t.Errorf("Tag %s has invalid export scope: %d", tag.name, tag.exportScope)
			}
		}
	}
}
