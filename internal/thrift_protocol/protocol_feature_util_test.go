package thrift_protocol

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/stretchr/testify/assert"
)

func TestProtocolFeatureSupport(t *testing.T) {
	testCases := []struct {
		name                       string
		version                    cli_service.TProtocolVersion
		expectDirectResults        bool
		expectLz4Compression       bool
		expectCloudFetch           bool
		expectArrow                bool
		expectCompressedArrow      bool
		expectParameterizedQueries bool
		expectMultipleCatalogs     bool
	}{
		{
			name:                       "Protocol V1",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V1,
			expectDirectResults:        true,
			expectLz4Compression:       false,
			expectCloudFetch:           false,
			expectArrow:                false,
			expectCompressedArrow:      false,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     false,
		},
		{
			name:                       "Protocol V2",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V2,
			expectDirectResults:        true,
			expectLz4Compression:       false,
			expectCloudFetch:           false,
			expectArrow:                false,
			expectCompressedArrow:      false,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     false,
		},
		{
			name:                       "Protocol V3",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V3,
			expectDirectResults:        true,
			expectLz4Compression:       false,
			expectCloudFetch:           true,
			expectArrow:                false,
			expectCompressedArrow:      false,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     false,
		},
		{
			name:                       "Protocol V4",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V4,
			expectDirectResults:        true,
			expectLz4Compression:       false,
			expectCloudFetch:           true,
			expectArrow:                false,
			expectCompressedArrow:      false,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     true,
		},
		{
			name:                       "Protocol V5",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V5,
			expectDirectResults:        true,
			expectLz4Compression:       false,
			expectCloudFetch:           true,
			expectArrow:                true,
			expectCompressedArrow:      false,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     true,
		},
		{
			name:                       "Protocol V6",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V6,
			expectDirectResults:        true,
			expectLz4Compression:       true,
			expectCloudFetch:           true,
			expectArrow:                true,
			expectCompressedArrow:      true,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     true,
		},
		{
			name:                       "Protocol V7",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V7,
			expectDirectResults:        true,
			expectLz4Compression:       true,
			expectCloudFetch:           true,
			expectArrow:                true,
			expectCompressedArrow:      true,
			expectParameterizedQueries: false,
			expectMultipleCatalogs:     true,
		},
		{
			name:                       "Protocol V8",
			version:                    cli_service.TProtocolVersion_SPARK_CLI_SERVICE_PROTOCOL_V8,
			expectDirectResults:        true,
			expectLz4Compression:       true,
			expectCloudFetch:           true,
			expectArrow:                true,
			expectCompressedArrow:      true,
			expectParameterizedQueries: true,
			expectMultipleCatalogs:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectDirectResults, SupportsDirectResults(tc.version),
				"DirectResults support check failed for %s", tc.name)

			assert.Equal(t, tc.expectLz4Compression, SupportsLz4Compression(tc.version),
				"LZ4Compression support check failed for %s", tc.name)

			assert.Equal(t, tc.expectCloudFetch, SupportsCloudFetch(tc.version),
				"CloudFetch support check failed for %s", tc.name)

			assert.Equal(t, tc.expectArrow, SupportsArrow(tc.version),
				"Arrow support check failed for %s", tc.name)

			assert.Equal(t, tc.expectCompressedArrow, SupportsCompressedArrow(tc.version),
				"CompressedArrow support check failed for %s", tc.name)

			assert.Equal(t, tc.expectParameterizedQueries, SupportsParameterizedQueries(tc.version),
				"ParameterizedQueries support check failed for %s", tc.name)

			assert.Equal(t, tc.expectMultipleCatalogs, SupportsMultipleCatalogs(tc.version),
				"MultipleCatalogs support check failed for %s", tc.name)
		})
	}
}
