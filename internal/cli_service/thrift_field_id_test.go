package cli_service

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestThriftFieldIdsAreWithinAllowedRange validates that all Thrift field IDs
// in cli_service.go are within the allowed range.
//
// Field IDs in Thrift must stay below 3329 to avoid conflicts with reserved ranges
// and ensure compatibility with various Thrift implementations and protocols.
func TestThriftFieldIdsAreWithinAllowedRange(t *testing.T) {
	const maxAllowedFieldID = 3329

	// Get the directory of this test file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("Failed to get current file path")
	}

	// Build path to cli_service.go
	testDir := filepath.Dir(filename)
	cliServicePath := filepath.Join(testDir, "cli_service.go")

	violations, err := validateThriftFieldIDs(cliServicePath, maxAllowedFieldID)
	if err != nil {
		t.Fatalf("Failed to validate thrift field IDs: %v", err)
	}

	if len(violations) > 0 {
		errorMessage := fmt.Sprintf(
			"Found Thrift field IDs that exceed the maximum allowed value of %d.\n"+
				"This can cause compatibility issues and conflicts with reserved ID ranges.\n"+
				"Violations found:\n",
			maxAllowedFieldID-1)

		for _, violation := range violations {
			errorMessage += fmt.Sprintf("  - %s\n", violation)
		}

		t.Fatal(errorMessage)
	}
}

// validateThriftFieldIDs parses the cli_service.go file and extracts all thrift field IDs
// to validate they are within the allowed range.
func validateThriftFieldIDs(filePath string, maxAllowedFieldID int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	var violations []string
	scanner := bufio.NewScanner(file)
	lineNumber := 0

	// Regex to match thrift field tags
	// Matches patterns like: `thrift:"fieldName,123,required"` or `thrift:"fieldName,123"`
	thriftTagRegex := regexp.MustCompile(`thrift:"([^"]*),(\d+)(?:,([^"]*))?"`)

	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()

		// Find all thrift tags in the line
		matches := thriftTagRegex.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) >= 3 {
				fieldName := match[1]
				fieldIDStr := match[2]

				fieldID, err := strconv.Atoi(fieldIDStr)
				if err != nil {
					// Skip invalid field IDs (shouldn't happen in generated code)
					continue
				}

				if fieldID >= maxAllowedFieldID {
					// Extract struct/field context from the line
					context := extractFieldContext(line)
					violation := fmt.Sprintf(
						"Line %d: Field '%s' has ID %d (exceeds maximum of %d) - %s",
						lineNumber, fieldName, fieldID, maxAllowedFieldID-1, context)
					violations = append(violations, violation)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return violations, nil
}

// extractFieldContext extracts the field declaration context from a line of code
func extractFieldContext(line string) string {
	// Remove leading/trailing whitespace
	line = strings.TrimSpace(line)

	// Try to extract the field name and type from the line
	// Format is typically: FieldName Type `tags...`
	parts := strings.Fields(line)
	if len(parts) >= 2 {
		fieldName := parts[0]
		fieldType := parts[1]
		return fmt.Sprintf("%s %s", fieldName, fieldType)
	}

	// Fallback to returning the trimmed line if we can't parse it
	if len(line) > 100 {
		return line[:100] + "..."
	}
	return line
}
