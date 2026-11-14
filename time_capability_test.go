package dbsql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

func serverTimeUnavailable(err error) bool {
	var execution dbsqlerr.DBExecutionError
	if !errors.As(err, &execution) || execution.SqlState() != "0A000" {
		return false
	}
	message := strings.ToUpper(execution.Error())
	return strings.Contains(message, "[UNSUPPORTED_TYPED_LITERAL]") && strings.Contains(message, `OF THE TYPE "TIME"`) ||
		strings.Contains(message, "TIME TYPE IS DISABLED") || strings.Contains(message, "TIME TYPE IS NOT ENABLED")
}

func TestServerTimeUnavailable(t *testing.T) {
	const unsupported = `[UNSUPPORTED_TYPED_LITERAL] Literals of the type "TIME" are not supported.`
	for _, test := range []struct {
		name    string
		state   string
		message string
		want    bool
	}{
		{"TIME literal", "0A000", unsupported, true},
		{"TIME disabled", "0A000", "The TIME type is disabled.", true},
		{"TIME not enabled", "0A000", "The TIME type is not enabled.", true},
		{"other literal", "0A000", `[UNSUPPORTED_TYPED_LITERAL] Literals of the type "DATE" are not supported. Supported types are "TIME".`, false},
		{"other feature", "0A000", "Some other feature is disabled. SELECT TIME '09:15:00'", false},
		{"unauthenticated", "28000", "HTTP 401: " + unsupported, false},
		{"forbidden", "42501", "HTTP 403: " + unsupported, false},
		{"missing state", "", unsupported, false},
		{"scanner failure", "0A000", "databricks: arrow row scanner unhandled type time64[ns]", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := dbsqlerrint.NewExecutionErrorWithState(context.Background(), test.message, nil, "", test.state)
			require.Equal(t, test.want, serverTimeUnavailable(err))
			require.Equal(t, test.want, serverTimeUnavailable(fmt.Errorf("query failed: %w", *err)))
		})
	}
	require.False(t, serverTimeUnavailable(nil))
	require.False(t, serverTimeUnavailable(errors.New(unsupported)))
}
