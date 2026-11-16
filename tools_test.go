package dbsql

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModuleRootPackageNames(test *testing.T) {
	sources, err := filepath.Glob("*.go")
	require.NoError(test, err)
	require.NotEmpty(test, sources)

	for _, source := range sources {
		test.Run(source, func(test *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), source, nil, parser.PackageClauseOnly)
			require.NoError(test, err)
			require.Contains(test, []string{"dbsql", "dbsql_test"}, parsed.Name.Name)
		})
	}
}
