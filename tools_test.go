package dbsql

import (
	"go/build"
	"testing"
)

// TestModuleRootIsASinglePackage guards the fix for #162: the module root must
// declare exactly one package no matter which build tags are in play.
//
// The build-time test runner used to be pinned by a `//go:build tools` file
// declaring `package tools` next to `package dbsql`. Tooling that resolves a
// directory's package without honoring build constraints (bazel/gazelle) — or
// that deliberately enables the `tools` tag (golangci-lint) — then saw two
// package names in one directory and failed to load the driver. gotestsum is
// pinned by the go.mod `tool` directive instead, so nothing but `dbsql` lives
// here.
func TestModuleRootIsASinglePackage(t *testing.T) {
	ctx := build.Default
	// Any tag a consumer might enable must not conjure a second package; "tools"
	// is the one that historically did.
	ctx.BuildTags = append(ctx.BuildTags, "tools")

	if _, err := ctx.ImportDir(".", 0); err != nil {
		if _, multiple := err.(*build.MultiplePackageError); multiple {
			t.Fatalf("module root declares more than one package: %v", err)
		}
		t.Fatalf("could not load the module root: %v", err)
	}
}
