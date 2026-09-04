package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Once a package name carries the soak context, repeating that context in
// every identifier creates two names for the same API: rpc.Action outside the
// package and soakRPCAction inside it. Keep the extracted packages on one
// naming convention so later lanes do not copy compatibility scaffolding.
func TestSoakPackages_DoNotRepeatTheSoakPrefixInIdentifiers(t *testing.T) {
	t.Parallel()

	for _, dir := range []string{
		"internal/soak/catalog",
		"internal/soak/rpc",
		"internal/soak/send",
		"internal/soak/userread",
		"internal/soak/wire",
	} {
		dir := dir
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			files, err := filepath.Glob(filepath.Join(dir, "*.go"))
			require.NoError(t, err)
			require.NotEmpty(t, files)

			fset := token.NewFileSet()
			for _, file := range files {
				if strings.HasSuffix(file, "_test.go") {
					continue
				}
				parsed, parseErr := parser.ParseFile(fset, file, nil, 0)
				require.NoError(t, parseErr)
				ast.Inspect(parsed, func(node ast.Node) bool {
					identifier, ok := node.(*ast.Ident)
					if !ok || !strings.HasPrefix(identifier.Name, "soak") {
						return true
					}
					position := fset.Position(identifier.Pos())
					assert.Failf(t, "redundant soak prefix",
						"%s: identifier %q repeats its package context",
						position, identifier.Name)
					return true
				})
			}
		})
	}
}
