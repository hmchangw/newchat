package natsrouter

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// registerFuncs are the natsrouter registrars that claim an rpc.method and so
// enter a router's Routes() table. RegisterVoid is deliberately excluded: it
// takes no method argument and never appears in Routes(), so a package that
// only calls RegisterVoid has nothing for a golden file to pin.
var registerFuncs = map[string]bool{
	"Register":             true,
	"RegisterNoBody":       true,
	"RegisterOptionalBody": true,
}

// skipDirNames are directories this scan must not descend into: VCS
// metadata, the multi-agent review workflow's own working notes, docs, and
// the semgrep rule's Go fixture (.semgrep/rpcmethod.go intentionally
// contains ruleid-annotated natsrouter.Register calls that are not real
// service registrations). Any directory whose name starts with "_" is
// skipped too — that is the Go toolchain's own convention for "ignore me".
var skipDirNames = map[string]bool{
	".git":         true,
	"node_modules": true,
	".superpowers": true,
	"docs":         true,
	".semgrep":     true,
}

// TestEveryRPCRouteRegistrationHasAGoldenFile is the pairing the other two
// gates assume but never check. A semgrep rule catches a bare string literal
// where a method belongs, and RPCMethod.Valid() catches a value outside the
// vocabulary — but a new package that registers a real, valid, differently
// wrong constant (e.g. history-service's MethodGetMessage on an unrelated
// route) passes both silently. The golden file is the only gate that pins a
// route to its own correct method, so this test makes having one mandatory
// for any directory that registers a method-bearing route, rather than
// leaving the pairing to the convention that all ten current packages
// happen to follow it.
func TestEveryRPCRouteRegistrationHasAGoldenFile(t *testing.T) {
	rootDir := repoRootForGoldenScan(t)

	// os.Root rather than a plain path walk: gosec flags os.ReadFile on a
	// WalkDir-supplied path (G122) because the path can change identity
	// between the walk and the read, and a symlink out of the tree would be
	// followed. A root-scoped FS cannot escape the repository.
	root, err := os.OpenRoot(rootDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	repo := root.FS()

	needsGolden := map[string]bool{}
	fset := token.NewFileSet()

	err = fs.WalkDir(repo, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if p != "." && (skipDirNames[name] || strings.HasPrefix(name, "_")) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}

		src, readErr := fs.ReadFile(repo, p)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", p, readErr)
		}
		file, parseErr := parser.ParseFile(fset, p, src, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", p, parseErr)
		}

		if fileRegistersRPCRoute(file) {
			needsGolden[path.Dir(p)] = true
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, needsGolden, "scan found no natsrouter registrations at all — the walker is probably broken")

	dirs := make([]string, 0, len(needsGolden))
	for dir := range needsGolden {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	for _, dir := range dirs {
		golden := path.Join(dir, "testdata", "routes.golden")
		_, statErr := fs.Stat(repo, golden)
		assert.NoError(t, statErr,
			"%s registers an RPC route (natsrouter.Register / RegisterNoBody / RegisterOptionalBody) "+
				"but has no testdata/routes.golden; add a routes_test.go there calling "+
				"testutil.AssertRoutesGolden(t, router.Routes())", dir)
	}
}

// fileRegistersRPCRoute reports whether file contains a call to one of the
// method-bearing registrars, in either call shape: inferred type arguments
// (natsrouter.Register(r, pattern, method, fn)) or explicit ones
// (natsrouter.Register[Req, Resp](...)), which bot-message-handler and
// bot-room-service use because Go can't infer Req from a nil handler literal
// in some call shapes.
func fileRegistersRPCRoute(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selectorNamesRegisterFunc(call.Fun) {
			found = true
			return false
		}
		return true
	})
	return found
}

// selectorNamesRegisterFunc unwraps an explicit-type-argument call
// (single type argument parses as *ast.IndexExpr, two or more as
// *ast.IndexListExpr) down to the underlying selector, then checks it names
// natsrouter and a method-bearing registrar. It deliberately matches on the
// literal package identifier "natsrouter", the same assumption the
// companion semgrep rule makes — this repo does not alias the import.
func selectorNamesRegisterFunc(fun ast.Expr) bool {
	switch e := fun.(type) {
	case *ast.IndexExpr:
		return selectorNamesRegisterFunc(e.X)
	case *ast.IndexListExpr:
		return selectorNamesRegisterFunc(e.X)
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == "natsrouter" && registerFuncs[e.Sel.Name]
	default:
		return false
	}
}

// repoRootForGoldenScan resolves the repository root relative to this test
// file's working directory (`go test` runs with cwd set to the package under
// test), by walking up until go.mod is found.
func repoRootForGoldenScan(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "walked past the filesystem root without finding go.mod")
		dir = parent
	}
}
