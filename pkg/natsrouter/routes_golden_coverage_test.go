//go:build (lines && ignore) || ignore || filename || (suffixes && ignore) || release || tags || (ignore && ignore) || cgo || ignore
// +build lines,ignore ignore filename suffixes,ignore release tags ignore,ignore cgo ignore

package natsrouter

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
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
//
// A golden file on disk is not by itself proof the gate runs: delete the
// routes_test.go and the file becomes an unread fixture that agrees with
// nothing. So the directory must also hold a test that calls
// testutil.AssertRoutesGolden — the file and its caller are checked together.
//
// What this proves, exactly: the call is present in a file that the default
// build compiles. Files excluded by a build constraint do not count, because
// `make test` runs no build tags — a call moved behind //go:build integration
// would otherwise satisfy this test while never running.
//
// What it does not prove: that the test body reaches the call. A t.Skip, an
// early return, or a call sitting in a helper nothing invokes all leave this
// test green. No source scan closes that, so it is stated rather than
// claimed away.
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

		called, scanErr := dirCallsAssertRoutesGolden(repo, fset, dir)
		require.NoError(t, scanErr)
		assert.True(t, called,
			"%s registers an RPC route but no default-build _test.go there calls "+
				"testutil.AssertRoutesGolden; without the call the golden file is never "+
				"compared and pins nothing (a call behind //go:build integration does not "+
				"count — make test runs no tags)", dir)
	}
}

// dirCallsAssertRoutesGolden reports whether any _test.go directly in dir
// calls testutil.AssertRoutesGolden. Non-recursive on purpose: the helper
// reads testdata/ relative to the package under test's own working directory,
// so a call from a subdirectory would compare a different package's golden.
func dirCallsAssertRoutesGolden(repo fs.FS, fset *token.FileSet, dir string) (bool, error) {
	entries, err := fs.ReadDir(repo, dir)
	if err != nil {
		return false, fmt.Errorf("read dir %s: %w", dir, err)
	}
	ctxt := buildContextFor(repo, dir)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		p := path.Join(dir, e.Name())
		src, readErr := fs.ReadFile(repo, p)
		if readErr != nil {
			return false, fmt.Errorf("read %s: %w", p, readErr)
		}
		match, matchErr := ctxt.MatchFile(dir, e.Name())
		if matchErr != nil {
			return false, fmt.Errorf("match %s: %w", p, matchErr)
		}
		if !match {
			continue
		}
		file, parseErr := parser.ParseFile(fset, p, src, 0)
		if parseErr != nil {
			return false, fmt.Errorf("parse %s: %w", p, parseErr)
		}
		if fileCallsAssertRoutesGolden(file) {
			return true, nil
		}
	}
	return false, nil
}

// buildContextFor returns a build.Context that reads through the repo FS, so
// MatchFile answers exactly as the toolchain would: //go:build and legacy
// all of it, for the default build with no -tags.
//
// This replaced a hand-rolled evaluator that understood only GOOS, GOARCH and
// unix. That version got `//go:build !go1.25` backwards — it treated the
// unknown tag go1.25 as false, so the negation came out true and a file the
// toolchain never compiles counted as coverage. `!cgo` failed the same way.
// Reimplementing the toolchain's tag rules is not worth doing by hand when the
// toolchain exports them.
func buildContextFor(repo fs.FS, dir string) *build.Context {
	ctxt := build.Default
	ctxt.GOOS = runtime.GOOS
	ctxt.GOARCH = runtime.GOARCH
	ctxt.BuildTags = nil
	ctxt.JoinPath = path.Join
	ctxt.IsAbsPath = func(string) bool { return false }
	ctxt.OpenFile = func(p string) (io.ReadCloser, error) {
		f, err := repo.Open(p)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", p, err)
		}
		return f, nil
	}
	ctxt.IsDir = func(p string) bool {
		info, err := fs.Stat(repo, p)
		return err == nil && info.IsDir()
	}
	ctxt.ReadDir = func(p string) ([]os.FileInfo, error) {
		entries, err := fs.ReadDir(repo, p)
		if err != nil {
			return nil, fmt.Errorf("read dir %s: %w", p, err)
		}
		out := make([]os.FileInfo, 0, len(entries))
		for _, e := range entries {
			info, statErr := e.Info()
			if statErr != nil {
				return nil, fmt.Errorf("stat %s: %w", e.Name(), statErr)
			}
			out = append(out, info)
		}
		return out, nil
	}
	_ = dir
	return &ctxt
}

// fileCallsAssertRoutesGolden looks for a testutil.AssertRoutesGolden call.
// The package qualifier is matched loosely (any selector base) so an import
// alias does not silently defeat the check.
func fileCallsAssertRoutesGolden(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name == "AssertRoutesGolden" {
			found = true
			return false
		}
		return true
	})
	return found
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
