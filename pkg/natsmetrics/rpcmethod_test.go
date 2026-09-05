package natsmetrics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var methodFormat = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// verbs is the closed set a method name may start with. It exists so a name
// like "room_rename" cannot land: the noun-first spelling makes every method in
// a domain sort together and read as a category, which is the shape this
// vocabulary replaced. service_name already carries the domain.
var verbs = map[string]bool{
	"add": true, "batch": true, "count": true, "create": true, "delete": true,
	"edit": true, "ensure": true, "get": true, "list": true, "mark": true,
	"migrate": true, "move": true, "open": true, "pin": true, "refresh": true,
	"remove": true, "rename": true, "reorder": true, "search": true, "send": true,
	"set": true, "start": true, "toggle": true, "translate": true, "unpin": true,
	"update": true,
}

func TestRPCMethodNamesFollowTheVocabularyRule(t *testing.T) {
	for _, m := range rpcMethods {
		t.Run(string(m), func(t *testing.T) {
			name := string(m)
			assert.Regexp(t, methodFormat, name, "must be lower snake_case")
			assert.LessOrEqual(t, len(name), 40, "keep method names short enough to read on an axis")
			verb, _, ok := cutFirstToken(name)
			require.True(t, ok, "must be <verb>_<object>, at least two tokens")
			assert.True(t, verbs[verb], "%q is not in the allowed verb set; verb comes first", verb)
		})
	}
}

// _OTHER is the semconv-mandated value for an unrecognized method
// (semconv/v1.40.0/attribute_group.go:13902 — "the attribute MUST be set to
// `_OTHER`"). It is the fallback, never a method a route may claim, so it is
// absent from rpcMethods and exempt from the naming rule above.
func TestOtherIsTheFallbackAndNotRegisterable(t *testing.T) {
	assert.Equal(t, RPCMethod("_OTHER"), MethodOther)
	assert.NotContains(t, rpcMethods, MethodOther)
	assert.False(t, MethodOther.Valid())
	assert.Equal(t, MethodOther, normalizeRPCMethod(RPCMethod("not_registered")))
	assert.Equal(t, MethodOther, normalizeRPCMethod(RPCMethod("")))
}

// TestConstBlockMatchesRPCMethodList keeps the const block and rpcMethods as
// one controlled pair. Nothing else compares them: rpcMethods is asserted at
// a hardcoded length (92) elsewhere, so a constant added to the block but
// left out of the list leaves that count unchanged and every other test
// green — the exact gap "It is the ONLY list" used to claim was a loud
// failure, when in fact nothing checked it at all. This test parses
// rpcmethod.go's const block directly (rather than trusting a second
// hand-maintained copy) and diffs its RPCMethod-typed constants, by value,
// against rpcMethodSet in both directions.
func TestConstBlockMatchesRPCMethodList(t *testing.T) {
	// Every non-test .go file in the package, not just rpcmethod.go. A hard
	// coded filename made the alias guard below cosmetic: a second constant
	// spelling an existing method from a sibling file was invisible to the
	// scan, valid at registration, and accepted by the semgrep rule.
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, parseErr := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, parseErr, "parse %s", name)
		files = append(files, f)
	}
	require.NotEmpty(t, files, "package scan found no non-test .go files")

	// Two indexes, because one is not enough. valueOf answers "what does this
	// constant spell", ownersOf answers "which constants spell this value" —
	// and only the second catches an alias, a second constant declared with a
	// value some other constant already owns. Keyed the other way round, an
	// alias silently overwrites the original owner and every check downstream
	// still passes, because the value itself is perfectly valid.
	valueOf := map[string]RPCMethod{}
	ownersOf := map[RPCMethod][]string{}
	var decls []ast.Decl
	for _, f := range files {
		decls = append(decls, f.Decls...)
	}
	for _, decl := range decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typeIdent, ok := vs.Type.(*ast.Ident)
			if !ok || typeIdent.Name != "RPCMethod" {
				continue
			}
			for i, name := range vs.Names {
				require.Less(t, i, len(vs.Values), "%s: expected an explicit value", name.Name)
				lit, ok := vs.Values[i].(*ast.BasicLit)
				require.True(t, ok && lit.Kind == token.STRING, "%s: expected a string literal value", name.Name)
				value, unquoteErr := strconv.Unquote(lit.Value)
				require.NoError(t, unquoteErr, "%s: unquote %s", name.Name, lit.Value)
				require.NotContains(t, valueOf, name.Name, "%s is declared more than once", name.Name)
				valueOf[name.Name] = RPCMethod(value)
				ownersOf[RPCMethod(value)] = append(ownersOf[RPCMethod(value)], name.Name)
			}
		}
	}
	require.NotEmpty(t, valueOf, "const-block scan found nothing — the parser is probably wrong")

	// One constant per value. An alias spells an existing method under a second
	// name: the label stays bounded and valid, so nothing else here can see it,
	// but the vocabulary then has two spellings for one series.
	for value, owners := range ownersOf {
		assert.Len(t, owners, 1, "%q is declared by more than one constant (%v); one value, one owner", value, owners)
	}

	// Every declared constant except MethodOther must be in rpcMethods.
	for name, value := range valueOf {
		if name == "MethodOther" {
			assert.False(t, value.Valid(), "MethodOther must stay outside rpcMethods, the fallback is not registerable")
			continue
		}
		assert.True(t, value.Valid(),
			"%s (%q) is declared in the const block but missing from rpcMethods; a route naming it would degrade to _OTHER instead of failing the build", name, value)
	}

	// Nothing in rpcMethods may be absent from the const block.
	for _, m := range rpcMethods {
		owners := ownersOf[m]
		require.NotEmpty(t, owners, "%q is in rpcMethods but has no matching RPCMethod constant declared in rpcmethod.go", m)
		assert.NotContains(t, owners, "MethodOther", "%q resolved to MethodOther, which must never appear in rpcMethods", m)
	}

	// MethodOther exists exactly once, and is the only constant outside the
	// list. Together with the counts below this closes the gap in both
	// directions: no extra constant can hide beside the list, and no listed
	// method can be missing one.
	assert.Len(t, ownersOf[MethodOther], 1, "MethodOther must be declared exactly once")
	outside := []string{}
	for name, value := range valueOf {
		if !value.Valid() {
			outside = append(outside, name)
		}
	}
	sort.Strings(outside)
	assert.Equal(t, []string{"MethodOther"}, outside,
		"MethodOther must be the only RPCMethod constant that is not in rpcMethods")
	assert.Len(t, valueOf, len(rpcMethods)+1,
		"the const block must hold exactly one constant per rpcMethods entry, plus MethodOther")
}

func cutFirstToken(name string) (head, tail string, ok bool) {
	for i := 0; i < len(name); i++ {
		if name[i] == '_' {
			return name[:i], name[i+1:], true
		}
	}
	return name, "", false
}

// A repeated entry in this list would mean two constants resolve to one value
// with nothing to tell them apart. It is not the per-route uniqueness guarantee
// — that one lives in natsrouter, because only a router knows which methods a
// single service registered.
func TestRPCMethodListHasNoDuplicateEntries(t *testing.T) {
	seen := map[RPCMethod]bool{}
	for _, m := range rpcMethods {
		assert.False(t, seen[m], "duplicate rpc method %q", m)
		seen[m] = true
	}
	assert.Len(t, rpcMethods, 92)
}

// Valid is what natsrouter calls at registration, so a gap here is a route
// that degrades to _OTHER despite naming a real constant — its samples merge
// into the fallback series instead of carrying the method the code declares.
func TestValidAcceptsEveryDeclaredMethodAndNothingElse(t *testing.T) {
	for _, m := range rpcMethods {
		assert.True(t, m.Valid(), "%q must be registerable", m)
		assert.Equal(t, m, normalizeRPCMethod(m))
	}
	assert.False(t, RPCMethod("").Valid(), "empty is not a method")
	assert.False(t, RPCMethod("not_registered").Valid())
	assert.False(t, MethodOther.Valid(), "the fallback is not registerable")
	assert.Equal(t, MethodOther, normalizeRPCMethod(RPCMethod("not_registered")))
	assert.Equal(t, MethodOther, normalizeRPCMethod(RPCMethod("")))
}
