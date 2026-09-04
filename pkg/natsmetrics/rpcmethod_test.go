package natsmetrics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
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
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rpcmethod.go", nil, 0)
	require.NoError(t, err, "parse rpcmethod.go")

	// declared maps each RPCMethod-typed constant's string value to the
	// identifier that declares it, so a mismatch can name the constant.
	declared := map[RPCMethod]string{}
	for _, decl := range file.Decls {
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
				declared[RPCMethod(value)] = name.Name
			}
		}
	}
	require.NotEmpty(t, declared, "const-block scan found nothing — the parser is probably wrong")

	// Every declared constant except MethodOther must be in rpcMethods.
	for value, name := range declared {
		if name == "MethodOther" {
			assert.False(t, value.Valid(), "MethodOther must stay outside rpcMethods, the fallback is not registerable")
			continue
		}
		assert.True(t, value.Valid(),
			"%s (%q) is declared in the const block but missing from rpcMethods; a route naming it would degrade to _OTHER instead of failing the build", name, value)
	}

	// Nothing in rpcMethods may be absent from the const block.
	for _, m := range rpcMethods {
		name, ok := declared[m]
		assert.True(t, ok, "%q is in rpcMethods but has no matching RPCMethod constant declared in rpcmethod.go", m)
		assert.NotEqual(t, "MethodOther", name, "%q resolved to MethodOther, which must never appear in rpcMethods", m)
	}
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

// Valid is what natsrouter calls at registration, so a gap here is a route that
// panics at startup despite naming a real constant.
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
