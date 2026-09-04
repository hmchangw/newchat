package natsmetrics

import (
	"regexp"
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

// The naming rule is only a rule if it runs over the same list the lookup
// reads. It did not: the guards iterated a test-file copy while Valid() read a
// switch, so a method added to the switch but not the copy was registerable
// while violating snake_case, the verb set and the length cap.
func TestVocabularyGuardsRunOverTheProductionList(t *testing.T) {
	require.NotEmpty(t, rpcMethods)
	for _, m := range rpcMethods {
		t.Run(string(m), func(t *testing.T) {
			name := string(m)
			assert.Regexp(t, methodFormat, name, "must be lower snake_case")
			assert.LessOrEqual(t, len(name), 40)
			verb, _, ok := cutFirstToken(name)
			require.True(t, ok, "must be <verb>_<object>")
			assert.True(t, verbs[verb], "%q is not an allowed verb", verb)
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

func TestEveryDeclaredMethodIsValidAndNormalizesToItself(t *testing.T) {
	for _, m := range rpcMethods {
		assert.True(t, m.Valid(), "%q must be registerable", m)
		assert.Equal(t, m, normalizeRPCMethod(m))
	}
	assert.Len(t, rpcMethods, 92)
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
