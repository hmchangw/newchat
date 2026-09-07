// Package testdata holds the fixtures for hardcoded-credentials.yml.
//
// The rule exists because the gate the repo runs and the gate the code is
// judged by had drifted apart: an external Semgrep ruleset reported five
// hard-coded passwords in pkg/msgraph/msgraph_test.go that `make sast` never
// saw, because neither p/golang nor p/security-audit carries a Go
// hardcoded-credential rule and gosec's G101 filters on entropy — "sup3rs3cr3t"
// and "secret" fall under its threshold, so gosec stayed green too. PR #471 was
// written from the green view and paired nosemgrep directives onto the sites
// that already carried #nosec G101, which were not the reported ones; the five
// findings moved down by the inserted lines and were otherwise untouched.
//
// `semgrep scan --test` reads the annotations: a `ruleid:` comment names every
// rule that must fire on the following line. Lines with no annotation are
// negative assertions — a rule firing there is reported as a false positive.
// Both directions carry weight here. The positives pin the declaration forms
// the rule covers, and the negatives pin the shapes it deliberately does not:
// the struct-literal field form matches 252 sites repo-wide and is not what the
// external ruleset reports, and an identifier that names a header, a cookie or
// a collection holds a wire constant rather than a credential.
//
// Coverage is per independently editable branch: each syntactic form and each
// alternative of the name regex gets its own line, so deleting any one of them
// fails here rather than quietly narrowing the gate.
//
// The file sits beside hardcoded-credentials.yml because the test runner
// matches a rule file to a target of the same basename and does not support a
// separate tests directory. The Go toolchain ignores any directory beginning
// with a dot, so it never reaches go build or golangci-lint. The scanners walk
// the tree themselves and need telling: SEMGREP_FLAGS excludes .semgrep, and
// GOSEC_FLAGS gained the matching -exclude-dir when this file's planted
// credentials became the first fixture content gosec could see.
package testdata

// --- positives: one line per declaration form ---

// ruleid: hardcoded-credential-literal
const constFormPassword = "sup3rs3cr3t"

// ruleid: hardcoded-credential-literal
var varFormSecret = "sup3rs3cr3t"

func declarationForms() {
	// ruleid: hardcoded-credential-literal
	shortDeclPassword := "sup3rs3cr3t"

	var assignedSecret string
	// ruleid: hardcoded-credential-literal
	assignedSecret = "sup3rs3cr3t"

	_, _ = shortDeclPassword, assignedSecret
}

// --- positives: one line per alternative of the name regex ---

func nameAlternatives() {
	// ruleid: hardcoded-credential-literal
	password := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	passwd := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	pwd := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	secret := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	token := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	apiKey := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	credential := "value-that-is-long-enough"

	_, _, _, _, _, _, _ = password, passwd, pwd, secret, token, apiKey, credential
}

// The match is on a substring of the identifier, not the whole of it, so a
// qualified name still trips the rule. This is the shape msgraph_test.go's
// findings actually take once a test names its fixture.
func qualifiedNames() {
	// ruleid: hardcoded-credential-literal
	proxyPassword := "value-that-is-long-enough"
	// ruleid: hardcoded-credential-literal
	clientSecretValue := "value-that-is-long-enough"

	_, _ = proxyPassword, clientSecretValue
}

// --- negatives: shapes the rule must not flag ---

// A struct-literal field is deliberately out of scope. The form matches 252
// sites repo-wide — every table-driven test that builds a Config — and the
// external ruleset that prompted this rule does not report it either. Widening
// to cover it would trade one real finding for hundreds of annotations.
type config struct {
	ProxyPassword string
	ClientSecret  string
}

func structLiteralFieldIsNotCovered() config {
	return config{ProxyPassword: "proxypass", ClientSecret: "s"}
}

// An identifier that names a header, cookie, collection or env var holds a
// protocol constant, not a credential. These are the false positives the name
// regex would otherwise produce against pkg/botauth, upload-service and
// user-service.
const (
	headerAuthToken    = "x-auth-token"
	ssoTokenName       = "ssoToken"
	ssoTokenHeader     = "ssoToken"
	ssoTokensCollection = "sso_tokens"
	passwordFieldName  = "password"
	tokenEnvVar        = "SSO_TOKEN"
)

// A Reason catalog constant is an error code, not a credential. pkg/errcode's
// codes_*.go files hold nine of these.
type Reason string

const (
	adminInvalidToken       Reason = "invalid_token"
	adminInvalidCredentials Reason = "invalid_credentials"
)

// An empty string cannot be a credential; table-driven tests use it to assert
// that a required setting was left unset.
func emptyIsNotACredential() {
	password := ""
	_ = password
}

// A name without a credential word is out of scope however secret-looking the
// value is. Entropy is gosec's job, and the whole reason this rule exists is
// that entropy filtering is what let "secret" and "sup3rs3cr3t" through.
func unrelatedName() {
	roomID := "sup3rs3cr3t"
	_ = roomID
}

// A value read from the environment is the shape the rule is steering toward,
// so it must stay silent even under a credential-shaped name.
func fromEnvIsFine(lookup func(string) string) {
	password := lookup("GRAPH_PROXY_PASSWORD")
	_ = password
}
