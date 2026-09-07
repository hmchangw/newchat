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
// with a dot, so it never reaches go build or golangci-lint. Every scanner
// walks the tree itself and has to be told separately: SEMGREP_FLAGS excludes
// .semgrep, .semgrepignore lists it, and GOSEC_FLAGS gained the matching
// -exclude-dir when this file's planted credentials became the first fixture
// content gosec could see.
//
// None of that reaches a scan outside this repo's Makefile. .semgrepignore
// says why in its own header — "an explicit file path bypasses this file
// entirely" — so a pipeline that scans a changed-file list rather than walking
// the tree sees this file whatever the ignore list holds, and reported all
// thirteen planted credentials the first time the branch went through one.
// Hence the in-place `// nosemgrep: gosec.G101-1` on every line a
// credential rule can reach, which is the same choice .semgrepignore already
// documents for test fixtures. The trailing position is load-bearing: `ruleid:`
// binds to the line directly below it, so a directive on its own line above the
// code would take the slot the test runner needs.
//
// The annotation covers ten lines beyond the thirteen this file asserts as
// positives — the negatives carry credential-shaped names too, and a rule
// slightly wider than ours reaches them. Annotating only what a report happens
// to name today is the mistake PR #471 made.
package testdata

// --- positives: one line per declaration form ---

// ruleid: hardcoded-credential-literal
const constFormPassword = "sup3rs3cr3t" // nosemgrep: gosec.G101-1

// ruleid: hardcoded-credential-literal
var varFormSecret = "sup3rs3cr3t" // nosemgrep: gosec.G101-1

// declarationForms covers the two patterns that only appear inside a function
// body. The package-level const and var above complete the set of four.
func declarationForms() {
	// ruleid: hardcoded-credential-literal
	shortDeclPassword := "sup3rs3cr3t" // nosemgrep: gosec.G101-1

	var assignedSecret string
	// ruleid: hardcoded-credential-literal
	assignedSecret = "sup3rs3cr3t" // nosemgrep: gosec.G101-1

	_, _ = shortDeclPassword, assignedSecret
}

// --- positives: one line per alternative of the name regex ---

// nameAlternatives gives each alternative of the rule's name regex its own line,
// so dropping one from the pattern fails here instead of narrowing the gate.
func nameAlternatives() {
	// ruleid: hardcoded-credential-literal
	password := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	passwd := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	pwd := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	secret := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	token := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	apiKey := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	credential := "value-that-is-long-enough" // nosemgrep: gosec.G101-1

	_, _, _, _, _, _, _ = password, passwd, pwd, secret, token, apiKey, credential
}

// The match is on a substring of the identifier, not the whole of it, so a
// qualified name still trips the rule. This is the shape msgraph_test.go's
// findings actually take once a test names its fixture.
func qualifiedNames() {
	// ruleid: hardcoded-credential-literal
	proxyPassword := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	clientSecretValue := "value-that-is-long-enough" // nosemgrep: gosec.G101-1
	// usernamePassword is the case that made the name exclusions anchored. An
	// unanchored "name" matches the middle of this identifier and vetoed it.
	// ruleid: hardcoded-credential-literal
	usernamePassword := "value-that-is-long-enough" // nosemgrep: gosec.G101-1

	_, _, _ = proxyPassword, clientSecretValue, usernamePassword
}

// valueForms pins the two literal shapes the value regex accepts. The escaped
// case is the one a naive regex misses: an embedded quote must not end the
// match, or `"p\"ass"` reads as a non-literal and walks straight past the gate.
func valueForms() {
	// ruleid: hardcoded-credential-literal
	escapedPassword := "p\"ass" // nosemgrep: gosec.G101-1
	// ruleid: hardcoded-credential-literal
	rawSecret := `raw-string-secret` // nosemgrep: gosec.G101-1

	_, _ = escapedPassword, rawSecret
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

// structLiteralFieldIsNotCovered is the negative assertion for the composite
// literal: no ruleid annotation, so the test runner reports a match here as a
// false positive if a pattern ever widens into that form.
func structLiteralFieldIsNotCovered() config {
	return config{ProxyPassword: "proxypass", ClientSecret: "s"} // nosemgrep: gosec.G101-1
}

// An identifier that names a header, cookie, collection or env var holds a
// protocol constant, not a credential. These are the false positives the name
// regex would otherwise produce against pkg/botauth, upload-service and
// user-service.
const (
	headerAuthToken     = "x-auth-token" // nosemgrep: gosec.G101-1
	ssoTokenName        = "ssoToken"     // nosemgrep: gosec.G101-1
	ssoTokenHeader      = "ssoToken"     // nosemgrep: gosec.G101-1
	ssoTokensCollection = "sso_tokens"   // nosemgrep: gosec.G101-1
	passwordFieldName   = "password"     // nosemgrep: gosec.G101-1
	tokenEnvVar         = "SSO_TOKEN"    // nosemgrep: gosec.G101-1
)

// A Reason catalog constant is an error code, not a credential. pkg/errcode's
// codes_*.go files hold nine of these.
type Reason string

const (
	adminInvalidToken       Reason = "invalid_token"       // nosemgrep: gosec.G101-1
	adminInvalidCredentials Reason = "invalid_credentials" // nosemgrep: gosec.G101-1
)

// An empty string cannot be a credential; table-driven tests use it to assert
// that a required setting was left unset.
func emptyIsNotACredential() {
	password := "" // nosemgrep: gosec.G101-1
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
